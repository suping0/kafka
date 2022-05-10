/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.utils._
import kafka.utils.timer._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.metrics.KafkaMetricsGroup

import java.util.LinkedList
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.utils.Utils

import scala.collection._

import com.yammer.metrics.core.Gauge


/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 * 一种操作，其处理最多需要延迟给定的延迟时间
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 * 完成延迟操作时的逻辑在onComplete（）中定义，并且将只调用一次。
 * 一旦操作完成，isCompleted（）将返回true。
 * onComplete（）可以由forceComplete（）触发，forceComplete（）在delayMs之后强制调用onComplete（），如果操作尚未完成，tryComplete（）则首先检查操作是否可以完成，如果是，则调用forceComplete（）。
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 * DelayedOperation的子类需要同时提供onComplete（）和tryComplete（）的实现。
 */
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {

  private val completed = new AtomicBoolean(false)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   */
  def isCompleted(): Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   */
  def onComplete(): Unit

  /*
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   */
  def tryComplete(): Boolean

  /*
   * run() method defines a task that is executed on timeout
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       reaperEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

  private val removeWatchersLock = new ReentrantReadWriteLock()

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value = delayed()
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   * 检查操作是否可以完成，如果不能按给定的监视键进行监视
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   * 请注意，可以在多个键上监视延迟操作。
   * 一个操作可能在被添加到监视列表中的某些键（而不是所有键）之后完成。
   * 在这种情况下，操作被视为已完成，不会添加到剩余键的监视列表中。
   * 过期收割机(The expiration reaper)线程将从存在此操作的任何观察者列表中删除此操作。
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   *         如果延迟的操作可以由调用方完成，则返回true
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.size > 0, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys.
    // Calling tryComplete() for each key is going to be expensive if there are many keys.
    // Instead, we do the check in the following way. Call tryComplete().
    // If the operation is not completed, we just add the operation to all keys.
    // Then we call tryComplete() again. At this time, if the operation is still not completed, we are guaranteed that it won't miss any future triggering event since the operation is already on the watcher list for all keys.
    // This does mean that if the operation is completed (by another thread) between the two tryComplete() calls, the operation is unnecessarily added for watch.
    // However, this is a less severe issue since the expire reaper will clean it up periodically.
    /*
    tryComplete（）的开销通常与键的数量成正比。
    如果存在多个键，则为每个键调用tryComplete（）将非常昂贵。
    相反，我们按以下方式进行检查。调用tryComplete（）。
    如果操作没有完成，我们只需将操作添加到所有键。
    然后我们再次调用tryComplete（）。此时，如果操作仍然没有完成，我们可以保证它不会错过任何未来的触发事件，因为操作已经在所有键的观察者列表中。
    这意味着，如果操作在两个tryComplete（）调用之间完成（由另一个线程完成），则不必要添加该操作以进行监视。
    然而，这是一个不太严重的问题，因为过期收割机将定期清理它。
     */

    var isCompletedByMe = operation synchronized operation.tryComplete()
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted())
        return false
      watchForOperation(key, operation)

      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }

    isCompletedByMe = operation synchronized operation.tryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    // 如果现在还不能完成，因此被监视，那么也添加到expire队列
    if (! operation.isCompleted()) {
      timeoutTimer.add(operation)
      if (operation.isCompleted()) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched() = allWatchers.map(_.watched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def delayed() = timeoutTimer.size

  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   */
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list.
   * 返回给定key的监视列表，注意我们需要获取removeWatchersLock以避免操作被添加到removeWatcher列表中。
   */
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      val watcher = watchersForKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.watched == 0) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   * 基于某个键的监视的延迟操作的链表
   */
  private class Watchers(val key: Any) {

    private[this] val operations = new LinkedList[T]()

    def watched: Int = operations synchronized operations.size

    // add the element to watch
    def watch(t: T) {
      operations synchronized operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    def tryCompleteWatched(): Int = {

      var completed = 0
      operations synchronized {
        val iter = operations.iterator()
        while (iter.hasNext) {
          val curr = iter.next()
          if (curr.isCompleted) {
            // another thread has completed this operation, just remove it
            iter.remove()
          } else if (curr synchronized curr.tryComplete()) { // 尝试执行
            completed += 1
            iter.remove()
          }
        }
      }

      if (operations.size == 0)
        removeKeyIfEmpty(key, this)

      completed
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0
      operations synchronized {
        val iter = operations.iterator()
        while (iter.hasNext) {
          val curr = iter.next()
          if (curr.isCompleted) {
            iter.remove()
            purged += 1
          }
        }
      }

      if (operations.size == 0)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  /**
   * 高级时钟
   * @param timeoutMs
   */
  def advanceClock(timeoutMs: Long) {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    /*
    如果已完成但仍被监视的操作数大于清除阈值，则触发清除。
    这个数字是由估计的操作总数和挂起的延迟操作数之间的差值来计算的。
     */
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)
      debug("Begin purging watch lists")
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d".format(brokerId),
    false) {

    override def doWork() {
      advanceClock(200L)
    }
  }
}
