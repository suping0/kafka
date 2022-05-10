/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.Iterator;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link org.apache.kafka.common.record.MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;
    private final long lingerMs;
    private final long retryBackoffMs;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    private final IncompleteRecordBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private int drainIndex;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link org.apache.kafka.common.record.MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<>();
        String metricGrpName = "producer-metrics";
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in abortIncompleteBatches().
        // 我们跟踪追加线程的数量，以确保在abortIncompleteBatches()中不会错过批次。
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            // 获取给定主题分区的deque，必要时创建它。
            // 存储了该分区对应的batchs
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            // 同步尝试添加records到batch
            synchronized (dq) {
                if (closed) {
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                }
                /*
                 *如果在tryAppend()的时候发现，则会在double check时重新申请新的bffer
                 */
                // 添加数据到records到batch
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // 如果batch是存在的，就会将消息放入队列最近一个batch中，这个方法就可以返回了
                    return appendResult;
                }
                // 如果说此时还没有创建对应的batch，此时会导致放入Deque会失败
            }

            /*
            * 你在实际生产环境，request.max.size，batch.size是必须要调优的，
            * 你必须要根据自己实际发送的消息的大小来设置request.max.size和batch.size，如果你的消息频繁的是超过了batch.sizse的话
            * 如果不设置的话，一个batch就一条消息，batch打包的机制还有意义吗？每条消息都对应一次网络请求
            * */
            // we don't have an in-progress record batch try to allocate a new batch
            // 一个batch就对应了一块内存空间，这里要放一堆的消息，batchSize默认的大小是16kb，
            // 如果你的消息最大的值是1mb，如果说你的消息大于了16kb的话，就会使用你的消息的大小来分配一块内存空间
            // 否则如果你的消息是小于16kb的话，那么就会基于16kb来分配内存空间
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            // 他会基于BufferPool给这个batch分配一块内存出来，之所以说是Pool，
            // 就是因为这个batch代表的内存空间是可以复用的，用完一块内存之后会放回去下次给别人来使用，
            // 复用内存，避免了频繁的使用内存，丢弃对象，垃圾回收
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            /*
             * 其实可能会有多个线程并发的执行，多个线程都可能分别拿到一个16kb的ByteBuffer
             * 但是只有一个线程可以进去下面的代码块
             * 3个线程，线程1，线程2，线程3，这3个线程都会获取到一个16kb的ByteBuffer内存
             */
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed) {
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                }
                /*
                 * 再次执行tryAppend()，进行doubule
                 * 假设线程2进入了synchronized代码块里面去，基于16kb的ByteBuffer构造一个batch，放入Dequeue中，就成功了
                 * 接着线程3进入了synchronized代码块里面去，直接把消息放入Dequeu中已有的一个batch里去，
                 * 那么他手上的一个16kb的ByteBuffer怎么办？
                 * 在这里就会把这个16kb的ByteBuffer给放入到BufferPool的池子里去，保证内存可以复用
                 */
                // 上面尝试执行了了一次tryAppend(), 这里又执行了一次，相当于是做了double check
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                // 首次执行tryAppend()，返回的是空的，因为还没有可分配的buffer
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    // 将分配后未使用的buffer，重新放到空闲列表中，保证内存可复用。
                    free.deallocate(buffer);
                    return appendResult;
                }

                // 构建一个可写的内存records
                MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression, this.batchSize);
                // 一批正在或将要发送的记录。
                RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());
                // batch.tryAppend() 添加新的记录和偏移量到缓冲区。
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));
                // 基于新分配的buffer,添加batch
                dq.addLast(batch);
                // 未发送的batch
                incomplete.add(batch);
                // 刚刚添加到记录累加器中的记录元数据。
                return new RecordAppendResult(future, dq.size() > 1 || batch.records.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full), close its memory records to release temporary
     * resources (like compression streams buffers).
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        // 返回队列中最后一个元素
        RecordBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null) {
                // 如果当前buffer不能写，则关闭buffer的写入流
                // 更新标志位，将该buffer用于读取
                last.records.close();
            } else {
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
            }
        }
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator for more than the configured requestTimeout
     * due to metadata being unavailable
     * 由于元数据不可用，中止已在RecordAccumulator中放置超过配置的requestTimeout的批处理
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> dq = entry.getValue();
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    while (batchIterator.hasNext()) {
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.records.isFull();
                        // check if the batch is expired 检查批次是否过期
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, this.lingerMs, isFull)) {
                            expiredBatches.add(batch);
                            count++;
                            batchIterator.remove();
                            deallocate(batch);
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", count);

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     * 在累加器中重新查询给定的记录批次，以便重试。
     */
    public void reenqueue(RecordBatch batch, long now) {
        batch.attempts++;
        batch.lastAttemptMs = now;
        batch.lastAppendTime = now;
        batch.setRetry();
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            /*
             * 重试的Batch会放入到队列的头部，不是尾部，这样的话，
             * 下一次循环的时候就可以优先处理这个要重新发送的Batch了，attempts、lastAttemptMs这些参数都会进行设置，
             * 辅助判断这个Batch下一次是什么时候要进行重试发送，
             * Batch的内存资源不会释放掉的。
             */
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        boolean unknownLeadersExist = false;

        // exhausted 为true，内存耗尽，有人在排队等待申请内存
        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();

            Node leader = cluster.leaderFor(part);
            if (leader == null) {
                // 如果某个分区的Leader Broker还不知道是谁，此时就会设置一个标志位，后面会尝试进行元数据的拉取
                unknownLeadersExist = true;
            } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                synchronized (deque) {
                    // 获取recored batch； 对一个Partiton的Dequeu在这里会遍历，只看first Batch，非常的关键
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        /*
                         * backingOff 是跟请求重试有关系的，表示这个是否是一个处于重试状态的batch
                         * 除非你的请求失败了，此时开始重试，然后就会在这里有一段判断的逻辑，重试是有一个间隔的，默认是100ms，
                         * 如果进入了重试的阶段，上一次发送这个batch的时间 + 重试间隔的时间，是否大于了当前时间。
                         * 如果一旦进入了重试阶段，每次发送这个Batch，都必须符合重试的间隔才可以，必须得是超过了重试间隔的时间之后，
                         * 才可以再次发送这个Batch。
                         * 刚开始的时候，默认情况下，发送一个Batch，肯定是不涉及到重试，attempts就一定是0，一定没有进入重试的状态。
                         * retryBackoffMs 默认 100ms
                         */
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        /*
                        * waitedTimeMs，当前时间减去上一次发送这个Batch的时间，
                        * 假设一个Batch从来没有发送过，此时当前时间减去这个Batch被创建出来的那个时间，这个Batch从创建开始到现在已经等待了多久了
                        * */
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        /*
                         * timeToWaitMs，这个Batch从创建开始算起，最多等待多久就必须去发送，
                         * 如果是在重试的阶段，这个时间就是重试间隔，但是在非重试的初始阶段，就是linger.ms的时间（100ms）。
                         */
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        // 剩余等待的时间
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        /*
                         * Batch是否已满，
                         * 如果说Dequeue里超过一个Batch了，说明这个peekFirst返回的Batch就一定是已经满的，
                         * 另外就是如果假设Dequeue里只有一个Batch，但是判断发现这个Batch达到了16kb的大小，也是已满的
                         */
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        /*
                         * expired，当前Batch已经等待的时间（120ms） >= Batch最多只能等待的时间（100ms），
                         * 已经超出了linger.ms的时间范围了，否则呢，60ms < 100ms，此时就没有过期。
                         * 如果linger.ms默认是0，就意味着说，只要Batch创建出来了，在这个地方一定是expired = true，
                         * 意味着创建出来的batch,没有等待的时间间隔，立马会被发送出去，所以linger.ms还是需要设置下。
                         * 如果你的batch一般是100ms会被写满，那么就将linger.ms设置成110ms，这样既可以使用batch，又不至于等待时间过长
                         */
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        /*
                         * sendable，综合上述所有条件来判断，这个Batch是否需要发送出去，
                         * full：如果Bach已满必须得发送，
                         * expired：如果Batch没有写满但是expired也必须得发送出去，
                         * exhausted：如果说Batch没有写满而且也没有expired，但是内存已经消耗完毕
                         * closed：当前客户端要关闭掉，此时就必须立马把内存缓冲的Batch都发送出去，
                         * flushInProgress()：就是当前强制必须把所有数据都flush出去到网络里面去，此时就必须得发送
                         */
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        if (sendable && !backingOff) {
                            /*
                             * 可以发送 && 不是重试状态
                             */
                            // 如果此时判断出来这个Batch是可以发送出去的，
                            // 此时就会将这个Batch对应的那个Partiton的Leader Broker给放入到一个Set里去，
                            // 他在这里找的不是说找哪些Partition可以发送数据，也不是找Batch
                            // 他在这里找的是哪些Broker有数据可以发送过去，而且通过Set进行了去重，
                            // 因为可能对于一个Broker而言，是有多个Partiton的Batch可以发送过去的
                            readyNodes.add(leader);
                        } else {
                            /*
                             * 但是如果说此时某个Batch还没有达到要发送的条件
                             * 比如说此时看到一个Partition的batch还没达到要发送的条件，batch没满，linger.ms也没到，
                             * 但是linger.ms设置的是最多等待100ms，但是此时已经等待了60ms，但是剩余等待的时间40ms（timeLeftMs），
                             * 40ms设置为nextReadyCheckDelayMs。
                             * 接下来又有一个Partition的batch同样的情况，batch没满，linger.ms没到，此时已经等待了90ms，剩余等待的时间就是10ms，
                             * 10ms会设置为nextReadyCheckDelayMs
                             *
                             * 他会算出来当前所有的Partition的Batch里，暂时不能发送的那些Batch，需要等待 最少时间 就能发送的那个Batch，
                             * 他还需要等待的时间，就设置为nextReadyCheckDelayMs，下次再来检查是否有batch可以发送，
                             * 起码要等nextReadyCheckDelayMs时间过了以后才可以。
                             */
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            // 请注意，这个结果是一个保守的估计，因为一个不可发送的分区可能有一个领导，而这个领导以后会被发现有可发送的数据。
                            // 不过，这已经很好了，因为我们只需要在剩余的时间里醒来，然后再睡觉。
                            // nextReadyCheckDelayMs : 下一次来检查是否有Batch可以发送的时间间隔
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        // unknownLeadersExist : 是否有Partiton还不知道自己的Leader所在的Broke
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeadersExist);
    }

    /**
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     * 排除给定节点的所有数据，并将其整理成一个批次列表，以每个节点为单位，在指定的大小范围内进行整理。
     * 这种方法试图避免重复选择相同的topic-node。
     * 
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {
        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }

        /*
         * 获取broker上所有的partition，遍历broker上的所有的partitions，
         * 对每个partition获取到dequeue里的first batch，放入待发送到broker的列表里，
         * 每个broker都有一个batches，最后有一个map，放了这些数据
         */
        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            // 存储每个broker可以发送的batch
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // Only proceed if the partition has no in-flight batches.
                if (!muted.contains(tp)) {
                    // 得到topic-partition的deque
                    Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            // 取出队头的batch，不删除batch
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                /**
                                 * lastAttemptMs，是他重新入队的时间，retryBackoffMs其实就是重试的间隔，默认是100ms，
                                 * 他的意思是必须间隔一定的时间再进行重试，这个100ms一般来说建议保持默认值就可以了，
                                 * 但是重试的次数可以自己设置一下，一遍来说建议设置为3次。
                                 * 如果3次重试 都不行，那么一定是Kafka的集群出现问题了，此时人家就会回调你，通知你的回调函数说，重试之后还是异常。
                                 *
                                 * first.lastAttemptMs + retryBackoffMs > now
                                 * 重新入队之后到现在必须已经过了100ms了，才能算做backingOff是true。
                                 * lastAttemptMs + retryBackoffMs > now，意思是什么？
                                 * 上次重新入队的时间到现在还没超过100ms呢，
                                 * 如果说当前时间距离上次入队时间还没到100ms，此时backingOff就是true，如果是true的话，就不能重试。
                                 * 假如说：lastAttemptMs + retryBackoffMs <= now，就说明现在的时间距离上次重新入队的时间已经超过了100ms了，
                                 * 此时backingOff就是false，此时就说明这个要重试的Batch就可以再次发送了。
                                 */
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // Only drain the batch if it is not during backoff period.
                                // 仅当批次不在backoff期间时才将其发送。
                                if (!backoff) {
                                    // 重试的batch
                                    if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // there is a rare case that a single batch size is larger than the request size due
                                        // to compression; in this case we will still eventually send this batch in a single
                                        // request
                                        break;
                                    } else {
                                        // 取出batch并从deque中删除
                                        RecordBatch batch = deque.pollFirst();
                                        batch.records.close();
                                        size += batch.records.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     * 他会从内存缓冲区里获取一个分区对应的Deque，这个Deque里是一个队列，放了很多的Batch，
     * 就是这个分区对应的多个batch，
     */
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        // batches 是 CopyOnWrite
        // 他内存中就是一个最最普通的，非线程安全的Map数据结构，
        // 但是他把这个数据结构定义为volatile类型，就可以保证可见性，就是只要有人更新了这个引用变量对应的实际的map对象的地址，就可以立即看到。
        // 读的时候是完全不用加锁的，多个线程并发进来，高并发的执行读的操作，在这里完全是没有任何的互相之间的影响的，可以实现高并发的读，没有锁在这里。
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null) {
            // 如果队列已经存在了，直接返回即可
            return d;
        }
        d = new ArrayDeque<>();
        // 多个线程会并发的执行putIfAbsent方法，在这个方法里可以保证线程安全的，
        // 除非队列不存在才会设置进去，在put方法的时候是有synchronized，可以保证同一时间只有一个线程会来更新这个值。
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        // 为什么说写数据的时候不会阻塞读的操作，针对副本进行kv设置，把副本通过volatile写的方式赋值给对应的变量
        if (previous == null) {
            return d;
        } else {
            return previous;
        }
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        incomplete.remove(batch);
        // 释放buffer资源，返回到buffer pool中
        free.deallocate(batch.records.buffer(), batch.records.initialCapacity());
    }
    
    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }
    
    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.records.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final boolean unknownLeadersExist;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, boolean unknownLeadersExist) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeadersExist = unknownLeadersExist;
        }
    }
    
    /*
     * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
     */
    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }
        
        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }
        
        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed)
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
            }
        }
        
        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
