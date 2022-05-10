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

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);

    public int recordCount = 0;
    public int maxRecordSize = 0;
    public volatile int attempts = 0;
    public final long createdMs;
    public long drainedMs;
    public long lastAttemptMs;
    public final MemoryRecords records;
    public final TopicPartition topicPartition;
    public final ProduceRequestResult produceFuture;
    public long lastAppendTime;
    private final List<Thunk> thunks;
    private long offsetCounter = 0L;
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 将记录追加到当前记录集中，并返回该记录集中的相对偏移量。
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     *         与该记录对应的RecordSend，如果没有足够的空间，则为空。
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        if (!this.records.hasRoomFor(key, value)) {
            // 检查我们是否有空间给包含给定键/值对的新记录。
            return null;
        } else {
            // 写消息到bufffer中
            long checksum = this.records.append(offsetCounter++, timestamp, key, value);
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            this.lastAppendTime = now;
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length);
            if (callback != null) {
                thunks.add(new Thunk(callback, future));
            }
            this.recordCount++;
            return future;
        }
    }

    /**
     * Complete the request
     * 
     * @param baseOffset The base offset of the messages assigned by the server
     * @param timestamp The timestamp returned by the broker.
     * @param exception The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long timestamp, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                  topicPartition,
                  baseOffset,
                  exception);
        // execute callbacks
        // 遍历得到每条消息对应的回调函数
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                // 封装了回调函数的thunk
                Thunk thunk = this.thunks.get(i);
                if (exception == null) {
                    // If the timestamp returned by server is NoTimestamp, that means CreateTime is used. Otherwise LogAppendTime is used.
                    RecordMetadata metadata = new RecordMetadata(this.topicPartition,  baseOffset, thunk.future.relativeOffset(),
                                                                 timestamp == Record.NO_TIMESTAMP ? thunk.future.timestamp() : timestamp,
                                                                 thunk.future.checksum(),
                                                                 thunk.future.serializedKeySize(),
                                                                 thunk.future.serializedValueSize());
                    // 如果正常情况下，就会回调你的每条消息对应的一个回调函数
                    thunk.callback.onCompletion(metadata, null);
                } else {
                    /*
                     * 如果一个Batch是重试发送出去的，成功了，没有什么特别的，直接就是回调函数，然后就是释放资源，
                     * 那么如果在指定次数内，3次，都没成功，哪怕重试几次都失败了，一定会回调通知你的。
                     * this.accumulator.deallocate(batch); 还是会执行的，还是最终会释放掉这个Batch占用的内存资源的。
                     */
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition {}:", topicPartition, e);
            }
        }
        this.produceFuture.done(topicPartition, baseOffset, exception);
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     * 要传递给它的回调和关联的FutureRecordMetadata参数。
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * 如果满足以下条件之一，则元数据不可用的批处理应过期：
     * <ol>
     *     <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     *     批处理未处于重试状态，并且在准备就绪后请求超时已过。
     *     <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     *     批处理处于重试状态，超过了retry的时间间隔。
     * </ol>
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        boolean expire = false;

        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime)) {
            expire = true;
        } else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs))) {
            expire = true;
        } else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs))) {
            expire = true;
        }

        if (expire) {
            /*
             * 如果说超时，一定会调用回调函数，必须去释放到这个batch的内存资源
             */
            // 释放Buffer
            this.records.close();
            // 执行回调
            this.done(-1L, Record.NO_TIMESTAMP, new TimeoutException("Batch containing " + recordCount + " record(s) expired due to timeout while requesting metadata from brokers for " + topicPartition));
        }

        return expire;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }
}
