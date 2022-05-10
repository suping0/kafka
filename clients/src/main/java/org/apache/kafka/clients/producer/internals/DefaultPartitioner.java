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
package org.apache.kafka.clients.producer.internals;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 * 默认的分区策略。
 *
 * 如果在记录中指定了一个分区，就使用该分区
 * 如果没有指定分区，但有一个密钥，则根据密钥的哈希值选择一个分区。
 * 如果没有分区或密钥，则以循环方式选择一个分区。
 */
public class DefaultPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
     * value.
     *
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition.
     *
     * @param number a given number
     * @return a positive number.
     */
    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            // 未指定分区key
            // 线程安全的递增值
            int nextValue = counter.getAndIncrement();
            // 该topic所有的分区
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                // 计算存储数据的分区号；
                // 根据分区数取模
                // 就是根据这个递增的数字23，路由到5个分区中的1个，3
                // 接下来一条消息，就会递增counter，比如说就是24，4；25 -> 0；26 -> 1；27 -> 2；28 -> 3；29 -> 4；30 -> 0，
                // 所以说采用了一个counter递增的方式，不断的用一个递增的数字来对分区的数量进行取模
                // 保证在不指定分区key的情况下，所有的消息会均匀的分发到各个分区中去
                int part = DefaultPartitioner.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return DefaultPartitioner.toPositive(nextValue) % numPartitions;
            }
        } else {
            // 指定了分区key
            // hash the keyBytes to choose a partition
            // 相同的key会生成相同的hash值，然后在根据numPartitions取模
            return DefaultPartitioner.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    public void close() {}

}
