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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface is used to define custom partition assignment for use in
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer}. Members of the consumer group subscribe
 * to the topics they are interested in and forward their subscriptions to a Kafka broker serving
 * as the group coordinator. The coordinator selects one member to perform the group assignment and
 * propagates the subscriptions of all members to it. Then {@link #assign(Cluster, Map)} is called
 * to perform the assignment and the results are forwarded back to each respective members
 * 此接口用于定义在{@link org.apache.kafka.clients.consumer.KafkaConsumer}中使用的消费分区分配.
 *
 * 消费者组的成员订阅他们感兴趣的topic，并将订阅转发给作为group coordinator的Kafka broker。
 * coordinator选择一个member(消费者组中的一员)来执行组分配，并向其传播所有成员的订阅。
 * 然后调用{@link #assign(Cluster, Map)}来执行assignment，结果被转发回各个members。
 *
 * In some cases, it is useful to forward additional metadata to the assignor in order to make assignment decisions.
 * For this, you can override {@link #subscription(Set)} and provide custom userData in the returned Subscription.
 * For example, to have a rack-aware assignor, an implementation can use this user data to forward the rackId belonging to each member.
 * 在某些情况下，将额外的元数据转发给分配者(assignor)以做出分配决策是有用的。
 * 为此，可以重写{@link #subscription(Set)}，并在返回的订阅中提供自定义用户数据。
 * 例如，为了具有机架感知的assignor，实现可以使用此用户数据转发属于每个成员的rackId。
 */
public interface PartitionAssignor {

    /**
     * Return a serializable object representing the local member's subscription. This can include
     * additional information as well (e.g. local host/rack information) which can be leveraged in
     * {@link #assign(Cluster, Map)}.
     * @param topics Topics subscribed to through {@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection)}
     *               and variants
     * @return Non-null subscription with optional user data
     */
    Subscription subscription(Set<String> topics);

    /**
     * Perform the group assignment given the member subscriptions and current cluster metadata.
     * 在给定成员订阅和当前群集元数据的情况下执行组分配。
     * @param metadata Current topic/broker metadata known by consumer
     * @param subscriptions Subscriptions from all members provided through {@link #subscription(Set)}
     * @return A map from the members to their respective assignment. This should have one entry
     *         for all members who in the input subscription map.
     *         从成员到各自assignment的map映射。对于输入订阅映射中的所有成员，应该有一个条目。
     */
    Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions);


    /**
     * Callback which is invoked when a group member receives its assignment from the leader.
     * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, Map)}
     */
    void onAssignment(Assignment assignment);


    /**
     * Unique name for this assignor (e.g. "range" or "roundrobin")
     * @return non-null unique name
     */
    String name();

    class Subscription {
        private final List<String> topics;
        private final ByteBuffer userData;

        public Subscription(List<String> topics, ByteBuffer userData) {
            this.topics = topics;
            this.userData = userData;
        }

        public Subscription(List<String> topics) {
            this(topics, ByteBuffer.wrap(new byte[0]));
        }

        public List<String> topics() {
            return topics;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                    "topics=" + topics +
                    ')';
        }
    }

    class Assignment {
        private final List<TopicPartition> partitions;
        private final ByteBuffer userData;

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData) {
            this.partitions = partitions;
            this.userData = userData;
        }

        public Assignment(List<TopicPartition> partitions) {
            this(partitions, ByteBuffer.wrap(new byte[0]));
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                    "partitions=" + partitions +
                    ')';
        }
    }

}
