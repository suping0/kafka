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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * 一个用于异步请求/响应网络i/o的网络客户端。这是一个内部类，用于实现面向用户的生产者和消费者客户端。
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    /* the selector used to perform network i/o */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* max time in ms for the producer to wait for acknowledgement from server*/
    private final int requestTimeoutMs;

    private final Time time;

    /**
     * NetworkClient主要是一个网络通信组件，
     * 底层核心的Selector负责最最核心的建立连接、发起请求、处理实际的网络IO，初始化的入口初步找到了
     * @param selector
     * @param metadata
     * @param clientId
     * @param maxInFlightRequestsPerConnection
     * @param reconnectBackoffMs
     * @param socketSendBuffer
     * @param socketReceiveBuffer
     * @param requestTimeoutMs
     * @param time
     */
    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this(null, metadata, selector, clientId, maxInFlightRequestsPerConnection,
                reconnectBackoffMs, socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this(metadataUpdater, null, selector, clientId, maxInFlightRequestsPerConnection, reconnectBackoffMs,
                socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    private NetworkClient(MetadataUpdater metadataUpdater,
                          Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int requestTimeoutMs,
                          Time time) {

        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.requestTimeoutMs = requestTimeoutMs;
        this.time = time;
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     * 开始连接到给定的节点，如果我们已经连接好并准备发送至该节点，则返回true。
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty()) {
            throw new IllegalArgumentException("Cannot connect to empty node " + node);
        }

        if (isReady(node, now)) {
            // 检查给定id的节点是否已经准备好发送更多的请求。
            return true;
        }
        /*
         * 要不然是已经建立好连接，底层的NIO Channel是ok的，inFlighRequests没有满5个，此时就可以针对这个Broker去发送一个请求过去了
         * 但是如果上述条件不满足，假设是因为还没有建立连接，此时如何判断是否可以跟一个Broker建立连接呢？
         */
        if (connectionStates.canConnect(node.idString(), now)) {
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            // 如果我们有兴趣向一个节点发送消息，但我们没有连接到它，那么发起一个
            initiateConnect(node, now);
        }

        return false;
    }

    /**
     * Closes the connection to a particular node (if there is one).
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        for (ClientRequest request : inFlightRequests.clearAll(nodeId))
            metadataUpdater.maybeHandleDisconnection(request);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.connectionState(node.idString()).equals(ConnectionState.DISCONNECTED);
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first priority
        // 如果我们需要更新我们的元数据，现在声明所有的请求未准备好，使元数据请求成为第一优先级
        // canSendRequest(node.idString()) 我们是否已经连接并准备好了，能够向给定的连接发送更多的请求？
        /*
         * !metadataUpdater.isUpdateDue(now) -> 如果条件判断是非的话，要不然是正在加载元数据，或者是还没到加载元数据的时候(元数据未更新)
         * 为什么前面一定要有这个条件？
         * 假设此时必须要更新元数据了，就不能发送请求，必须要等待这个元数据被刷新了再次去发送请求
         */
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString());
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     */
    private boolean canSendRequest(String node) {
        /*
         * （1）有一个Broker连接状态的缓存，先查一下这个缓存，当前这个Broker是否已经建立了连接了，如果是的话，才可以继续判断其他的条件、
         * （2）Selector，你大概可以认为底层封装的就是Java NIO的 Selector，但凡是看过我的NIO课程，跟着做NIO研发分布式文件系统，
         *      Selector上要注册很多Channel，每个Channel就代表了跟一个Broker建立的连接。
         * （3）inFlightRequests，有一个参数可以设置这个东西，默认是对同一个Broker同一时间最多容忍5个请求发送过去但是还没有收到响应，
         *      所以如果对一个Broker已经发送了5个请求，都没收到响应，此时就不可以继续发送了。
         */
        return connectionStates.isConnected(node) && selector.isChannelReady(node) && inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     *
     * @param request The request
     * @param now The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        String nodeId = request.request().destination();
        if (!canSendRequest(nodeId)) {
            throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        /*
         * 看看依托于KafkaChannel和NIO selector多路复用的机制，
         * 其实就是依托inFlightRequests去暂存了正在发送的Request
         *
         * 要发送请求的时候，会把这个请求暂存到KafkaChannel里去，
         * 同时让Selector监视他的OP_WRITE事件，增加一种OP_WRITE事件，
         * 同时保留了OP_READ事件，此时Selector会同时监听这个连接的OP_WRITE和OP_READ事件
         */
        doSend(request, now);
    }

    private void doSend(ClientRequest request, long now) {
        request.setSendTimeMs(now);
        this.inFlightRequests.add(request);
        // 增加一种OP_WRITE事件的监听
        selector.send(request.request());
    }

    /**
     * Do actual reads and writes to sockets.
     * 对socket进行实际的读写。
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        // 更新元数据的请求
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            // 发送请求的核心发送
            this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        // 处理响应
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        // 处理任何已完成的请求发送。特别是如果预期没有响应，则认为请求已完成。
        handleCompletedSends(responses, updatedNow);
        // 处理任何已完成的接收，并根据收到的responses更新response清单。
        handleCompletedReceives(responses, updatedNow);
        // 处理任何断开的连接
        handleDisconnections(responses, updatedNow);
        // 与broker创建好连接后，修改broker的连接状态为已连接
        handleConnections();
        // 迭代所有inflight requests，并使任何超过配置的requestTimeout的请求过期。
        // 与请求关联的节点的连接将被终止，并将被视为断开连接。
        handleTimedOutRequests(responses, updatedNow);

        // invoke callbacks
        // 调用回调函数
        for (ClientResponse response : responses) {
            if (response.request().hasCallback()) {
                try {
                    // 执行每个消息的的回调函数
                    response.request().callback().onComplete(response);
                } catch (Exception e) {
                    log.error("Uncaught error in request completion:", e);
                }
            }
        }

        return responses;
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.inFlightRequestCount();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.inFlightRequestCount(node);
    }

    /**
     * Generate a request header for the given API key
     *
     * @param key The api key
     * @return A request header with the appropriate client id and correlation id
     */
    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, clientId, correlation++);
    }

    /**
     * Generate a request header for the given API key and version
     *
     * @param key The api key
     * @param version The api version
     * @return A request header with the appropriate client id and correlation id
     */
    @Override
    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id, version, clientId, correlation++);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        this.selector.close();
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period.
     * 选择未完成请求最少且至少符合连接条件的节点。
     * 此方法将首选具有现有连接的节点，但如果所有现有连接都在使用中，则可能会选择尚未连接的节点。
     * 此方法永远不会选择一个没有现有连接且在重新连接退避期间已断开连接的节点。
     *
     * @return The node with the fewest in-flight requests.
     */
    @Override
    public Node leastLoadedNode(long now) {
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        int inflight = Integer.MAX_VALUE;
        Node found = null;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            int currInflight = this.inFlightRequests.inFlightRequestCount(node.idString());
            if (currInflight == 0 && this.connectionStates.isConnected(node.idString())) {
                // if we find an established connection with no in-flight requests we can stop right away
                return node;
            } else if (!this.connectionStates.isBlackedOut(node.idString(), now) && currInflight < inflight) {
                // otherwise if this is the best we have found so far, record that
                inflight = currInflight;
                found = node;
            }
        }

        return found;
    }

    public static Struct parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
        // Always expect the response version id to be the same as the request version id
        short apiKey = requestHeader.apiKey();
        short apiVer = requestHeader.apiVersion();
        Struct responseBody = ProtoUtils.responseSchema(apiKey, apiVer).read(responseBuffer);
        // 验证响应是否与我们期望的请求相对应，否则会报错
        correlate(requestHeader, responseHeader);
        return responseBody;
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    private void processDisconnection(List<ClientResponse> responses, String nodeId, long now) {
        connectionStates.disconnected(nodeId, now);
        for (ClientRequest request : this.inFlightRequests.clearAll(nodeId)) {
            log.trace("Cancelled request {} due to node {} being disconnected", request, nodeId);
            if (!metadataUpdater.maybeHandleDisconnection(request)) {
                // 添加一个disconnected响应
                responses.add(new ClientResponse(request, now, true, null));
            }
        }
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        // 得到请求超时的nodes
        List<String> nodeIds = this.inFlightRequests.getNodesWithTimedOutRequests(now, this.requestTimeoutMs);
        for (String nodeId : nodeIds) {
            /*
             * 如果说发现有节点对请求是超时响应的，过了60s还没响应，
             * 此时会关闭掉跟那个Broker的连接，认为那个Broker已经故障了，
             * 做很多内存数据结构的清理，
             * 再次标记为需要去重新拉取元数据。
             */
            // close connection to the node
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            processDisconnection(responses, nodeId, now);
        }

        // we disconnected, so we should probably refresh our metadata
        if (nodeIds.size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            // 实际上就是在刚刚的那个poll方法里，对一个broker发送出去的request
            ClientRequest request = this.inFlightRequests.lastSent(send.destination());
            /*
             * expectResponse应该是通过acks计算出来的，如果说acks = 0的话，
             * 也就是不需要对一个请求接收响应，此时expectResponse应该就是false，
             * 这个时候直接就会把这个Request从inFlightRequests里面移出去, 直接就可以返回一个响应了，其实就是做一个回调。
             */
            if (!request.expectResponse()) {
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(new ClientResponse(request, now, false, null));
            }
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     * 处理任何已完成的接收，并根据收到的responses更新response清单。
     * 其实在这里，仅仅是解析一个响应，还没有对响应进行处理呢！
     * 主要就是对获取到的请求进行二进制字节数组的解析，把人家回传过来的数据给解析出来，把响应和请求一一匹配起来，
     * 一次请求是对应的每个Partition会有一个Batch放在这个请求里。
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            /*
             * 从inFlightRequests中，移除掉一个request，腾出来一个位置，其中的一个请求是获取到了响应消息了，
             * 不管是不是成功。
             * 你一定是可以在inFlighRequest里面是知道他对应的请求的，
             * 对于同一个broker，连续发送多个request出去，但是会在inFlighRequest里面排队，
             * inFlighRequests -> <请求1，请求2，请求3，请求4，请求5>。
             * 此时对broker读取响应，响应1，响应2，都在stagedReceives，响应1放在completedReceives，只会获取到响应1
             * 就是直接从inFlighRequests里面移除掉请求1，按照顺序，先发送请求1，那么就应该先获取到请求1对应的响应1，而不是响应2
             */
            ClientRequest req = inFlightRequests.completeNext(source);
            /*
             * 去解析他的响应，读取到的数据一定是一段二进制字节数组的一段数据
             * 这段数据一定是按照人家的二进制协议来定义的，比如说返回什么什么东西，什么什么东西，
             * 把这段二进制的字节数组，一点一点从里面，先读取8个字节，代表了什么，再读取20个字节，代表了什么，
             * 放到一个Java对象里去，就代表了他的响应消息
             */
            Struct body = parseResponse(receive.payload(), req.request().header());
            if (!metadataUpdater.maybeHandleCompletedReceive(req, now, body)) {
                // 将解析完的响应放到response中
                responses.add(new ClientResponse(req, now, false, body));
            }
        }
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (String node : this.selector.disconnected()) {
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now);
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        if (this.selector.disconnected().size() > 0){
            /*
            如果在发送数据到broker时，发送失败导致连接断开，则一定会去拉取元数据
             */
            metadataUpdater.requestUpdate();
        }
    }

    /**
     * Record any newly completed connections
     */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            log.debug("Completed connection to node {}", node);
            this.connectionStates.connected(node);
        }
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     * 验证响应是否与我们期望的请求相对应，否则会爆炸
     */
    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + ")");
    }

    /**
     * Initiate a connection to the given node
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            log.debug("Initiating connection to node {} at {}:{}.", node.id(), node.host(), node.port());
            /*
             * 有一个broker的连接状态，是有一个设计模式在里面，状态机的模式，类似于我们一直说的那个状态模式，
             * 抽取和封装一个组件的多个状态，然后通过一个状态机管理组件，可以让这个状态可以互相流转。
             * null，CONNECTING，CONNECTED，DISCONNECTED，针对不同的状态，还可以做不同的事情，
             * 如果是null就可以发起连接，如果连接成功，就可以进入已连接的状态，如果中间发生连接的故障，就进入连接失败
             */
            this.connectionStates.connecting(nodeConnectionId, now);
            /*
             * 底层建立的都是Socket连接，发送请求也是通过底层的Socket来走的，收取数据也是通过Socket读取的，
             * 在工业级的网络通信的开发里面，两个核心的参数必须设置的，就是对于Socket的发送和接收的缓冲区。
             * Selector的组件进行连接，NIO建立连接他其实就是在底层初始化一个SocketChannel发起一个连接的请求，
             * 就会把SocketChannel给注册到Selector上面去，让Selector监听他的建立连接的事件。
             * 如果Broker返回响应说可以建立连接，Selector就会告诉你，你就可以通过一个API的调用，完成底层的网络连接，
             * TCP三次握手，双方都有一个Socket（Socket 操作系统级别的概念，Socket代表了网络通信终端）
             */
            // Selector的组件来进行的，是怎么来的，就得先分析一下Network初始化的过程
            // 建立scoket连接
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            connectionStates.disconnected(nodeConnectionId, now);
            /* maybe the problem is our metadata, update it */
            metadataUpdater.requestUpdate();
            log.debug("Error connecting to node {} at {}:{}:", node.id(), node.host(), node.port(), e);
        }
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        /* true iff there is a metadata request that has been sent and for which we have not yet received a response
        * 如果发送了元数据请求，但我们还没有收到响应，则为true。
        * */
        private boolean metadataFetchInProgress;

        /* the last timestamp when no broker node is available to connect */
        private long lastNoNodeAvailableMs;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.metadataFetchInProgress = false;
            this.lastNoNodeAvailableMs = 0;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            /*
             * 条件1：元数据没有在更新，为true
             * 条件2：现在没有加载元数据，但是马上就应该要加载元数据了
             * 如果对上述条件判断是非的话，要不然是正在加载元数据，或者是还没到加载元数据的时候
             */
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            long timeToNextReconnectAttempt = Math.max(this.lastNoNodeAvailableMs + metadata.refreshBackoff() - now, 0);
            long waitForMetadataFetch = this.metadataFetchInProgress ? Integer.MAX_VALUE : 0;
            // if there is no node available to connect, back off refreshing metadata
            long metadataTimeout = Math.max(Math.max(timeToNextMetadataUpdate, timeToNextReconnectAttempt),
                    waitForMetadataFetch);

            if (metadataTimeout == 0) {
                // Beware that the behavior of this method and the computation of timeouts for poll() are
                // highly dependent on the behavior of leastLoadedNode.
                // 请注意，这个方法的行为和poll()的超时计算高度依赖于leastLoadedNode的行为。
                Node node = leastLoadedNode(now);
                // 添加元数据请求到列表中
                maybeUpdate(now, node);
            }

            return metadataTimeout;
        }

        @Override
        public boolean maybeHandleDisconnection(ClientRequest request) {
            ApiKeys requestKey = ApiKeys.forId(request.request().header().apiKey());

            if (requestKey == ApiKeys.METADATA) {
                Cluster cluster = metadata.fetch();
                if (cluster.isBootstrapConfigured()) {
                    int nodeId = Integer.parseInt(request.request().destination());
                    Node node = cluster.nodeById(nodeId);
                    if (node != null) {
                        log.warn("Bootstrap broker {}:{} disconnected", node.host(), node.port());
                    }
                }

                metadataFetchInProgress = false;
                return true;
            }

            return false;
        }

        @Override
        public boolean maybeHandleCompletedReceive(ClientRequest req, long now, Struct body) {
            short apiKey = req.request().header().apiKey();
            if (apiKey == ApiKeys.METADATA.id && req.isInitiatedByNetworkClient()) {
                // 处理响应
                handleResponse(req.request().header(), body, now);
                return true;
            }
            return false;
        }

        @Override
        public void requestUpdate() {
            this.metadata.requestUpdate();
        }

        private void handleResponse(RequestHeader header, Struct body, long now) {
            this.metadataFetchInProgress = false;
            MetadataResponse response = new MetadataResponse(body);
            Cluster cluster = response.cluster();
            // check if any topics metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", header.correlationId(), errors);

            // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            if (cluster.nodes().size() > 0) {
                // 更新元数据
                this.metadata.update(cluster, now);
            } else {
                log.trace("Ignoring empty metadata response with correlation id {}.", header.correlationId());
                this.metadata.failedUpdate(now);
            }
        }

        /**
         * Create a metadata request for the given topics
         */
        private ClientRequest request(long now, String node, MetadataRequest metadata) {
            RequestSend send = new RequestSend(node, nextRequestHeader(ApiKeys.METADATA), metadata.toStruct());
            return new ClientRequest(now, true, send, null, true);
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         * 在发送列表中添加元数据请求，如果我们能做一个的话
         */
        private void maybeUpdate(long now, Node node) {
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                // mark the timestamp for no node available to connect
                this.lastNoNodeAvailableMs = now;
                return;
            }
            String nodeConnectionId = node.idString();

            if (canSendRequest(nodeConnectionId)) {
                this.metadataFetchInProgress = true;
                MetadataRequest metadataRequest;
                if (metadata.needMetadataForAllTopics()) {
                    metadataRequest = MetadataRequest.allTopics();
                } else {
                    metadataRequest = new MetadataRequest(new ArrayList<>(metadata.topics()));
                }
                ClientRequest clientRequest = request(now, nodeConnectionId, metadataRequest);
                log.debug("Sending metadata request {} to node {}", metadataRequest, node.id());
                // 发送请求
                doSend(clientRequest, now);
            } else if (connectionStates.canConnect(nodeConnectionId, now)) {
                // we don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node.id());
                initiateConnect(node, now);
                // If initiateConnect failed immediately, this node will be put into blackout and we
                // should allow immediately retrying in case there is another candidate node. If it
                // is still connecting, the worst case is that we end up setting a longer timeout
                // on the next round and then wait for the response.
            } else { // connected, but can't send more OR connecting
                // In either case, we just need to wait for a network event to let us know the selected
                // connection might be usable again.
                this.lastNoNodeAvailableMs = now;
            }
        }

    }

}
