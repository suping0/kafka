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
package org.apache.kafka.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka自己封装的selector组件，但是呢，他的底层是有最最核心的Java NIO的Selector
 *
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 * 一个nioSelector接口，用于进行非阻塞的多连接网络I/O。
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 * 可以通过以下操作将一个连接添加到与整数id相关联的nioSelector。
 *
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 * connect调用在建立TCP连接时并不阻塞，所以connect方法只是开始发起连接。
 * 该方法的成功调用并不意味着一个有效的连接已经建立。
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 * 在现有连接上发送请求、接收响应、处理连接完成和断开连接都是使用<code>poll()</code>调用完成的
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 * nioSelector维护着几个列表，这些列表通过每次调用<code>poll()</code>而被重置，
 * 这些列表通过各种getters来获得。每次调用<code>poll()</code>都会重新设置这些获取器。
 *
 * This class is not thread safe!
 */
public class Selector implements Selectable {

    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    private final java.nio.channels.Selector nioSelector;
    private final Map<String, KafkaChannel> channels;
    private final List<Send> completedSends;
    private final List<NetworkReceive> completedReceives;
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;
    private final Set<SelectionKey> immediatelyConnectedKeys;
    private final List<String> disconnected;
    private final List<String> connected;
    private final List<String> failedSends;
    private final Time time;
    private final SelectorMetrics sensors;
    private final String metricGrpPrefix;
    private final Map<String, String> metricTags;
    private final ChannelBuilder channelBuilder;
    private final Map<String, Long> lruConnections;
    private final long connectionsMaxIdleNanos;
    private final int maxReceiveSize;
    private final boolean metricsPerConnection;
    private long currentTimeNanos;
    private long nextIdleCloseCheckTime;


    /**
     * Create a new nioSelector
     * kafka自己封装的selector组件，但是呢，他的底层是有最最核心的Java NIO的Selector
     */
    public Selector(int maxReceiveSize, long connectionMaxIdleMs, Metrics metrics, Time time, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection, ChannelBuilder channelBuilder) {
        try {
            // 最最核心的一点，就是在KafkaSelector的底层，其实就是封装了原生的Java NIO的Selector，很关键的组件，
            // 就是一个多路复用组件，他会一个线程调用他直接监听多个网络连接的请求和响应
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        // maxReceiveSize，最大可以接收的数据量的大小
        this.maxReceiveSize = maxReceiveSize;
        // connectionsMaxIdle，每个网络连接最多可以空闲的时间的大小，就要回收掉
        this.connectionsMaxIdleNanos = connectionMaxIdleMs * 1000 * 1000;
        this.time = time;
        this.metricGrpPrefix = metricGrpPrefix;
        this.metricTags = metricTags;
        /*
         * 这里保存了每个broker id到Channel的映射关系，
         * 对于每个broker都有一个网络连接，每个连接在NIO的语义里，都有一个对应的SocketChannel，
         * 我们估计，KafkaChannel封装了SocketChannel
         * Map<String, KafkaChannel> channels;
         * broker id对应一个网络连接，一个网络连接对应一个KafkaChannel
         */
        this.channels = new HashMap<>();
        // 已经成功发送出去的请求
        this.completedSends = new ArrayList<>();
        // 已经接收回来的响应而且被处理完了
        this.completedReceives = new ArrayList<>();
        // 每个Broker的收到的但是还没有被处理的响应
        this.stagedReceives = new HashMap<>();
        this.immediatelyConnectedKeys = new HashSet<>();
        /*
         * conneted、disconnected、failedSends，
         * 已经成功建立连接的brokers，以及还没成功建立连接的brokers，发送请求失败的brokers
         */
        this.connected = new ArrayList<>();
        this.disconnected = new ArrayList<>();
        this.failedSends = new ArrayList<>();

        this.sensors = new SelectorMetrics(metrics);
        this.channelBuilder = channelBuilder;
        // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
        this.lruConnections = new LinkedHashMap<>(16, .75F, true);
        currentTimeNanos = time.nanoseconds();
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
        this.metricsPerConnection = metricsPerConnection;
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, new HashMap<String, String>(), true, channelBuilder);
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * 开始连接到给定的地址，并将连接添加到这个与给定id号关联的nioSelector。
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * 请注意，这个调用只启动了连接，它将在未来的{@link #poll(long)}调用中完成。
     * 检查{@link #connected()}，查看在给定的轮询调用后，哪些（如果有）连接已经完成。
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.channels.containsKey(id)) {
            throw new IllegalStateException("There is already a connection for id " + id);
        }
        /*
         * 对于客户端而言，他的SocketChannel到底应该如何来设置呢？你就可以参考人家做法：KeepAlive、TcpNoDelay、SocketBuffer
         * NetworkClient、Selector、KafkaChannel、ConnectStates，
         * 这些东西是极为值得我们来研究的，对我们的技术底层的功底的夯实极为有好处，假设我们真的要去开发一个网络通信的程序，打算基于NIO来做
         */
        /*
         * 下面的这些建立连接的代码，其实就是java nio的代码
         * 观察一下，在这个工业级的网络通信框架的封装中，对底层的NIO是如何来使用的，一些参数是如何来设置的
         */

        SocketChannel socketChannel = SocketChannel.open();
        // 配置非阻塞连接
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        /*
         * 保持连接
         * 主要是避免客户端和服务端任何一方如果断开连接之后，别人不知道，一直保持着网络连接的资源；
         * 所以设置这个之后，2小时内如果双方没有任何通信，那么发送一个探测包，根据探测包的结果保持连接、重新连接或者断开连接
         */
        socket.setKeepAlive(true);
        /*
         * 需要去设置socket的发送和接收的缓冲区的大小，分别是128kb和32kb，
         * 这个缓冲区的大小一般都是在NIO编程里需要自己去设置的
         */
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
            socket.setSendBufferSize(sendBufferSize);
        }
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
            socket.setReceiveBufferSize(receiveBufferSize);
        }
        /*
         * 如果默认是设置为false的话，那么就开启Nagle算法，就是把网络通信中的一些小的数据包给收集起来，
         * 组装成一个大的数据包然后再一次性的发送出去，如果大量的小包在传递，会导致网络拥塞。
         * 如果设置为true的话，意思就是关闭Nagle，让你发送出去的数据包立马就是通过网络传输过去，所以这个参数大家也要注意下
         */
        socket.setTcpNoDelay(true);
        boolean connected;
        try {
            /*
             * 如果这个SocketChannel是被设置为非阻塞模式的话，那么对这个connect方法的调用，会初始化一个非阻塞的连接请求，
             * 如果这个发起的连接立马就成功了，比如说客户端跟要连接的服务端都在一台机器上
             *
             * 如果这个通道处于非阻塞模式，那么调用这个方法就会启动一个非阻塞连接操作。
             * 如果连接立即建立，就像本地连接一样，那么这个方法返回<tt>true</tt>。
             * 否则这个方法返回<tt>false</tt>，并且连接操作必须随后通过调用{@link #finishConnect finishConnect}方法来完成。
             * 如果该通道处于阻塞模式，那么对该方法的调用将阻塞，直到建立连接或发生I/O错误。
             * 这个方法可以在任何时候被调用。
             * 如果在本方法的调用进行中，调用了本通道的读或写操作，那么该操作将首先阻塞，直到本调用完成。
             * 如果一个连接尝试被发起但失败了，也就是说，如果这个方法的调用引发了一个检查异常，那么这个通道将被关闭。
             */
            // 发起nio的socket connect请求
            connected = socketChannel.connect(address);
        } catch (UnresolvedAddressException e) {
            socketChannel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            socketChannel.close();
            throw e;
        }
        /*
         * 你直接初始化了一个SocketChannel然后就发起了一个连接请求，接着不管连接请求是成功还是暂时没成功，
         * 都需要把这个SocketChannel给缓存起来，接下来你才可以基于这个东西去完成连接，或者是发起读写请求。
         *
         * 发起连接之后，直接就把这个SocketChannel给注册到Selector上去了，
         * 让Selector监视这个SocketChannel的OP_CONNECT事件，就是是否有人同意跟他建立连接，会获取到一个SelectionKey，
         * 大概可以认为这个SelectionKey就是和SocketChannel是一一对应的。
         */
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);
        /*
         * 接着就是将SelectionKey、brokerid封装为了KafkaChannel，
         * 他是先把SelectionKey封装到TransportLayer里面去（SelectionKey底层是跟SocketChannel是一一对应起来），
         * Authenticator，brokerid，直接封装一个KafkaChannel。
         * see : org.apache.kafka.common.network.PlaintextChannelBuilder#buildChannel(java.lang.String, java.nio.channels.SelectionKey, int)
         */
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        /*
         * 大概可以认为是把一个核心的组件跟SelectionKey给关联起来，
         * 后续在通过SelectionKey进行网络请求和相应的处理的时候，
         * 就可以从SelectionKey里获取出来SocketChannel，可以获取出来之前attach过的一个核心组件，复制请求响应的处理
         *
         * 将给定对象附加到这个键上。
         * 附加的对象以后可以通过附加方法检索。一次只能附加一个对象；调用该方法会导致之前的附加被丢弃。当前的附加对象可以通过附加null来丢弃。
         */
        key.attach(channel);
        this.channels.put(id, channel);

        if (connected) {
            // 如果连接被立即建立了，connected为true, 但其实不会立即出发，
            // 具体完成者连接需要到poll方法里才能实现，我们现在先不看。
            // OP_CONNECT won't trigger for immediately connected channels; OP_CONNECT不会触发立即连接的通道。
            log.debug("Immediately connected to node {}", channel.id());
            immediatelyConnectedKeys.add(key);
            // 将此键的兴趣设置为给定值。这个方法可以在任何时候被调用。是否阻塞，以及阻塞多长时间，都取决于实现。
            key.interestOps(0);
        }
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * Note that we are not checking if the connection id is valid - since the connection already exists
     * 用现有通道注册nioSelector。
     * 当一个连接被另一个线程接受但被选择器处理时，在服务器端使用这个选项。
     * 注意，我们没有检查连接id是否有效-因为连接已经存在。
     */
    public void register(String id, SocketChannel socketChannel) throws ClosedChannelException {
        /*
         * kafka使用的是统一的nio封装类
         * 服务端和producer端使用的nio组件一致的
         */
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        // 将SelectionKey和KafkaChannel关联起来
        key.attach(channel);
        // 缓存channel到当前processor上
        this.channels.put(id, channel);
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        for (String id : connections)
            close(id);
        try {
            this.nioSelector.close();
        } catch (IOException | SecurityException e) {
            log.error("Exception closing nioSelector:", e);
        }
        sensors.close();
        channelBuilder.close();
    }

    /**
     * Queue the given request for sending in the subsequent {@link #poll(long)} calls
     * @param send The request to send
     */
    @Override
    public void send(Send send) {
        KafkaChannel channel = channelOrFail(send.destination());
        try {
            // 添加对OP_WRITE事件的监听
            channel.setSend(send);
        } catch (CancelledKeyException e) {
            this.failedSends.add(send.destination());
            close(channel);
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-
     * 在每个连接上做任何可以不阻塞的I/O。
     * 这包括完成连接，完成断开连接，发起新的发送，或对正在进行的发送或接收进行进度。
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each `poll` call and repopulated by the call if there is
     * any completed I/O.
     *
     * In the "Plaintext" setting, we are using socketChannel to read & write to the network. But for the "SSL" setting,
     * we encrypt the data before we use socketChannel to write data to the network, and decrypt before we return the responses.
     * This requires additional buffers to be maintained as we are reading from network, since the data on the wire is encrypted
     * we won't be able to read exact no.of bytes as kafka protocol requires. We read as many bytes as we can, up to SSLEngine's
     * application buffer size. This means we might be reading additional bytes than the requested size.
     * If there is no further data to read from socketChannel selector won't invoke that channel and we've have additional bytes
     * in the buffer. To overcome this issue we added "stagedReceives" map which contains per-channel deque. When we are
     * reading a channel we read as many responses as we can and store them into "stagedReceives" and pop one response during
     * the poll to add the completedReceives. If there are any active channels in the "stagedReceives" we set "timeout" to 0
     * and pop response and add to the completedReceives.
     *
     * @param timeout The amount of time to wait, in milliseconds, which must be non-negative
     * @throws IllegalArgumentException If `timeout` is negative
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout should be >= 0");
        }

        clear();

        if (hasStagedReceives() || !immediatelyConnectedKeys.isEmpty()) {
            timeout = 0;
        }

        /* check ready keys */
        long startSelect = time.nanoseconds();
        /*
         * 调用Java NIO的Selector.select ->
         * 他会负责去看看，注册到他这里的多个Channel，谁有响应过来可以接收，或者谁现在可以执行一个请求的发送，
         * 如果Channel可以准备执行IO读写操作，此时就把那个Channel的SelectionKey返回。
         * 检查数据，等待到给定的超时。如果有返回（连接/响应）则返回
         */
        int readyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        currentTimeNanos = endSelect;
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        if (readyKeys > 0 || !immediatelyConnectedKeys.isEmpty()) {
            /*
             * 接下来就会对获取到的一堆SelectionKeys进行处理，
             * 到这一步为止，我们就可以看到基于NIO来开发的很多企业级的一些功能，
             * 一个是SocketChannel如何构建，二个是一个客户端如何连接多个服务器，三个如何通过轮询调用Selector.select
             */
            pollSelectionKeys(this.nioSelector.selectedKeys(), false);
            pollSelectionKeys(immediatelyConnectedKeys, true);
        }

        // 检查是否有任何 staged receives(返回的响应)，并添加到completedReceives
        addToCompletedReceives();

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());
        // 根据lur算法，断开空闲事件超过9分钟的连接
        maybeCloseOldestConnection();
    }

    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys, boolean isImmediatelyConnected) {
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        /*
         * 迭代处理所有的SelectionKey,包括以下逻辑
         * 1. 建立连接
         * 2. 读取数据
         * 3. 发送数据
         * 通过producer一个线程，处理维护的所有的连接
         */
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            // 根据selectKey获取到attach进去的channel
            KafkaChannel channel = channel(key);

            // register all per-connection metrics at once; 一次注册所有的每个连接指标
            sensors.maybeRegisterConnectionMetrics(channel.id());
            /*
             * lruConnections，因为一般来说一个客户端不能放太多的Socket连接资源，否则会导致这个客户端的复杂过重，
             * 所以他需要采用lru的方式来不断的淘汰掉最近最少使用的一些连接，很多连接最近没怎么发送消息。
             * 比如说有一个连接，最近一次使用是在1个小时之前了，还有一个连接，最近一次使用是在1分钟之前，
             * 此时如果要淘汰掉一个连接，你会选择谁？LRU算法，明显是淘汰掉那个1小时之前才使用的连接。
             *
             * 比如说有一个连接，最近一次使用是在1个小时之前了，还有一个连接，最近一次使用是在1分钟之前，
             * 此时如果要淘汰掉一个连接，你会选择谁？LRU算法，明显是淘汰掉那个1小时之前才使用的连接。
             * channel.id() 是 brokerId
             */
            lruConnections.put(channel.id(), currentTimeNanos);

            try {

                /* complete any connections that have finished their handshake (either normally or immediately)
                * 完成所有已完成握手的连接（正常或立即）。
                * */
                if (isImmediatelyConnected || key.isConnectable()) {
                    /*
                     * 如果发现SelectionKey当前处于的状态是可以建立连接，isConnectable方法是true，
                     * 接着其实就是调用到KafkaChannel最底层的SocketChannel的finishConnect方法，等待这个连接必须执行完毕。
                     * 同时接下来就不要关注OP_CONNECT事件了，对于这个Channel，接下来Selector就不要关注连接相关的事件了，
                     * 也不是OP_READ读取事件，肯定selector要关注的是OP_WRITE事件，要针对这个连接写数据
                     */
                    // 建立连接后的，关注OP_READ事件
                    if (channel.finishConnect()) {
                        // 缓存连接
                        this.connected.add(channel.id());
                        this.sensors.connectionCreated.record();
                    } else {
                        continue;
                    }
                }

                /* if channel is not ready finish prepare 如果通道没有准备好，就完成准备 */
                if (channel.isConnected() && !channel.ready()) {
                    channel.prepare();
                }

                /* if channel is ready read from any connections that have readable data
                   如果通道准备好了，则从任何有可读数据的连接处读取。 */
                if (channel.ready() && key.isReadable() && !hasStagedReceive(channel)) {
                    NetworkReceive networkReceive;
                    /*
                     * 如果说broker给你返回了响应消息，那么你一定会感知到一个OP_READ事件，在这里会使用while循环，
                     * 针对一个broker的连接，反复的读。推测一下，
                     * 你的一个broker是可以通过一个连接连续发送出去多个请求的，这个多个请求可能都没有收到响应消息，
                     * 此时人家broker端可能会连续处理完多个请求然后连续返回多个响应给你，所以在这里，你一旦去读数据，
                     * 可能会连续读到多个请求的响应。
                     * 所以说在这里处理OP_READ事件的时候，必须要通过一个while循环，连续不断的读，
                     * 可能会读到多个响应消息，全部放到一个暂存的集合里，stagedReceives。
                     */
                    /*
                     * channel.read() // 循环读取所有的响应
                     * 这段代码，你在外面绝对见不到的，完美的处理了发送请求和读取响应的粘包和拆包的问题，
                     * 用NIO来编程，主要要自己考虑的其实就是粘包和拆包的问题
                     */
                    while ((networkReceive = channel.read()) != null) {
                        addToStagedReceives(channel, networkReceive);
                    }
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data
                 * 如果通道已准备好写入缓冲区中有空间且有数据的任何套接字
                 * key.isWritable() 表示SelectionKey有OP_WRITE事件
                 * */
                if (channel.ready() && key.isWritable()) {
                    // 发送数据
                    /*
                     * 如果说已经发送完毕数据了，那么就可以取消对OP_WRITE事件的关注，
                     * 否则如果一个Request的数据都没发送完毕，此时还需要保持对OP_WRITE事件的关注。
                     * 而且如果发送完毕了，就会放到completedSends里面去
                     */
                    // 如果没有完成数据的发送，则返回null
                    Send send = channel.write();
                    if (send != null) {
                        // 如果发送完毕了，就会放到completedSends里面去。
                        this.completedSends.add(send);
                        this.sensors.recordBytesSent(channel.id(), send.size());
                    }
                }

                /* cancel any defunct sockets */
                if (!key.isValid()) {
                    close(channel);
                    this.disconnected.add(channel.id());
                }

            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException) {
                    log.debug("Connection with {} disconnected", desc, e);
                } else {
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                }
                close(channel);
                this.disconnected.add(channel.id());
            }
        }
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public List<String> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = channelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = channelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        channel.unmute();
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    private void maybeCloseOldestConnection() {
        if (currentTimeNanos > nextIdleCloseCheckTime) {
            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
            } else {
                Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
                Long connectionLastActiveTime = oldestConnectionEntry.getValue();
                nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;
                if (currentTimeNanos > nextIdleCloseCheckTime) {
                    String connectionId = oldestConnectionEntry.getKey();
                    if (log.isTraceEnabled()) {
                        log.trace("About to close the idle connection from " + connectionId
                                + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis");
                    }

                    disconnected.add(connectionId);
                    close(connectionId);
                }
            }
        }
    }

    /**
     * Clear the results from the prior poll
     */
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        this.disconnected.addAll(this.failedSends);
        this.failedSends.clear();
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param ms Length of time to wait, in milliseconds, which must be non-negative
     * @return The number of keys ready
     * @throws IllegalArgumentException
     * @throws IOException
     */
    private int select(long ms) throws IOException {
        if (ms < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        if (ms == 0L)
            return this.nioSelector.selectNow();
        else
            return this.nioSelector.select(ms);
    }

    /**
     * Close the connection identified by the given id
     */
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null)
            close(channel);
    }

    /**
     * Begin closing this connection
     */
    private void close(KafkaChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        }
        this.stagedReceives.remove(channel);
        this.channels.remove(channel.id());
        this.lruConnections.remove(channel.id());
        this.sensors.connectionClosed.record();
    }


    /**
     * check if channel is ready
     */
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    private KafkaChannel channelOrFail(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no open connection. Connection id " + id + " existing connections " + channels.keySet());
        return channel;
    }

    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Get the channel associated with selectionKey
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a staged receive
     * 检查给定通道是否有分级接收
     */
    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
    }

    /**
     * check if stagedReceives have unmuted channel
     */
    private boolean hasStagedReceives() {
        for (KafkaChannel channel : this.stagedReceives.keySet()) {
            if (!channel.isMute())
                return true;
        }
        return false;
    }


    /**
     * adds a receive to staged receives
     */
    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        if (!stagedReceives.containsKey(channel)) {
            stagedReceives.put(channel, new ArrayDeque<NetworkReceive>());
        }

        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        deque.add(receive);
    }

    /**
     * checks if there are any staged receives and adds to completedReceives
     * 检查是否有任何staged receives，并添加到completedReceives。
     */
    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iter = this.stagedReceives.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iter.next();
                KafkaChannel channel = entry.getKey();
                if (!channel.isMute()) {
                    // 只要当channel关注OP_READ事件时，才会将请求或者响应放到completedReceives中
                    // 只有当响应处理完了，才会让channel重新关注OP_READ
                    // 这样就是说，客户端会同时发送多个请求过来，积压在stagedReceives中，但是一次只会只会处理一个请求
                    Deque<NetworkReceive> deque = entry.getValue();
                    /*
                     * 如果一个连接一次OP_READ读取出来多个响应消息的话，
                     * 在这里仅仅只会把每个连接对应的第一个响应消息会放到completedReceives里面去，放到后面去进行处理，
                     * 此时有可能某个连接的stagedReceives是不为空的
                     */
                    // 一个客户端一次只会拿出一个请求出来
                    NetworkReceive networkReceive = deque.poll();
                    // 添加到已收到响应队列中
                    this.completedReceives.add(networkReceive);
                    this.sensors.recordBytesReceived(channel.id(), networkReceive.payload().limit());
                    if (deque.isEmpty()) {
                        iter.remove();
                    }
                }
            }
        }
    }


    private class SelectorMetrics {
        private final Metrics metrics;
        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor bytesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = metricGrpPrefix + "-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix.toString());
            MetricName metricName = metrics.metricName("connection-close-rate", metricGrpName, "Connections closed per second in the window.", metricTags);
            this.connectionClosed.add(metricName, new Rate());

            this.connectionCreated = sensor("connections-created:" + tagsSuffix.toString());
            metricName = metrics.metricName("connection-creation-rate", metricGrpName, "New connections established per second in the window.", metricTags);
            this.connectionCreated.add(metricName, new Rate());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix.toString());
            metricName = metrics.metricName("network-io-rate", metricGrpName, "The average number of network operations (reads or writes) on all connections per second.", metricTags);
            bytesTransferred.add(metricName, new Rate(new Count()));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, "The average number of outgoing bytes sent per second to all servers.", metricTags);
            this.bytesSent.add(metricName, new Rate());
            metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", metricTags);
            this.bytesSent.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("incoming-byte-rate", metricGrpName, "Bytes/second read off all sockets", metricTags);
            this.bytesReceived.add(metricName, new Rate());
            metricName = metrics.metricName("response-rate", metricGrpName, "Responses received sent per second.", metricTags);
            this.bytesReceived.add(metricName, new Rate(new Count()));

            this.selectTime = sensor("select-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("select-rate", metricGrpName, "Number of times the I/O layer checked for new I/O to perform per second", metricTags);
            this.selectTime.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-wait-ratio", metricGrpName, "The fraction of time the I/O thread spent waiting.", metricTags);
            this.selectTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            this.ioTime = sensor("io-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-ratio", metricGrpName, "The fraction of time the I/O thread spent doing I/O", metricTags);
            this.ioTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return channels.size();
                }
            });
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    MetricName metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, tags);
                    nodeRequest.add(metricName, new Rate());
                    metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", tags);
                    nodeRequest.add(metricName, new Rate(new Count()));
                    metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + connectionId + ".bytes-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    metricName = metrics.metricName("incoming-byte-rate", metricGrpName, tags);
                    nodeResponse.add(metricName, new Rate());
                    metricName = metrics.metricName("response-rate", metricGrpName, "The average number of responses received per second.", tags);
                    nodeResponse.add(metricName, new Rate(new Count()));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void recordBytesReceived(String connection, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (!connection.isEmpty()) {
                String nodeRequestName = "node-" + connection + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
        }
    }

}
