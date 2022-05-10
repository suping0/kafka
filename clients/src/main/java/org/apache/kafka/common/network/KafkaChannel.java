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

package org.apache.kafka.common.network;


import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;

public class KafkaChannel {
    /**
     * broker id对应一个网络连接，一个网络连接对应一个KafkaChannel，
     * 底层对应的是SocketChannel，SocketChannel对应的是最最底层的网络通信层面的一个Socket，套接字通信，Socket通信，TCP
     */
    private final String id;
    /**
     * 是封装了底层的Java NIO的SocketChannel
     */
    private final TransportLayer transportLayer;
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    private NetworkReceive receive;
    /**
     * 应该是说要交给这个底层的Channel发送出去的请求，可能会不断的变换的，
     * 因为发送完一个请求需要发送下一个请求
     * 封装请求数据
     */
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute() {
        return transportLayer.isMute();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    public void setSend(Send send) {
        if (this.send != null) {
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        }
        this.send = send;
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 读取数据的时候，其实可能会遇到连续不断的多个响应粘在一起给你返回回来的，就是你在这里读取，
     * 可能会在不停的读取的过程中发现你读到了多个响应消息，这个就是类似于粘包的问题。
     * 发送请求，是不会出现粘包类的问题，你自己是可以控制一次只能把一个请求给人家发送过去，
     * 你只会出现拆包类的问题，就是一个请求一次没有发送完毕，就需要通过执行多次OP_WRITE事件才能发送出去。
     * 但是在读取响应的时候，有可能会出现拆包问题，有可能会出现粘包。
     * @return
     * @throws IOException
     */
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }
        // 读取响应数据（包括处理粘包拆包问题）
        receive(receive);
        if (receive.complete()) {
            // size（读取响应数据大小）, buffer（读取响应的具体值）这两个都读满了。
            // rewind(); 将position重置为0
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        // 返回读取到的一个完成的响应
        return result;
    }

    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        // 如果没有完成数据的发送，则返回null
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    private boolean send(Send send) throws IOException {
        /*
         * 如果说已经发送完毕数据了，那么就可以取消对OP_WRITE事件的关注，
         * 否则如果一个Request的数据都没发送完毕，此时还需要保持对OP_WRITE事件的关注，
         */
        send.writeTo(transportLayer);
        /*
         * 如果说一个请求对应的ByteBuffer中的二进制字节数据一次write没有全部发送完毕，
         * 如果说一次请求没有发送完毕，此时肯定remaining是大于0，此时就不会取消对OP_WRITE事件的监听。
         * 防止出现拆包问题
         */
        if (send.completed()) {
            // 一旦写完请求之后，就会把OP_WRITE事件取消监听，就是此时不关注这个写请求的事件了，此时仅仅保留关注OP_READ事件
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        }

        return send.completed();
    }

}
