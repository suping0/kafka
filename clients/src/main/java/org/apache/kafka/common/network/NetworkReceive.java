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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 * 一种以大小分隔的接收，由4字节网络有序大小N和N字节内容组成
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        return !size.hasRemaining() && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
    }

    //
    /**
     * Need a method to read from ReadableByteChannel because BlockingChannel requires read with timeout
     * See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
     * This can go away after we get rid of BlockingChannel
     * 需要从ReadableByteChannel读取的方法，因为BlockingChannel需要带超时的读取
     * 请参见：http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
     * 这可以在我们摆脱BlockingChannel后消失
     *
     *
     */
    @Deprecated
    public long readFromReadableChannel(ReadableByteChannel channel) throws IOException {
        /**
         * 包含了对粘包和拆包的处理
         * 1: 先说粘包问题
         * 如果要解决粘包问题，就是每个响应中间必须插入一个特殊的几个字节的分隔符，
         * 一般来说用作分隔符比如很经典的就是在响应消息前面先插入4个字节（integer类型的）代表响应消息自己本身数据大小的数字，
         * 响应消息1，199个字节；响应消息2,238个字节；响应消息3,355个字节
         * 199 + 响应消息1 + 238+ 响应消息2 + 355+ 响应消息3 。
         * receive.readFrom(transportLayer); 的一次调用，刚好就是读取一条响应消息
         *
         * 2：拆包问题
         * 在读取消息的时候，4个字节的size都没读完，2个字节，或者是199个字节的消息就读到了162个字节，拆包问题怎么来处理的呢？
         * 拆包，假如说size是4个字节，你一次read就读取到了2个字节，连size都没有读取完毕，出现了拆包，此时怎么办呢？
         * 或者你读取到了一个size，199个字节，但是在读取响应消息的时候，就读取到了162个字节，拆包问题，响应消息没有读取完毕。
         * 下一次如果再次执行poll，发现又有数据可以读取了，此时的话呢，就会再次运行到这里去,
         * NetworkReceive receive还是停留在那里，所以呢可以继续读取 ，不会被重置为null ,
         * 剩余只能读2个字节，所以最多就只能读取2个字节到里面去，4个字节凑满了，此时就说明size数字是可以读取出来了，解决了size的拆包的问题，
         * 第二次拆包问题发生了，199个字节的消息，只读取到了162个字节，37个字节是剩余可以读取的,
         * 下一次又发现这个broker有OP_READ可以读取的时候，再次进来，继续读取数据。
         */
        // 存储本次读取了多少个字节
        int read = 0;
        if (size.hasRemaining()) {
            /*
             * 此时会从channel中读取4个字节的数字，写入到size ByteBuffer（4个字节），
             */
            int bytesRead = channel.read(size);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            read += bytesRead;
            if (!size.hasRemaining()) {
                /*
                 * 如果已经读取到了4个字节，position就会变成4，就会跟limit是一样的，
                 * 此时就代表着size ByteBuffer的4个字节已经读满了。
                 * ByteBuffer.rewind，把position设置为0，一个ByteBuffer写满之后，调用rewind，
                 * 把position重置为0，此时就可以从ByteBuffer里读取数据了。
                 */
                size.rewind();
                // ByteBuffer.getInt()，就会默认从ByteBuffer当前position的位置获取4个字节，转换为一个int类型的数字返回给你
                int receiveSize = size.getInt();
                if (receiveSize < 0) {
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                }
                if (maxSize != UNLIMITED && receiveSize > maxSize) {
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                }
                // 分配一个size值大小的buffer，读取具体的响应数据
                this.buffer = ByteBuffer.allocate(receiveSize);
            }
        }
        if (buffer != null) {
            // 接下来就会直接把channel里的一条响应消息的数据读取到一个跟他的大小一致的ByteBuffer中去，
            // 粘包问题的解决，就是完美的通过每条消息基于一个4个字节的int数字（他们自己的大小）来进行分割
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            read += bytesRead;
        }

        return read;
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

}
