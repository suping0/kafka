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
import java.nio.channels.GatheringByteChannel;

/**
 * A send backed by an array of byte buffers
 */
public class ByteBufferSend implements Send {

    private final String destination;
    private final int size;
    protected final ByteBuffer[] buffers;
    private int remaining;
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        super();
        this.destination = destination;
        this.buffers = buffers;
        for (int i = 0; i < buffers.length; i++)
            remaining += buffers[i].remaining();
        this.size = remaining;
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = channel.write(buffers);
        if (written < 0) {
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        }
        // 剩余需要发送的数据量
        remaining -= written;
        // This is temporary workaround. As Send , Receive interfaces are being used by BlockingChannel.
        // Once BlockingChannel is removed we can make Send, Receive to work with transportLayer rather than
        // GatheringByteChannel or ScatteringByteChannel.
        // 这是临时解决办法。作为发送接口，接收接口被BlockingChannel使用。
        // 一旦阻塞通道被移除，我们就可以使用transportLayer进行发送、接收，而不是通过TechChannel进行收集或通过TechChannel进行分散。
        if (channel instanceof TransportLayer) {
            pending = ((TransportLayer) channel).hasPendingWrites();
        }

        return written;
    }
}
