/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.net.factory;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.buffer.BufferQueue;

/**
 * @author xianmao.hexm
 */
public abstract class FrontendConnectionFactory {

    protected int socketRecvBuffer = 32 * 1024;
    protected int socketSendBuffer = 64 * 1024;
    protected int packetHeaderSize = 4;
    protected int maxPacketSize = 16 * 1024 * 1024;
    protected int writeQueueCapcity = 16;
    protected long idleTimeout = 8 * 3600 * 1000L;
    protected String charset = "utf8";
    /**
     * 压缩时的压缩头大小，这与packetHeaderSize是不同的， 压缩内容解压缩后是完整的packetHeaderSize+payload，
     */
    protected int compressPacketHeaderSize = 7;

    protected abstract FrontendConnection getConnection(SocketChannel channel);

    public FrontendConnection make(SocketChannel channel) throws IOException {
        Socket socket = channel.socket();

        // If bufs set 0, using '/etc/sysctl.conf' system settings on default
        // refer: net.ipv4.tcp_wmem / net.ipv4.tcp_rmem
        if (socketRecvBuffer > 0) {
            socket.setReceiveBufferSize(socketRecvBuffer);
        }
        if (socketSendBuffer > 0) {
            socket.setSendBufferSize(socketSendBuffer);
        }

        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        FrontendConnection c = getConnection(channel);
        c.setPacketHeaderSize(packetHeaderSize);
        c.setMaxPacketSize(maxPacketSize);
        c.setWriteQueue(new BufferQueue(writeQueueCapcity));
        c.setIdleTimeout(idleTimeout);
        c.setCharset(charset);
        c.setCompressPacketHeaderSize(compressPacketHeaderSize);
        return c;
    }

    public int getSocketRecvBuffer() {
        return socketRecvBuffer;
    }

    public void setSocketRecvBuffer(int socketRecvBuffer) {
        this.socketRecvBuffer = socketRecvBuffer;
    }

    public int getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public void setSocketSendBuffer(int socketSendBuffer) {
        this.socketSendBuffer = socketSendBuffer;
    }

    public int getPacketHeaderSize() {
        return packetHeaderSize;
    }

    public void setPacketHeaderSize(int packetHeaderSize) {
        this.packetHeaderSize = packetHeaderSize;
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    public void setMaxPacketSize(int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public int getWriteQueueCapcity() {
        return writeQueueCapcity;
    }

    public void setWriteQueueCapcity(int writeQueueCapcity) {
        this.writeQueueCapcity = writeQueueCapcity;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

}
