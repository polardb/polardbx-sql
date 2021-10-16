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

package com.alibaba.polardbx.net.reverse;

import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.buffer.BufferQueue;
import com.alibaba.polardbx.net.sample.SamplePrivileges;
import com.alibaba.polardbx.net.sample.handle.SampleQueryHandler;
import com.alibaba.polardbx.net.sample.net.SampleConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

public class Server {

    private static final int RECV_BUFFER_SIZE = 16 * 1024;
    private static final int SEND_BUFFER_SIZE = 8 * 1024;
    protected int socketRecvBuffer = 8 * 1024;
    protected int socketSendBuffer = 16 * 1024;
    protected int packetHeaderSize = 4;
    protected int maxPacketSize = 16 * 1024 * 1024;
    protected int writeQueueCapcity = 16;
    protected long idleTimeout = 8 * 3600 * 1000L;
    protected String charset = "utf8";

    public Server(String host, int port) throws IOException {
        NIOProcessor processor = new NIOProcessor(0, "Processor", 4);
        processor.startup();

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(host, port));
        Socket socket = socketChannel.socket();
        socket.setTcpNoDelay(true);
        socket.setTrafficClass(0x04 | 0x10);
        socket.setPerformancePreferences(0, 2, 1);
        socket.setReceiveBufferSize(RECV_BUFFER_SIZE);
        socket.setSendBufferSize(SEND_BUFFER_SIZE);
        socketChannel.finishConnect();

        SampleConnection c = new SampleConnection(socket.getChannel());
        c.setPrivileges(new SamplePrivileges());
        c.setQueryHandler(new SampleQueryHandler(c));

        c.setPacketHeaderSize(packetHeaderSize);
        c.setMaxPacketSize(maxPacketSize);
        c.setWriteQueue(new BufferQueue(writeQueueCapcity));
        c.setIdleTimeout(idleTimeout);
        c.setCharset(charset);
        c.setProcessor(processor);
        processor.postRegister(c);
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        Server server = new Server("127.0.0.1", 8508);
        System.out.println(server);
        Thread.sleep(60 * 1000 * 100);
    }
}
