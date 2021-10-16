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

import com.alibaba.polardbx.net.packet.BinaryPacket;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class Manager extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(Manager.class);
    private Selector selector;
    private ServerSocketChannel serverChannel;

    public Manager() throws IOException {
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.socket().bind(new InetSocketAddress(8508));
        this.serverChannel.configureBlocking(false);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        final Selector selector = this.selector;
        for (; ; ) {
            try {
                selector.select(1000L);
                Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isAcceptable()) {
                            accept();
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Throwable e) {
                logger.warn(getName(), e);
            }
        }
    }

    private void accept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            ManagerChannel conn = new ManagerChannel(channel.socket());
            BinaryPacket packet = conn.execute("select 1 from dual");
            System.out.println(packet);
        } catch (Throwable e) {
            closeChannel(channel);
            logger.warn(getName(), e);
        }
    }

    private static void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
        }
    }

    public static void main(String args[]) throws IOException {
        Manager manager = new Manager();
        manager.start();
    }
}
