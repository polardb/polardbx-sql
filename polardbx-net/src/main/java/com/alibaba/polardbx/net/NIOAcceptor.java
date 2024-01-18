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

package com.alibaba.polardbx.net;

import com.alibaba.polardbx.net.factory.FrontendConnectionFactory;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

/**
 * @author xianmao.hexm
 */
public final class NIOAcceptor extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(NIOAcceptor.class);
    private final int port;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private final FrontendConnectionFactory factory;
    private NIOProcessor[] processors;
    private int nextProcessor;
    private long acceptCount;

    public NIOAcceptor(String name, int port, FrontendConnectionFactory factory, boolean online) throws IOException {
        super.setName(name);
        this.port = port;
        this.factory = factory;
        if (online) {
            this.selector = Selector.open();
            this.serverChannel = ServerSocketChannel.open();
            this.serverChannel.socket().bind(new InetSocketAddress(port), 65535);
            this.serverChannel.configureBlocking(false);
            this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        }
    }

    public int getPort() {
        return port;
    }

    public long getAcceptCount() {
        return acceptCount;
    }

    public void setProcessors(NIOProcessor[] processors) {
        this.processors = processors;
    }

    @Override
    public void run() {
        for (; ; ) {
            ++acceptCount;
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
                if (this.serverChannel != null && this.serverChannel.isOpen()) {
                    logger.warn(getName(), e);
                } else {
                    long sleep = 1000;
                    if (this.serverChannel == null) {
                        sleep = 100;
                    }

                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e1) {
                        // ignore
                    }
                }
            }
        }
    }

    private void accept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.configureBlocking(false);
            FrontendConnection c = factory.make(channel);
            c.setAccepted(true);

            NIOProcessor processor = nextProcessor();
            c.setProcessor(processor);
            processor.postRegister(c);
        } catch (Throwable e) {
            closeChannel(channel);
            logger.info(getName(), e);
        }
    }

    private NIOProcessor nextProcessor() {
        if (++nextProcessor == processors.length) {
            nextProcessor = 0;
        }
        return processors[nextProcessor];
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

    synchronized public void offline() {
        if (this.serverChannel == null || !this.serverChannel.isOpen()) {
            return;
        }
        try {
            this.serverChannel.close();
            this.selector.close();

            logger.info(this.getName() + " offline success " + this.getPort());
        } catch (IOException e) {
            logger.error("offline error", e);
        }
    }

    synchronized public void online() {
        if (this.serverChannel != null && this.serverChannel.isOpen()) {
            return;
        }

        try {
            this.selector = Selector.open();
            this.serverChannel = ServerSocketChannel.open();
            this.serverChannel.socket().bind(new InetSocketAddress(port));
            this.serverChannel.configureBlocking(false);
            this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info(this.getName() + " is started and listening on " + this.getPort());
        } catch (IOException e) {
            logger.error(this.getName() + " online error", e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public FrontendConnectionFactory getFactory() {
        return factory;
    }

}
