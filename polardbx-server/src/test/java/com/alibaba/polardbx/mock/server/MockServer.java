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

package com.alibaba.polardbx.mock.server;

import com.alibaba.polardbx.net.NIOAcceptor;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Mock Server
 *
 * @author changyuan.lh 2019年2月27日 上午12:15:52
 * @since 5.0.0
 */
public class MockServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockServer.class);
    private static final int SERVER_PORT = 8507;
    private static final MockServer INSTANCE = new MockServer();

    public static final MockServer getInstance() {
        return INSTANCE;
    }

    private ScheduledThreadPoolExecutor scheduler;
    private NIOProcessor[] processors;
    private NIOAcceptor server;

    private MockServer() {
        this.scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "Mock-Timer");
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    public void startup() throws IOException {
        LOGGER.info("===============================================");
        LOGGER.info("Mock-Server is ready to startup ...");

        // schedule timer task
        scheduler.scheduleWithFixedDelay(new TimerTask() {

            @Override
            public void run() {
                TimeUtil.update();
            }
        }, 0L, 20, TimeUnit.MILLISECONDS);

        // startup processors
        processors = new NIOProcessor[ThreadCpuStatUtil.NUM_CORES];
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new NIOProcessor(i, "Mock-Processor" + i, 16);
            processors[i].startup();
        }

        // startup server
        MockConnectionFactory factory = new MockConnectionFactory();
        server = new NIOAcceptor("Mock-Server", SERVER_PORT, factory, true);
        server.setProcessors(processors);
        server.start();
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());

        // end
        LOGGER.info("===============================================");
    }

    public static void main(String[] args) throws IOException {
        MockServer.getInstance().startup();
    }
}
