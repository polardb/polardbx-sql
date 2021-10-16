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

package com.alibaba.polardbx.net.sample;

import com.alibaba.polardbx.net.NIOAcceptor;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.sample.net.SampleConnectionFactory;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 服务器组装示例
 *
 * @author xianmao.hexm
 */
public class SampleServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleServer.class);
    private static final int SERVER_PORT = 8507;
    private static final long TIME_UPDATE_PERIOD = 100L;
    private static final SampleServer INSTANCE = new SampleServer();

    public static final SampleServer getInstance() {
        return INSTANCE;
    }

    private SampleConfig config;
    private Timer timer;
    private NIOProcessor[] processors;
    private NIOAcceptor server;

    private SampleServer() {
        this.config = new SampleConfig();
    }

    public SampleConfig getConfig() {
        return config;
    }

    public void startup() throws IOException {
        String name = config.getServerName();
        LOGGER.info("===============================================");
        LOGGER.info(name + " is ready to startup ...");

        // schedule timer task
        timer = new Timer(name + "Timer", true);
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                TimeUtil.update();
            }
        }, 0L, TIME_UPDATE_PERIOD);
        LOGGER.info("Task Timer is started ...");

        // startup processors
        processors = new NIOProcessor[ThreadCpuStatUtil.NUM_CORES];
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new NIOProcessor(i, name + "Processor" + i, 4);
            processors[i].startup();
        }

        // startup server
        SampleConnectionFactory factory = new SampleConnectionFactory();
        server = new NIOAcceptor(name + "Server", SERVER_PORT, factory, true);
        server.setProcessors(processors);
        server.start();
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());

        // end
        LOGGER.info("===============================================");
    }

}
