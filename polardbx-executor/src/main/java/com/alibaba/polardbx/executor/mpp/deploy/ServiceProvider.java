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

package com.alibaba.polardbx.executor.mpp.deploy;

import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;

import java.util.concurrent.ScheduledExecutorService;

public class ServiceProvider {

    private static ServiceProvider instance;
    private volatile Server server;
    private ServerThreadPool serverExecutor;
    private ScheduledExecutorService timerTaskExecutor;

    public static ServiceProvider getInstance() {
        if (instance == null) {
            synchronized (Server.class) {
                if (instance == null) {
                    instance = new ServiceProvider();
                }
            }
        }
        return instance;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public ServerThreadPool getServerExecutor() {
        return serverExecutor;
    }

    public void setServerExecutor(ServerThreadPool serverExecutor) {
        this.serverExecutor = serverExecutor;
    }

    public ScheduledExecutorService getTimerTaskExecutor() {
        return timerTaskExecutor;
    }

    public void setTimerTaskExecutor(ScheduledExecutorService timerTaskExecutor) {
        this.timerTaskExecutor = timerTaskExecutor;
    }

    public boolean clusterMode() {
        if (server != null && server instanceof MppServer) {
            return true;
        } else {
            return false;
        }
    }
}
