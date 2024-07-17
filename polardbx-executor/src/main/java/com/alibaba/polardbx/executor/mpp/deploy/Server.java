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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.discover.PolarDBXNodeStatusManager;
import com.alibaba.polardbx.executor.mpp.execution.MemoryRevokingScheduler;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newScheduledThreadPool;

public abstract class Server {

    protected static final Logger log = LoggerFactory.getLogger(Server.class);

    public static final String NODEID_PREFIX = "node_";
    public static final String MPP_LOADAPP = "loadapp";
    public static final String LOCAL_CLUSTER_DEFAULT = "LOCAL";

    protected String nodeId;
    protected int mppPort;

    protected QueryManager queryManager;
    protected TaskExecutor taskExecutor;
    protected InternalNode localNode;
    protected InternalNodeManager nodeManager;
    protected SpillerFactory spillerFactory;

    protected MemoryRevokingScheduler memoryRevokingScheduler;

    protected PolarDBXNodeStatusManager manager;

    public Server(int id, int mppHttpPort) {
        this.nodeId = NODEID_PREFIX + id;
        this.mppPort = mppHttpPort;
    }

    public void reloadConfig() {
        if (queryManager != null) {
            queryManager.reloadConfig();
        }
    }

    public InternalNode getLocalNode() {
        return localNode;
    }

    public QueryManager getQueryManager() {
        return queryManager;
    }

    public TaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    public MemoryRevokingScheduler getMemoryRevokingScheduler() {
        return memoryRevokingScheduler;
    }

    public void start() {
        if (MemorySetting.ENABLE_SPILL && memoryRevokingScheduler == null) {
            ScheduledExecutorService memoryManagementExecutor =
                newScheduledThreadPool(1, Threads.threadsNamed("memory-revoke-management"));
            memoryRevokingScheduler = new MemoryRevokingScheduler(memoryManagementExecutor);
            memoryRevokingScheduler.start();
        }
    }

    public void stop() {
        if (memoryRevokingScheduler != null) {
            memoryRevokingScheduler.stop();
        }

        if (manager != null) {
            manager.destroy(true);
        }

    }

    public abstract void run() throws TddlRuntimeException;

    public abstract boolean isCoordinator();

    public boolean isEnableConfigLeader() {
        return false;
    }

    public PolarDBXNodeStatusManager getStatusManager() {
        return manager;
    }

    public InternalNodeManager getNodeManager() {
        return nodeManager;
    }

    public String getNodeId() {
        return nodeId;
    }

    public SpillerFactory getSpillerFactory() {
        return spillerFactory;
    }
}
