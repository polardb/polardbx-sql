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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPool;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

public final class QueryContext {

    private static final Logger log = LoggerFactory.getLogger(QueryContext.class);

    private final String queryId;
    private final ScheduledExecutorService yieldExecutor;
    private QueryMemoryPool queryMemoryPool;
    private QuerySpillSpaceMonitor querySpillSpaceMonitor;
    private final Map<String, MemoryPool> taskMemoryPools = new ConcurrentHashMap<>();
    protected boolean reNewQueryPool;

    public QueryContext(
        ScheduledExecutorService yieldExecutor,
        String queryId) {
        this.yieldExecutor = yieldExecutor;
        this.queryId = queryId;
    }

    public QueryMemoryPool getQueryMemoryPool() {
        return queryMemoryPool;
    }

    public QuerySpillSpaceMonitor getQuerySpillSpaceMonitor() {
        return querySpillSpaceMonitor;
    }

    public String getQueryId() {
        return queryId;
    }

    public TaskContext createTaskContext(
        TaskStateMachine taskStateMachine, Session session) {
        TaskContext taskContext = new TaskContext(yieldExecutor, taskStateMachine, session.getClientContext(),
            taskStateMachine.getTaskId());
        taskStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                taskContext.end();
            }
        });
        return taskContext;
    }

    public QueryMemoryPool createQueryMemoryPool(String schema, ExecutionContext executionContext) {
        if (this.queryMemoryPool == null) {
            Preconditions.checkArgument(executionContext.getWorkloadType() != null, "Workload is null!");
            //String workerQueryPoolName = queryId + WORKER_QUERY_POOL_SUFFIX;
            //如果server和worker处于一个进程，query进来的时候会在server创建queryPool，所以为了区分worker端创建的queryPool
            //名称定为workerQueryPoolName
            boolean ap = WorkloadUtil.isApWorkload(executionContext.getWorkloadType());
            if (ap) {
                this.reNewQueryPool = !MemoryManager.getInstance().getApMemoryPool().getChildren().containsKey(
                    executionContext.getTraceId());
            } else {
                this.reNewQueryPool = !MemoryManager.getInstance().getTpMemoryPool().getChildren().containsKey(
                    executionContext.getTraceId());
            }

            this.queryMemoryPool = (QueryMemoryPool) MemoryManager.getInstance().createQueryMemoryPool(
                ap,
                executionContext.getTraceId(), executionContext.getExtraCmds());
        }
        return this.queryMemoryPool;
    }

    public QuerySpillSpaceMonitor createQuerySpillSpaceMonitor() {
        if (this.querySpillSpaceMonitor == null) {
            this.querySpillSpaceMonitor = new QuerySpillSpaceMonitor(queryId);
        }
        return this.querySpillSpaceMonitor;
    }

    public void registerTaskMemoryPool(MemoryPool taskMemoryPool) {
        taskMemoryPools.put(taskMemoryPool.getName(), taskMemoryPool);
    }

    public synchronized void destroyQueryMemoryPool() {
        taskMemoryPools.clear();
        if (queryMemoryPool != null && reNewQueryPool) {
            try {
                queryMemoryPool.destroy();
            } catch (Exception e) {
                log.error("destroyMemoryAndStat queryMemoryPool: " + queryId, e);
            }
        }

        try {
            if (querySpillSpaceMonitor != null) {
                querySpillSpaceMonitor.close();
            }
        } catch (Exception e) {
            log.error("close querySpillSpaceMonitor: " + queryId, e);
        }
    }
}
