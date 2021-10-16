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

package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.collect.Ordering;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.GlobalMemoryPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 **/
public class MemoryKiller {

    private static final Logger log = LoggerFactory.getLogger(MemoryKiller.class);

    private static final Ordering<TaskContext> TASK_ORDER_BY_REVOCABLE_MEMORY_SIZE =
        Ordering.natural().onResultOf(taskContext -> taskContext.getContext().getMemoryPool().getRevocableBytes());

    private GlobalMemoryPool memoryPool;
    private TaskManager sqlTaskManager;
    private QueryManager queryManager;

    public MemoryKiller() {
        this.memoryPool = MemoryManager.getInstance().getGlobalMemoryPool();
        this.queryManager = ServiceProvider.getInstance().getServer().getQueryManager();

        if (ServiceProvider.getInstance().getServer() instanceof MppServer) {
            this.sqlTaskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
        }
    }

    public synchronized void killMemory(long remainingBytesToRevoke) {
        if (memoryPool.getRevocableBytes() > 0) {
            List<TaskContext> revocableTaskContexts = new ArrayList<>();
            if (sqlTaskManager != null) {
                List<SqlTask> sqlTasks = sqlTaskManager.getAllTasks();
                for (SqlTask task : sqlTasks) {
                    MemoryPool memoryPool = task.getTaskMemoryPool();
                    if (memoryPool != null && memoryPool.getRevocableBytes() > 0) {
                        revocableTaskContexts.add(task.getTaskExecution().getTaskContext());
                    }
                }
            }
            if (queryManager != null) {
                revocableTaskContexts.addAll(queryManager.getAllLocalQueryContext());
            }
            requestKilling(revocableTaskContexts, remainingBytesToRevoke);
        }
    }

    private void requestKilling(List<TaskContext> revocableTaskContexts, long remainingBytesToRevoke) {
        AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
        revocableTaskContexts.stream().sorted(TASK_ORDER_BY_REVOCABLE_MEMORY_SIZE)
            .forEach(taskContext -> {
                if (remainingBytesToRevokeAtomic.get() > 0) {
                    ExecutionContext executionContext = taskContext.getContext();
                    log.warn("try kill:" + executionContext.getTraceId() + "," + executionContext.getOriginSql());
                    try {
                        ISyncAction action =
                            (ISyncAction) Class.forName("com.alibaba.polardbx.server.response.KillSyncAction")
                                .getConstructor(String.class, long.class, boolean.class)
                                .newInstance(executionContext.getSchemaName(), executionContext.getConnId(), true);
                        SyncManagerHelper.sync(action, executionContext.getSchemaName());
                        remainingBytesToRevokeAtomic.addAndGet(-executionContext.getMemoryPool().getMemoryUsage());
                    } catch (Exception e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
                    }
                }
            });
    }

}
