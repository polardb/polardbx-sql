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

import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverExec;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.optimizer.memory.GlobalMemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolListener;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPool;
import com.google.common.collect.Ordering;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MemoryRevokingScheduler {

    private static final Logger log = LoggerFactory.getLogger(MemoryRevokingScheduler.class);

    private ScheduledFuture<?> scheduledFuture;
    private ScheduledExecutorService taskManagementExecutor;

    private final AtomicBoolean checkPending = new AtomicBoolean();
    private GlobalMemoryPool memoryPool;
    private QueryManager queryManager;

    private final MemoryPoolListener memoryPoolListener;

    @Nullable
    private TaskManager sqlTaskManager;

    public MemoryRevokingScheduler(ScheduledExecutorService taskManagementExecutor) {
        this.taskManagementExecutor = taskManagementExecutor;
        this.memoryPool = MemoryManager.getInstance().getGlobalMemoryPool();
        this.queryManager = ServiceProvider.getInstance().getServer().getQueryManager();
        if (ServiceProvider.getInstance().getServer() instanceof MppServer) {
            this.sqlTaskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
        }
        this.memoryPoolListener = (targetMemoryPool, target) -> onMemoryReserved(targetMemoryPool, target);
    }

    public void start() {
        registerPeriodicCheck();
        memoryPool.setMaxElasticMemory(MppConfig.getInstance().getMemoryRevokingThreshold());
        memoryPool.addListener(memoryPoolListener);
    }

    private void registerPeriodicCheck() {
        this.scheduledFuture = taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (checkPending.compareAndSet(false, true)) {
                    runMemoryRevoking(true, MppConfig.getInstance().getMemoryRevokingTarget());
                }
            } catch (Throwable e) {
                log.error("Error requesting system memory revoking", e);
            }
        }, 1, 1, SECONDS);
    }

    private void onMemoryReserved(MemoryPool memoryPool, double target) {
        if (checkPending.compareAndSet(false, true)) {
            log.info("release the memory actively for " + memoryPool.getFullName());
            taskManagementExecutor.execute(() -> {
                try {
                    runMemoryRevoking(false, target);
                } catch (Throwable e) {
                    log.error("Error requesting memory revoking", e);
                }
            });
        }
    }

    private synchronized void runMemoryRevoking(boolean forceNotify, double target) {
        if (checkPending.getAndSet(false)) {
            if (memoryPool.getRevocableBytes() > 0) {
                List<TaskContext> revocableTaskContexts = new ArrayList<>();
                if (sqlTaskManager != null) {
                    List<SqlTask> sqlTasks = sqlTaskManager.getAllTasks();
                    for (SqlTask task : sqlTasks) {
                        MemoryPool memoryPool = task.getTaskMemoryPool();
                        if (memoryPool != null && memoryPool.getRevocableBytes() > 0) {
                            if (task.getTaskExecution() != null) {
                                revocableTaskContexts.add(task.getTaskExecution().getTaskContext());
                            }
                        }
                    }
                }
                if (queryManager != null) {
                    revocableTaskContexts.addAll(queryManager.getAllLocalQueryContext());
                }

                requestForceRevokingForQuery(revocableTaskContexts);

                if (memoryRevokingNeeded(memoryPool)) {

                    log.info("GlobalMemory is used much more memory: used " + memoryPool.getMemoryUsage() + " total " +
                        memoryPool.getMaxLimit());
                    //revoke the memory
                    long realRevokeBytes = Math.max(
                        (long) (memoryPool.getMaxLimit() * (1.0 - target) - memoryPool.getFreeBytes()),
                        memoryPool.getMinRequestMemory());

                    if (realRevokeBytes <= 0) {
                        //设置查询级别内存释放的最小单位
                        realRevokeBytes = MppConfig.getInstance().getLessRevokeBytes();
                    }

                    AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(realRevokeBytes);
                    List<TaskContext> sortTaskContexts = revocableTaskContexts.stream().sorted(
                        TASK_ORDER_BY_REVOCABLE_MEMORY_SIZE).collect(Collectors.toList());

                    //release the operator whose memory exceed the REVOKE_MEMORY_STEP firstly.
                    // 设置被动触发spill内存阈值
                    long[] guaranteedMemoryBytes = new long[] {
                        MppConfig.getInstance().getLessRevokeBytes() * 2, MppConfig.getInstance().getLessRevokeBytes()};
                    for (int i = 0; i < guaranteedMemoryBytes.length; i++) {
                        for (TaskContext taskContext : sortTaskContexts) {
                            requestRevokingForTask(taskContext, guaranteedMemoryBytes[i], remainingBytesToRevokeAtomic);
                        }
                        if (remainingBytesToRevokeAtomic.get() <= 0) {
                            return;
                        }
                    }

                    if (forceNotify) {
                        for (TaskContext taskContext : sortTaskContexts) {
//                            QueryMemoryPool memoryPool = taskContext.getQueryMemoryPool();
//                            if (memoryPool.getRevocableBytes() >= MppConfig.getInstance().getLessRevokeBytes()) {
//                                //Don't spill the query whose used memory is less than LESS_REVOKE_BYTES
//                                requestRevokingForOrderByDesc(taskContext, remainingBytesToRevokeAtomic);
//                                if (remainingBytesToRevokeAtomic.get() <= 0) {
//                                    return;
//                                }
//                            }
                            //考虑到尝试性申请内存的时候，可能会卡主，这里屏蔽上述代码
                            requestRevokingForOrderByDesc(taskContext, remainingBytesToRevokeAtomic);
                            if (remainingBytesToRevokeAtomic.get() <= 0) {
                                return;
                            }
                        }
                    }
                    memoryPool.resetNeedMemoryRevoking();
                }
            }
        }
    }

    private boolean memoryRevokingNeeded(GlobalMemoryPool memoryPool) {
        boolean hasRevokeMemory = memoryPool.getRevocableBytes() > 0;
        boolean queryNeedMemory =
            (memoryPool.getBlockedFuture() != null && !memoryPool.getBlockedFuture().isDone()) || (
                memoryPool.getTrySettableFuture() != null && !memoryPool.getTrySettableFuture().isDone());

        return (memoryPool.getFreeBytes() <= memoryPool.getMaxLimit() * (1.0 - MppConfig.getInstance()
            .getMemoryRevokingThreshold()) || memoryPool.isNeedMemoryRevoking() || queryNeedMemory) && hasRevokeMemory;
    }

    private static final Ordering<TaskContext> TASK_ORDER_BY_REVOCABLE_MEMORY_SIZE =
        Ordering.natural().onResultOf(taskContext -> taskContext.getContext().getMemoryPool().getRevocableBytes());

    private static final Ordering<OperatorMemoryAllocatorCtx> OPERATOR_ORDER_BY_REVOCABLE_MEMORY_SIZE =
        Ordering.natural().onResultOf(memoryContext -> memoryContext.getRevocableAllocated());

    private void requestRevokingForTask(
        TaskContext taskContext, long guaranteedMemoryBytes, AtomicLong remainingBytesToRevokeAtomic) {
        for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {

            if (pipelineContext.getDriverContexts().size() > 0) {
                DriverExec exec = pipelineContext.getDriverContexts().get(0).getDriverExec();
                if (exec != null) {
                    int levelMax = exec.getMemoryRevokers().size();
                    if (levelMax > 0) {
                        for (int opLevel = 0; opLevel < levelMax; opLevel++) {
                            for (DriverContext driverContext : pipelineContext.getDriverContexts()) {
                                DriverExec driverExec = driverContext.getDriverExec();
                                if (driverExec != null && driverExec.isOpened() && !driverExec.isFinished() &&
                                    !driverExec.getMemoryRevokers().isEmpty()) {
                                    if (remainingBytesToRevokeAtomic.get() <= 0) {
                                        return;
                                    }
                                    List<MemoryRevoker> revokers = driverExec.getMemoryRevokers();
                                    OperatorMemoryAllocatorCtx memoryAllocatorCtx =
                                        revokers.get(opLevel).getMemoryAllocatorCtx();
                                    // not force revoke memory && revocable bytes less than guaranteedMemoryBytes
                                    if (memoryAllocatorCtx.getRevocableAllocated() > guaranteedMemoryBytes) {
                                        long revokeSize =
                                            memoryAllocatorCtx.requestMemoryRevokingOrReturnRevokingBytes();
                                        if (revokeSize > 0) {
                                            remainingBytesToRevokeAtomic.addAndGet(-revokeSize);
                                            if (log.isDebugEnabled()) {
                                                log.debug("memoryPool=" + memoryAllocatorCtx.getName()
                                                    + ": requested revoking "
                                                    + revokeSize + "; remaining " + remainingBytesToRevokeAtomic.get());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void requestRevokingForOrderByDesc(TaskContext taskContext, AtomicLong remainingBytesToRevokeAtomic) {
        List<OperatorMemoryAllocatorCtx> memoryContexts = new ArrayList<>();
        for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {
            for (DriverContext driverContext : pipelineContext.getDriverContexts()) {
                DriverExec driverExec = driverContext.getDriverExec();
                if (driverExec != null && !driverExec.isFinished() && driverExec.isOpened()) {
                    for (MemoryRevoker memoryRevoker : driverExec.getMemoryRevokers()) {
                        OperatorMemoryAllocatorCtx memoryAllocatorCtx = memoryRevoker.getMemoryAllocatorCtx();
                        if (memoryAllocatorCtx != null &&
                            !memoryAllocatorCtx.isMemoryRevokingRequested() &&
                            memoryAllocatorCtx.getRevocableAllocated() > 0) {
                            memoryContexts.add(memoryRevoker.getMemoryAllocatorCtx());
                        }
                    }
                }
            }
        }

        memoryContexts.stream().sorted(OPERATOR_ORDER_BY_REVOCABLE_MEMORY_SIZE)
            .forEach(memoryContext -> {
                    if (remainingBytesToRevokeAtomic.get() < 0) {
                        return;
                    }
                    long revokedBytes = memoryContext.requestMemoryRevokingOrReturnRevokingBytes();
                    if (revokedBytes > 0) {
                        remainingBytesToRevokeAtomic.addAndGet(-revokedBytes);
                        if (log.isDebugEnabled()) {
                            log.debug("memoryPool=" + memoryContext.getName() + ": requested revoking "
                                + revokedBytes + "; remaining " + remainingBytesToRevokeAtomic.get());
                        }
                    }
                }
            );
    }

    private void requestForceRevokingForQuery(List<TaskContext> revocableTaskContexts) {
        Map<String, List<TaskContext>> blockedTaskCts = new HashMap<>();
        Map<String, Long> queryToMinRequests = new HashMap<>();
        for (TaskContext taskContext : revocableTaskContexts) {
            String queryId = taskContext.getTaskId().getQueryId();
            QueryMemoryPool memoryPool = taskContext.getQueryMemoryPool();
            if (!memoryPool.isDestoryed()) {
                boolean needQueryRequest = false;
                if (memoryPool.getBlockedFuture() != null && !memoryPool.getBlockedFuture().isDone()) {
                    needQueryRequest = true;
                } else if (memoryPool.getTrySettableFuture() != null && !memoryPool.getTrySettableFuture().isDone()) {
                    needQueryRequest = true;
                }
                if (needQueryRequest) {
                    if (!blockedTaskCts.containsKey(queryId)) {
                        blockedTaskCts.put(queryId, new ArrayList<>());
                        //revoke the memory
                        long realRevokeBytes = Math.max((long) (
                                memoryPool.getMaxLimit() * (1.0 - MppConfig.getInstance().getMemoryRevokingTarget())
                                    - memoryPool.getFreeBytes()),
                            memoryPool.getMinRequestMemory());

                        queryToMinRequests.put(queryId, realRevokeBytes);
                    }
                    blockedTaskCts.get(queryId).add(taskContext);
                }
            }
        }

        for (Map.Entry<String, List<TaskContext>> entry : blockedTaskCts.entrySet()) {
            String queryId = entry.getKey();
            long realRevokeBytes = queryToMinRequests.get(queryId);

            // real want revoke memory for query
            if (realRevokeBytes <= MppConfig.getInstance().getLessRevokeBytes() / 8) {
                realRevokeBytes = MppConfig.getInstance().getLessRevokeBytes() / 8;
            }

            AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(realRevokeBytes);
            List<TaskContext> sortTaskContexts = entry.getValue().stream().sorted(
                TASK_ORDER_BY_REVOCABLE_MEMORY_SIZE).collect(Collectors.toList());

            for (TaskContext taskContext : sortTaskContexts) {
                requestRevokingForTask(
                    taskContext, MppConfig.getInstance().getBlockSize(), remainingBytesToRevokeAtomic);
            }
        }
    }

    public void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
    }
}
