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

package com.alibaba.polardbx.executor.changeset;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wumu
 */
public class ChangeSetApplyExecutor {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private Future future;
    private final ConcurrentLinkedQueue<ChangeSetApplyInfo> taskExecutionQueue = new ConcurrentLinkedQueue<>();

    private final int maxParallelism;
    private final String schemaName;
    private final Long jobId;

    private final AtomicReference<Boolean> started;
    private final AtomicReference<Boolean> interrupt;
    private final AtomicReference<Exception> exception;

    public ChangeSetApplyExecutor(String schema, Long jobId, int maxParallelism) {
        this.schemaName = schema;
        this.jobId = jobId;
        this.maxParallelism = maxParallelism;
        this.started = new AtomicReference<>(false);
        this.interrupt = new AtomicReference<>(false);
        this.exception = new AtomicReference<>(null);
    }

    public static ChangeSetApplyExecutor create(String schemaName, Long jobId, int maxParallelism) {
        return new ChangeSetApplyExecutor(schemaName, jobId, maxParallelism);
    }

    private void run() {
        FutureTask<Void> task = new FutureTask<>(
            () -> {
                Long count = 0L;
                while (true) {
                    if (interrupt.get()) {
                        LOGGER.info(String.format("ChangeSetApplyExecutor exit success, schema: %s jobId: %s",
                            schemaName, jobId));
                        return;
                    }

                    if (exception.get() != null) {
                        LOGGER.info(String.format("ChangeSetApplyExecutor exit by error %s, schema: %s jobId: %s",
                            exception.get(), schemaName, jobId));
                        return;
                    }

                    if (taskExecutionQueue.isEmpty()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                            "ChangeSetApplyExecutor run error, taskExecutionQueue is empty, schema: %s jobId: %s",
                            schemaName, jobId));
                    }

                    List<Future> futures = new ArrayList<>(maxParallelism);
                    List<ChangeSetApplyInfo> changeSetApplyInfoList = new ArrayList<>(maxParallelism);

                    while (futures.size() < maxParallelism) {
                        ChangeSetApplyInfo changeSetApplyInfo = taskExecutionQueue.poll();
                        if (changeSetApplyInfo == null) {
                            break;
                        }

                        FutureTask<Void> applyTask = new FutureTask<>(
                            () -> {
                                if (changeSetApplyInfo.ec.getDdlContext().isInterrupted()) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                        "The job '" + jobId + "' has been cancelled");
                                }

                                ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
                                changeSetManager.logicalTableChangeSetCatchUp(
                                    changeSetApplyInfo.logicalTableName,
                                    changeSetApplyInfo.indexTableName,
                                    changeSetApplyInfo.sourcePhyTableNames,
                                    changeSetApplyInfo.orderedTargetTableLocations,
                                    changeSetApplyInfo.taskType,
                                    changeSetApplyInfo.catchUpStatus,
                                    changeSetApplyInfo.changeSetId,
                                    changeSetApplyInfo.ec
                                );
                            }, null);
                        futures.add(applyTask);
                        changeSetApplyInfoList.add(changeSetApplyInfo);

                        ChangeSetApplyExecutorMap.getInstance4CatchUp()
                            .executeWithContext(applyTask, PriorityFIFOTask.TaskPriority.CHANGESET_APPLY_TASK);
                    }

                    for (Future future : futures) {
                        try {
                            future.get();
                        } catch (Exception e) {
                            if (null == exception.get()) {
                                exception.set(e);
                            }
                        }
                    }

                    try {
                        TimeUnit.MILLISECONDS.sleep(1000L);
                    } catch (InterruptedException ex) {
                        throw new TddlNestableRuntimeException(ex);
                    }

                    LOGGER.info(
                        String.format("ChangeSetApplyExecutor execute times %s, schema: %s jobId: %s, tasks: %s",
                            ++count, schemaName, jobId, changeSetApplyInfoList.size()));
                    // add back
                    taskExecutionQueue.addAll(changeSetApplyInfoList);
                }
            }, null);

        future = task;
        ChangeSetApplyExecutorMap.getInstance()
            .executeWithContext(task, PriorityFIFOTask.TaskPriority.CHANGESET_APPLY_TASK);
    }

    public boolean addTask(ChangeSetApplyInfo changeSetApplyInfo) {
        if (exception.get() != null) {
            return false;
        }

        synchronized (started) {
            taskExecutionQueue.add(changeSetApplyInfo);

            if (!started.get()) {
                run();
                started.set(true);
            }
        }

        LOGGER.info(String.format("ChangeSetApplyExecutor addTask, schema: %s jobId: %s, changeSetId: %s",
            schemaName, jobId, changeSetApplyInfo.changeSetId));
        return true;
    }

    public boolean waitAndStop() {
        interrupt.set(true);

        try {
            future.get();
        } catch (Exception e) {
            if (null == exception.get()) {
                exception.set(e);
            }
        }

        return exception.get() == null;
    }

    public AtomicReference<Exception> getException() {
        return exception;
    }

    public static class ChangeSetApplyInfo {
        final public String logicalTableName;
        final public String indexTableName;
        final public Map<String, Set<String>> sourcePhyTableNames;
        final public Map<String, String> orderedTargetTableLocations;
        final public ChangeSetManager.ChangeSetCatchUpStatus catchUpStatus;
        final public ComplexTaskMetaManager.ComplexTaskType taskType;
        final public Long changeSetId;
        final public ExecutionContext ec;

        public ChangeSetApplyInfo(String logicalTableName, String indexTableName,
                                  Map<String, Set<String>> sourcePhyTableNames,
                                  Map<String, String> orderedTargetTableLocations,
                                  ChangeSetManager.ChangeSetCatchUpStatus catchUpStatus,
                                  ComplexTaskMetaManager.ComplexTaskType taskType, Long changeSetId,
                                  ExecutionContext ec) {
            this.logicalTableName = logicalTableName;
            this.indexTableName = indexTableName;
            this.sourcePhyTableNames = sourcePhyTableNames;
            this.orderedTargetTableLocations = orderedTargetTableLocations;
            this.catchUpStatus = catchUpStatus;
            this.taskType = taskType;
            this.changeSetId = changeSetId;
            this.ec = ec;
        }
    }
}
