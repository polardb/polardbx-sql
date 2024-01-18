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

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.ChangeSetThreadPool;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.util.concurrent.RateLimiter;
import lombok.Data;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.MapUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genBatchRowList;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;

public class ChangeSetManager {
    private final static Logger LOG = SQLRecorderLogger.scaleOutTaskLogger;

    private static final IdGenerator ID_GENERATOR = IdGenerator.getIdGenerator();

    private final String schemaName;

    private final ChangeSetMetaManager changeSetMetaManager;

    // speed ctl
    private volatile RateLimiter rateLimiter;
    private com.alibaba.polardbx.executor.backfill.Throttle throttle;

    public ChangeSetManager(String schemaName) {
        this.schemaName = schemaName;
        this.changeSetMetaManager = new ChangeSetMetaManager(schemaName);
    }

    /**
     * change set opt for logical table
     */
    public void logicalTableChangeSetStart(String logicalTableName,
                                           Map<String, Set<String>> sourcePhyTableNames,
                                           ComplexTaskMetaManager.ComplexTaskType taskType,
                                           Long changeSetId, ExecutionContext originEc) {
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return;
        }

        changeSetMetaManager.getChangeSetReporter().initChangeSetMeta(
            changeSetId, originEc.getDdlJobId(), -1,
            schemaName, logicalTableName, null, null,
            sourcePhyTableNames);

        sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
            for (String phyTableName : phyTableNames) {
                migrateTableStart(
                    schemaName,
                    logicalTableName,
                    sourceGroupName,
                    phyTableName,
                    taskType,
                    originEc
                );
            }
        });

        // After all physical table finished
        changeSetMetaManager.getChangeSetReporter().loadChangeSetMeta(changeSetId);
        changeSetMetaManager.getChangeSetReporter().updateChangeSetStatus(ChangeSetMetaManager.ChangeSetStatus.START);
    }

    public void logicalTableChangeSetCatchUp(String logicalTableName, String indexName,
                                             Map<String, Set<String>> sourcePhyTableNames,
                                             Map<String, String> targetTableLocations,
                                             ComplexTaskMetaManager.ComplexTaskType taskType,
                                             ChangeSetCatchUpStatus status, Long changeSetId,
                                             ExecutionContext originEc) {
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return;
        }

        changeSetMetaManager.getChangeSetReporter().loadChangeSetMeta(changeSetId);

        if (status == ChangeSetCatchUpStatus.ABSENT) {
            changeSetMetaManager.getChangeSetReporter()
                .updateChangeSetStatus(ChangeSetMetaManager.ChangeSetStatus.RUNNING);
        }

        long speedLimit = originEc.getParamManager().getLong(ConnectionParams.CHANGE_SET_APPLY_SPEED_LIMITATION);
        long speedMin = originEc.getParamManager().getLong(ConnectionParams.CHANGE_SET_APPLY_SPEED_MIN);

        this.rateLimiter = speedLimit <= 0 ? null : RateLimiter.create(speedLimit);
        this.throttle = new com.alibaba.polardbx.executor.backfill.Throttle(speedMin, speedLimit, schemaName);

        List<Future> futures = new ArrayList<>(16);

        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] ChangeSet Fetch job: {1} start with {2} phyTable(s).",
                originEc.getTraceId(), changeSetId, sourcePhyTableNames.size()));

        AtomicReference<Exception> excep = new AtomicReference<>(null);
        sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
            for (String phyTableName : phyTableNames) {
                FutureTask<Void> task = new FutureTask<>(
                    () -> migrateTableCatchup(
                        schemaName, logicalTableName,
                        sourceGroupName, phyTableName,
                        targetTableLocations,
                        taskType, status, originEc
                    ), null);
                futures.add(task);
                ChangeSetThreadPool.getInstance()
                    .executeWithContext(task, PriorityFIFOTask.TaskPriority.CHANGESET_APPLY_TASK);
            }
        });

        if (excep.get() != null) {
            // Interrupt all.
            futures.forEach(f -> {
                try {
                    f.cancel(true);
                } catch (Throwable ignore) {
                }
            });
        }

        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });
                if (null == excep.get()) {
                    excep.set(e);
                }
            }
        }

        throttle.stop();

        if (excep.get() != null) {
            throw GeneralUtil.nestedException(excep.get());
        }

        // After all physical table finished
        if (status == ChangeSetCatchUpStatus.ABSENT_FINAL) {
            changeSetMetaManager.getChangeSetReporter()
                .updateChangeSetStatus(ChangeSetMetaManager.ChangeSetStatus.RUNNING_AFTER_CHECK);
        } else if (status == ChangeSetCatchUpStatus.WRITE_ONLY_FINAL) {
            changeSetMetaManager.getChangeSetReporter()
                .updateChangeSetStatus(ChangeSetMetaManager.ChangeSetStatus.SUCCESS);
        }
    }

    public void logicalTableChangeSetRollBack(String logicalTableName,
                                              Map<String, Set<String>> sourcePhyTableNames,
                                              ComplexTaskMetaManager.ComplexTaskType taskType,
                                              Long changeSetId, ExecutionContext originEc) {
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return;
        }

        sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
            for (String phyTableName : phyTableNames) {
                migrateTableRollback(
                    schemaName,
                    logicalTableName,
                    sourceGroupName,
                    phyTableName,
                    taskType,
                    originEc
                );
            }
        });

        // After all physical table finished
        changeSetMetaManager.getChangeSetReporter().loadChangeSetMeta(changeSetId);
        changeSetMetaManager.getChangeSetReporter().updateChangeSetStatus(ChangeSetMetaManager.ChangeSetStatus.FAILED);
    }

    /**
     * only start change set for phy table
     */
    public void migrateTableStart(String schema, String logicalTableName,
                                  String sourceGroup, String sourceTable,
                                  ComplexTaskMetaManager.ComplexTaskType taskType,
                                  ExecutionContext originEc) {
        ExecutionContext ec = originEc.copy();
        ec.getAsyncDDLContext().setAsyncDDLSupported(false);

        final ParamManager pm = OptimizerContext.getContext(schemaName).getParamManager();
        ChangeSetParams params = new ChangeSetParams(pm);

        final String physicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(sourceGroup);

        ChangeSetMeta meta = new ChangeSetMeta();
        meta.setSchemaName(schema);
        meta.setSourceTableName(logicalTableName);
        meta.setSourceGroup(sourceGroup);
        meta.setSourcePhysicalDb(physicalDb);
        meta.setSourcePhysicalTable(sourceTable);
        meta.setTaskType(taskType);

        ChangeSetData data = new ChangeSetData(meta);
        ChangeSetTask task = new ChangeSetTask(data, params);

        try {
            task.setTaskStatus(ChangeSetTaskStatus.INIT);
            startChangeSet(ec, task);
        } catch (Exception e) {
            task.setTaskStatus(ChangeSetTaskStatus.ERROR);
            LOG.error(String.format("changeset %s failed with exception: %s", task, e));
            throw e;
        }

        LOG.info(String.format("finish start changeset task %s", task));
    }

    /**
     * copy baseline
     */
    public void migrateTableCopyBaseline(String schema, String logicalTableName,
                                         String sourceGroup, String targetGroup,
                                         String sourceTable, String targetTable,
                                         ComplexTaskMetaManager.ComplexTaskType taskType, Long taskId,
                                         ExecutionContext originEc) {
        LOG.info(String.format("migrate table copy baseline %s.%s", schema, logicalTableName));

        ExecutionContext ec = originEc.copy();
        ec.getAsyncDDLContext().setAsyncDDLSupported(false);

        final ParamManager pm = OptimizerContext.getContext(schemaName).getParamManager();
        ChangeSetParams params = new ChangeSetParams(pm);

        final String physicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(sourceGroup);
        final String targetPhysicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(targetGroup);

        ChangeSetMeta meta = new ChangeSetMeta(
            schema, logicalTableName, null,
            sourceGroup, targetGroup,
            physicalDb, targetPhysicalDb,
            sourceTable, targetTable,
            null, taskType
        );

        ChangeSetData data = new ChangeSetData(meta);
        ChangeSetTask task = new ChangeSetTask(data, params);

        try {
            task.setTaskStatus(ChangeSetTaskStatus.COPY);
            copyBaseline(ec, task, taskId);
        } catch (Exception e) {
            task.setTaskStatus(ChangeSetTaskStatus.ERROR);
            LOG.error(String.format("changeset copy baseline %s failed with exception: %s", task, e));
            throw e;
        }

        LOG.info(String.format("finish copy baseline %s", task));
    }

    /**
     * changeset catch up
     * 1. move partition
     * sourceGroup1[physicalTableP1] --> targetGroup2[physicalTableP1]
     * eg: WUMU_P00000[tbl_xxxx_00000]  --> WUMU_P00001[tbl_xxxx_00000]
     * <p>
     * 2. merge partition
     * sourceGroup1[physicalTableP1] &
     * sourceGroup2[physicalTableP2]  -->  targetGroup3[physicalTableP3]
     * eg: WUMU_P00000[tbl_xxxx_00000] &
     * WUMU_P00001[tbl_xxxx_00001]  --> WUMU_P00002[tbl_xxxx_00002]
     * <p>
     * 3. split partition
     * sourceGroup1[physicalTableP1] --> targetGroup2[physicalTableP2] & targetGroup3[physicalTableP3]
     * eg: WUMU_P00000[tbl_xxxx_00000] --> WUMU_P00001[tbl_xxxx_00001] & WUMU_P00002[tbl_xxxx_00002]
     */
    public void migrateTableCatchup(String schema, String logicalTableName,
                                    String sourceGroup, String sourceTable,
                                    Map<String, String> targetTableLocations,
                                    ComplexTaskMetaManager.ComplexTaskType taskType,
                                    ChangeSetCatchUpStatus status, ExecutionContext originEc) {
        LOG.info(String.format("migrate table catch up %s.%s", schema, logicalTableName));

        ExecutionContext ec = originEc.copy();
        ec.getAsyncDDLContext().setAsyncDDLSupported(false);

        Pair<String, String> groupAndPhyTable =
            ChangeSetUtils.getTargetGroupNameAndPhyTableName(sourceTable, sourceGroup, taskType, targetTableLocations);

        final String physicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(sourceGroup);
        final String targetPhysicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(groupAndPhyTable.getKey());

        ChangeSetMeta meta = new ChangeSetMeta(
            schema, logicalTableName, null,
            sourceGroup, groupAndPhyTable.getKey(),
            physicalDb, targetPhysicalDb,
            sourceTable, groupAndPhyTable.getValue(),
            targetTableLocations, taskType
        );

        final ParamManager pm = OptimizerContext.getContext(schemaName).getParamManager();
        ChangeSetParams params = new ChangeSetParams(pm);

        // 判断是否需要断点续传
        params.setReCatchup(changeSetMetaManager.getChangeSetReporter().needReCatchUp(sourceGroup, sourceTable));

        ChangeSetData data = new ChangeSetData(meta);
        ChangeSetTask task = new ChangeSetTask(data, params);

        try {
            changeSetMetaManager.getChangeSetReporter().updateCatchUpStart(sourceGroup, sourceTable);
            if (status.isWriteOnly()) {
                task.setTaskStatus(ChangeSetTaskStatus.CATCHUP_WO);
                catchUpOnce(ec, task, status);
            } else if (status.isDeleteOnly()) {
                task.setTaskStatus(ChangeSetTaskStatus.CATCHUP_DO);
                catchUpOnce(ec, task, status);
            } else {
                task.setTaskStatus(ChangeSetTaskStatus.CATCHUP);
                catchUpByChangeSet(ec, task, status);
            }
            if (status == ChangeSetCatchUpStatus.WRITE_ONLY_FINAL) {
                task.setTaskStatus(ChangeSetTaskStatus.FINISH);
                finishChangeSet(ec, task);
            }
            changeSetMetaManager.getChangeSetReporter().updatePositionMark(task);
        } catch (Exception e) {
            task.setTaskStatus(ChangeSetTaskStatus.ERROR);
            LOG.error(String.format("changeset %s failed with exception: %s", task, e));
            throw e;
        }

        LOG.info(String.format("finish move table task %s", task));
    }

    /**
     * changeset rollback, stop changeset
     */
    public void migrateTableRollback(String schema,
                                     String logicalTableName,
                                     String sourceGroup,
                                     String sourceTable,
                                     ComplexTaskMetaManager.ComplexTaskType taskType,
                                     ExecutionContext originEc) {
        LOG.info(String.format("rollback migrate table %s.%s", schema, logicalTableName));

        ExecutionContext ec = originEc.copy();
        ec.getAsyncDDLContext().setAsyncDDLSupported(false);

        final String physicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(sourceGroup);

        ChangeSetMeta meta = new ChangeSetMeta();
        meta.setSchemaName(schema);
        meta.setSourceTableName(logicalTableName);
        meta.setSourceGroup(sourceGroup);
        meta.setSourcePhysicalDb(physicalDb);
        meta.setSourcePhysicalTable(sourceTable);
        meta.setTaskType(taskType);

        ChangeSetData data = new ChangeSetData(meta);
        ChangeSetTask task = new ChangeSetTask(data, null);
        task.setTaskStatus(ChangeSetTaskStatus.ROLLBACK);

        finishChangeSet(ec, task);
    }

    private void startChangeSet(ExecutionContext ec, ChangeSetTask task) {
        LOG.info(String.format("changeset %s start", task));

        final ChangeSetMeta meta = task.getData().getMeta();
        final ChangeSetParams params = task.getParams();

        final String table = TStringUtil.quoteString(meta.getSourcePhysicalTable().toLowerCase());
        final String sql = String.format(ChangeSetUtils.SQL_START_CHANGESET, table, params.getTaskMemoryLimitBytes());
        try {
            ChangeSetUtils.execGroup(ec, meta.getSchemaName(), meta.getSourceGroup(), sql);
        } catch (RuntimeException e) {
            if (e.getCause() == null || !e.getCause().getMessage().contains("changeset already in progress")) {
                throw e;
            }
        }
    }

    private void finishChangeSet(ExecutionContext ec, ChangeSetTask task) {
        LOG.info(String.format("changeset %s finish", task));

        final ChangeSetMeta meta = task.getData().getMeta();

        final String table = TStringUtil.quoteString(meta.getSourcePhysicalTable().toLowerCase());
        final String sql = String.format(ChangeSetUtils.SQL_FINISH_CHANGESET, table);
        try {
            ChangeSetUtils.execGroup(ec, meta.getSchemaName(), meta.getSourceGroup(), sql);
        } catch (RuntimeException e) {
            if (e.getCause() == null || (!e.getCause().getMessage().contains("changeset not exists") &&
                !e.getCause().getMessage().contains("PROCEDURE polarx.changeset_finish does not exist"))) {
                throw e;
            }
        }
    }

    public boolean canCatchupTaskLoopBreak(ExecutionContext executionContext) {
        return this.changeSetMetaManager.canCatchupStop(executionContext.getDdlJobId());
    }

    private void copyBaseline(ExecutionContext executionContext, ChangeSetTask task, Long taskId) {
        final ChangeSetMeta meta = task.getData().getMeta();
        executionContext = executionContext.copy();
        executionContext.setBackfillId(taskId);
        executionContext.setSchemaName(meta.getSchemaName());

        Map<String, Set<String>> sourcePhyTables = new HashMap<>(1);
        Map<String, Set<String>> targetPhyTables = new HashMap<>(1);
        Map<String, String> sourceTargetGroup = new HashMap<>(1);

        sourcePhyTables.put(meta.getSourceGroup(), Collections.singleton(meta.getSourcePhysicalTable()));
        targetPhyTables.put(meta.getTargetGroup(), Collections.singleton(meta.getTargetPhysicalTable()));
        sourceTargetGroup.put(meta.getSourceGroup(), meta.getTargetGroup());

    }

    private void catchUpByChangeSet(ExecutionContext ec, ChangeSetTask task, ChangeSetCatchUpStatus status) {
        LOG.info(String.format("changeset %s start catchup", task));

        int count = 0;
        int replayTimes = task.getParams().getReplayTimes();

        while (count != replayTimes) {
            if (!catchUpOnce(ec, task, status)) {
                break;
            }
            count++;
        }

        LOG.info(String.format("changeset %s finish catchup, times : %d", task, count));
    }

    private boolean catchUpOnce(ExecutionContext ec, ChangeSetTask task, ChangeSetCatchUpStatus status) {
        int count = 0;
        boolean res = true;
        int changeSetTimes = getFetchChangeSetTimes(ec, task);

        for (; count != changeSetTimes; count++) {
            // fetch change set pks
            fetchChangeSet(ec, task);
            if (task.getData().isEmpty()) {
                res = false;
                break;
            }

            // replay change set
            replayChangeSet(ec, task, !status.isAbsent());

            // interrupt
            if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()) {
                long jobId = ec.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
        }

        LOG.info(
            String.format("changeset %s catchup once, times %d (predicted times %d)", task, count, changeSetTimes));

        return res;
    }

    private int getFetchChangeSetTimes(ExecutionContext ec, ChangeSetTask task) {
        final ChangeSetMeta meta = task.getData().getMeta();

        final String table = TStringUtil.quoteString(meta.getSourcePhysicalTable().toLowerCase());
        final String sql = String.format(ChangeSetUtils.SQL_FETCH_CHANGESET_TIMES, table);

        List<List<Object>> result =
            ChangeSetUtils.queryGroup(ec, meta.getSchemaName(), meta.getSourceGroup(), sql);

        return ChangeSetData.getFetchTimes(result);
    }

    private void fetchChangeSet(ExecutionContext ec, ChangeSetTask task) {
        final ChangeSetMeta meta = task.getData().getMeta();

        final String table = TStringUtil.quoteString(meta.getSourcePhysicalTable().toLowerCase());

        int param = task.getParams().isReCatchup() ? 1 : 0;
        final String sql = String.format(ChangeSetUtils.SQL_FETCH_CAHNGESET, table, param);
        task.getParams().setReCatchup(false);

        List<List<Object>> changeSet =
            ChangeSetUtils.queryGroup(ec, meta.getSchemaName(), meta.getSourceGroup(), sql);
        ChangeSetData.buildPkResultList(task.getData(), meta.getFullSourceTable(), changeSet);

        // stats info
        task.getData().increaseFetchTimes();
    }

    private void replayChangeSet(ExecutionContext ec, ChangeSetTask task, boolean lock) {
        if (task.getData().isEmpty()) {
            return;
        }

        if (ec.getParamManager().getBoolean(ConnectionParams.SKIP_CHANGE_SET_APPLY)) {
            return;
        }

        final ChangeSetMeta meta = task.getData().getMeta();
        final Map<String, List<Row>> insertData = task.getData().getInsertKeys();
        final Map<String, List<Row>> deleteData = task.getData().getDeleteKeys();

        final List<Row> insertKeys = insertData.get(meta.getFullSourceTable());
        final List<Row> deleteKeys = deleteData.get(meta.getFullSourceTable());

        int batchSize = task.getParams().getCatchupBatchSize();
        ITransactionManager tm = ExecutorContext.getContext(meta.getSchemaName()).getTransactionManager();

        int phyParallelism = OptimizerContext.getContext(schemaName).getParamManager()
            .getInt(ConnectionParams.CHANGE_SET_APPLY_PHY_PARALLELISM);
        final Semaphore semaphore = new Semaphore(phyParallelism);

        List<Future> futures = new ArrayList<>(16);

        if (deleteKeys != null && !deleteKeys.isEmpty()) {
            List<List<Row>> deleteKeysList = genBatchRowList(deleteKeys, batchSize);
            for (List<Row> rowPks : deleteKeysList) {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TddlNestableRuntimeException(e);
                }

                FutureTask<Void> futureTask = new FutureTask<>(
                    () -> {
                        try {
                            PhyTableOperation selectPlan = genPhySelectPlan(ec, task, rowPks, lock);

                            GsiUtils.retryOnException(
                                () -> GsiUtils.wrapWithDistributedTrx(tm, ec, (insertEc) -> {
                                    try {
                                        applyBatchDelete(insertEc, task, selectPlan, rowPks, lock);
                                        insertEc.getTransaction().commit();
                                        return true;
                                    } catch (Exception e) {
                                        LOG.error("apply change set statement failed!", e);
                                        throw e;
                                    }
                                }),
                                e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                                    || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT)
                                    || e.getMessage().contains("Deadlock found when trying to get lock")),
                                (e, retryCount) -> ChangeSetUtils.deadlockErrConsumer(selectPlan, ec, e, retryCount));

                        } finally {
                            semaphore.release();
                        }
                    }, null);

                futures.add(futureTask);

                ec.getExecutorService().submit(ec.getSchemaName(), ec.getTraceId(), AsyncTask.build(futureTask));
            }
        }

        waitApplyFinish(futures);

        if (insertKeys != null && !insertKeys.isEmpty()) {
            List<List<Row>> insertKeysList = genBatchRowList(insertKeys, batchSize);
            for (List<Row> rowPks : insertKeysList) {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TddlNestableRuntimeException(e);
                }

                FutureTask<Void> futureTask = new FutureTask<>(
                    () -> {
                        try {
                            PhyTableOperation selectPlan = genPhySelectPlan(ec, task, rowPks, lock);

                            // todo (wumu): adjust batch size, avoid the data size of param too big
                            GsiUtils.retryOnException(
                                () -> GsiUtils.wrapWithDistributedTrx(tm, ec, (insertEc) -> {
                                    try {
                                        applyBatchInsert(insertEc, task, selectPlan, rowPks, lock);
                                        insertEc.getTransaction().commit();
                                        return true;
                                    } catch (Exception e) {
                                        LOG.error("apply change set statement failed!", e);
                                        throw e;
                                    }
                                }),
                                e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                                    || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT)
                                    || e.getMessage().contains("Deadlock found when trying to get lock")),
                                (e, retryCount) -> ChangeSetUtils.deadlockErrConsumer(selectPlan, ec, e, retryCount));

                        } finally {
                            semaphore.release();
                        }
                    }, null);

                futures.add(futureTask);

                ec.getExecutorService().submit(ec.getSchemaName(), ec.getTraceId(), AsyncTask.build(futureTask));
            }
        }
        waitApplyFinish(futures);

        task.getData().increaseReplayTimes();
        task.getData().clear();
    }

    private void waitApplyFinish(List<Future> futures) {
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });

                throw GeneralUtil.nestedException(e);
            }
        }

        futures.clear();
    }

    public PhyTableOperation genPhySelectPlan(ExecutionContext ec, ChangeSetTask task, List<Row> rowPks, boolean lock) {
        final ChangeSetMeta meta = task.getData().getMeta();

        return ChangeSetUtils.buildSelectWithInPk(
            meta.getSchemaName(),
            meta.getSourceTableName(),
            meta.getSourceGroup(),
            meta.getSourcePhysicalTable(),
            rowPks,
            ec,
            lock
        );
    }

    public Parameters selectRowByPksFromSourceTable(ExecutionContext ec, ChangeSetTask task, List<Row> rowPks,
                                                    PhyTableOperation selectPlan, boolean lock, boolean forDelete) {
        long start, end;
        final boolean debugMode = task.getParams().debugMode;

        start = System.currentTimeMillis();

        Parameters parameters = ChangeSetUtils.executePhySelectPlan(selectPlan, ec);

        end = System.currentTimeMillis();

        if (debugMode) {
            LOG.info(String.format(
                "changeset replay task %s: select pk num %d （tot %d）,time: %d ms, for delete [%s], sql: %s",
                task,
                parameters.getBatchParameters().size(),
                rowPks.size(),
                (end - start),
                forDelete,
                selectPlan.getNativeSql())
            );
            if (lock) {
                LOG.info(String.format("changeset replay task %s, sql %s",
                    task,
                    selectPlan.getNativeSql())
                );
            }
        }

        return parameters;
    }

    public void replaceRowToTargetTablesWithReShard(ExecutionContext ec, ChangeSetTask task, Parameters parameters,
                                                    boolean afterDelete) {
        final ChangeSetMeta meta = task.getData().getMeta();

        if (meta.getTargetGroup() == null && meta.getTargetPhysicalTable() == null) {
            // shard
            Map<String, Map<String, Parameters>> shardResult =
                BuildPlanUtils.getShardResults(schemaName, meta.getSourceTableName(), parameters, ec);

            shardResult.forEach((group, tbResult) -> {
                tbResult.forEach((phyTb, result) -> {
                    replaceRowToTargetTable(ec, task, result, group, phyTb, afterDelete);
                });
            });
        } else {
            replaceRowToTargetTable(ec, task, parameters, meta.getTargetGroup(),
                meta.getTargetPhysicalTable(), afterDelete);
        }
    }

    public void replaceRowToTargetTable(ExecutionContext ec, ChangeSetTask task, Parameters parameters,
                                        String targetGroup, String targetPhyTable, boolean afterDelete) {
        long start, end;
        final ChangeSetMeta meta = task.getData().getMeta();
        final boolean debugMode = task.getParams().debugMode;

        PhyTableOperation replacePlan = ChangeSetUtils.buildReplace(
            meta.getSchemaName(),
            meta.getSourceTableName(),
            targetGroup,
            targetPhyTable,
            parameters,
            ec
        );

        start = System.currentTimeMillis();
        int replaceRow = ChangeSetUtils.executePhyWritePlan(replacePlan, ec);
        ec.getStats().changeSetReplaceRows.addAndGet(replaceRow);
        task.getData().increaseReplaceRowCount(replaceRow);
        end = System.currentTimeMillis();

        if (debugMode) {
            LOG.info(String.format(
                "changeset replay task %s: replace pk num %d, time: %d ms, after delete [%s], targetPhyTable name [%s.%s] param %s",
                task,
                replaceRow,
                (end - start),
                afterDelete,
                targetGroup,
                targetPhyTable,
                parameters)
            );
        }
    }

    public void deleteRowFromTargetTableWithReShard(ExecutionContext ec, ChangeSetTask task, List<Row> rowPks,
                                                    boolean lock) {
        final ChangeSetMeta meta = task.getData().getMeta();
        Map<String, String> targetTableLocations = meta.getTargetTableLocations();

        if (meta.getTargetGroup() == null && meta.getTargetPhysicalTable() == null) {
            targetTableLocations.forEach((tb, group) -> {
                deleteRowFromTargetTable(ec, task, group, tb, rowPks, lock);
            });
        } else {
            deleteRowFromTargetTable(ec, task, meta.getTargetGroup(), meta.getTargetPhysicalTable(), rowPks, lock);
        }
    }

    public void deleteRowFromTargetTable(ExecutionContext ec, ChangeSetTask task,
                                         String targetGroup, String targetPhyTable,
                                         List<Row> rowPks, boolean lock) {
        long start, end;
        final ChangeSetMeta meta = task.getData().getMeta();
        final boolean debugMode = task.getParams().debugMode;

        PhyTableOperation deletePlan = ChangeSetUtils.buildDeleteWithInPk(
            meta.getSchemaName(),
            meta.getSourceTableName(),
            targetGroup,
            targetPhyTable,
            rowPks,
            ec
        );

        start = System.currentTimeMillis();
        int deleteRow = ChangeSetUtils.executePhyWritePlan(deletePlan, ec);
        ec.getStats().changeSetDeleteRows.addAndGet(deleteRow);
        task.getData().increaseDeleteRowCount(deleteRow);
        end = System.currentTimeMillis();
        if (debugMode) {
            LOG.info(String.format("changeset replay task %s : delete pk num %d, time: %d ms",
                task,
                deleteRow,
                (end - start))
            );
            if (lock) {
                LOG.info(String.format("changeset replay task %s, sql %s",
                    task,
                    deletePlan.getNativeSql())
                );
            }
        }
    }

    public void applyBatchDelete(ExecutionContext ec, ChangeSetTask task, PhyTableOperation selectPlan,
                                 List<Row> rowPks, boolean lock) {

        if (rateLimiter != null) {
            rateLimiter.acquire(task.getParams().catchupBatchSize);
        }
        long start = System.currentTimeMillis();

        // Dynamic adjust lower bound of rate.
        final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
        if (dynamicRate > 0) {
            throttle.resetMaxRate(dynamicRate);
        }

        Parameters parameters = null;
        if (lock) {
            // select lock in share mode
            parameters = selectRowByPksFromSourceTable(ec, task, rowPks, selectPlan, true, true);
        }

        // replay delete op
        deleteRowFromTargetTableWithReShard(ec, task, rowPks, lock);

        if (parameters != null && !parameters.getBatchParameters().isEmpty()) {
            // replace rows which was reinserted
            replaceRowToTargetTablesWithReShard(ec, task, parameters, true);

            throttle.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
                System.currentTimeMillis() - start, start, parameters.getBatchParameters().size()));
        }

        DdlEngineStats.METRIC_CHANGESET_APPLY_ROWS_SPEED.set((long) throttle.getActualRateLastCycle());

        if (rateLimiter != null) {
            // Limit rate.
            rateLimiter.setRate(throttle.getNewRate());
        }
    }

    public void applyBatchInsert(ExecutionContext ec, ChangeSetTask task, PhyTableOperation selectPlan,
                                 List<Row> rowPks, boolean lock) {

        if (rateLimiter != null) {
            rateLimiter.acquire(task.getParams().catchupBatchSize);
        }
        long start = System.currentTimeMillis();

        // Dynamic adjust lower bound of rate.
        final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
        if (dynamicRate > 0) {
            throttle.resetMaxRate(dynamicRate);
        }

        Parameters parameters = selectRowByPksFromSourceTable(ec, task, rowPks, selectPlan, lock, false);

        if (parameters.getBatchParameters().isEmpty()) {
            LOG.info(String.format("changeset replay task %s, fetch pk not found", task));
            return;
        }

        // replay insert op
        replaceRowToTargetTablesWithReShard(ec, task, parameters, false);

        throttle.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
            System.currentTimeMillis() - start, start, parameters.getBatchParameters().size()));

        DdlEngineStats.METRIC_CHANGESET_APPLY_ROWS_SPEED.set((long) throttle.getActualRateLastCycle());

        if (rateLimiter != null) {
            // Limit rate.
            rateLimiter.setRate(throttle.getNewRate());
        }
    }

    public enum ChangeSetCatchUpStatus {
        ABSENT(0),
        DELETE_ONLY(1),
        WRITE_ONLY(2),
        ABSENT_FINAL(3),
        DELETE_ONLY_FINAL(4),
        WRITE_ONLY_FINAL(5);

        private final int value;

        ChangeSetCatchUpStatus(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            switch (value) {
            case 0:
                return "ABSENT";
            case 1:
                return "DELETE_ONLY";
            case 2:
                return "WRITE_ONLY";
            case 3:
                return "ABSENT_FINAL";
            case 4:
                return "DELETE_ONLY_FINAL";
            case 5:
                return "WRITE_ONLY_FINAL";
            default:
                return null;
            }
        }

        public int getValue() {
            return value;
        }

        public boolean isWriteOnly() {
            return WRITE_ONLY == this || WRITE_ONLY_FINAL == this;
        }

        public boolean isDeleteOnly() {
            return DELETE_ONLY == this || DELETE_ONLY_FINAL == this;
        }

        public boolean isAbsent() {
            return ABSENT == this || ABSENT_FINAL == this;
        }
    }

    enum ChangeSetTaskStatus {
        INIT,
        COPY,
        CATCHUP,
        CATCHUP_DO,
        CATCHUP_WO,
        FINISH,
        ERROR,
        ROLLBACK,
    }

    @Data
    static class ChangeSetMeta {
        public String schemaName;

        public String sourceTableName;
        public String targetTableName;

        // group name like GROUP_000001
        public String sourceGroup;
        public String targetGroup;

        // physical database name
        public String sourcePhysicalDb;
        public String targetPhysicalDb;

        // physical table
        public String sourcePhysicalTable;
        public String targetPhysicalTable;

        public Map<String, String> targetTableLocations;

        public ComplexTaskMetaManager.ComplexTaskType taskType;

        // topology of logical table
        public Map<String, List<String>> topology = new HashMap<>();

        public ChangeSetMeta() {
        }

        public ChangeSetMeta(String schemaName,
                             String sourceTableName, String targetTableName,
                             String sourceGroup, String targetGroup,
                             String sourcePhysicalDb, String targetPhysicalDb,
                             String sourcePhysicalTable, String targetPhysicalTable,
                             Map<String, String> targetTableLocations,
                             ComplexTaskMetaManager.ComplexTaskType taskType) {
            this.schemaName = schemaName;
            this.sourceTableName = sourceTableName;
            this.targetTableName = targetTableName;
            this.sourceGroup = sourceGroup;
            this.targetGroup = targetGroup;
            this.sourcePhysicalDb = sourcePhysicalDb;
            this.targetPhysicalDb = targetPhysicalDb;
            this.sourcePhysicalTable = sourcePhysicalTable;
            this.targetPhysicalTable = targetPhysicalTable;
            this.targetTableLocations = targetTableLocations;
            this.taskType = taskType;
        }

        public String getChangeSetName() {
            return String.format("%s.%s", this.schemaName.toLowerCase(), this.sourceTableName.toLowerCase());
        }

        public String getSourceDbGroupName() {
            return GroupInfoUtil.buildGroupNameFromPhysicalDb(sourcePhysicalDb);
        }

        public String getFullSourceTable() {
            return TStringUtil.concatTableName(sourcePhysicalDb, sourcePhysicalTable);
        }

        public String getFullTargetTable() {
            return TStringUtil.concatTableName(targetPhysicalDb, targetPhysicalTable);
        }

        public void initTopology() {
            if (!topology.isEmpty()) {
                return;
            }

            final OptimizerContext context = OptimizerContext.getContext(schemaName);
            //test the table existence
            TableMeta tableMeta = context.getLatestSchemaManager().getTable(sourceTableName);
            final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

            Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
                partitionInfo.getPhysicalPartitionTopology(new ArrayList<>());

            physicalPartitionInfos.forEach((grpGroupKey, phyPartList) -> {
                topology.putIfAbsent(grpGroupKey,
                    phyPartList.stream().map(PhysicalPartitionInfo::getPhyTable).collect(Collectors.toList()));
            });
        }

        public Pair<List<String>, List<Integer>> getPrimaryKeyNameAndIndex() {
            SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
            TableMeta tableMeta = sm.getTable(sourceTableName);
            return GlobalIndexMeta.getPrimaryKeysNotOrdered(tableMeta);
        }
    }

    @Data
    static class ChangeSetData {
        public ChangeSetMeta meta;
        public int primaryKeySize;

        public long fetchTimes;
        public long replayTimes;
        public long deleteRowCount;
        public long replaceRowCount;

        /**
         * INSERT:
         * Each inserted row is the same as data in source table
         * <p>
         * DELETE:
         * Each deleteKeys contains only primary-keys of deleted row from source table
         */
        public Map<String, List<Row>> insertKeys = new HashMap<>();
        public Map<String, List<Row>> deleteKeys = new HashMap<>();

        public ChangeSetData() {
        }

        public ChangeSetData(ChangeSetMeta meta) {
            this.meta = meta;
            this.fetchTimes = 0;
            this.replayTimes = 0;
            this.deleteRowCount = 0;
            this.replaceRowCount = 0;
        }

        /**
         * +--------+------+---------------------+------+
         * | OP     | a    | b                   | c    |
         * +--------+------+---------------------+------+
         * | INSERT | wumu | 2022-04-27 14:49:58 | 1.40 |
         * +--------+------+---------------------+------+
         */
        public static void buildPkResultList(ChangeSetData changeSetData,
                                             String fullTableName, List<List<Object>> rows) {
            if (changeSetData == null || rows == null || rows.isEmpty()) {
                return;
            }
            ChangeSetMeta meta = changeSetData.getMeta();
            CursorMeta cursorMeta = ChangeSetUtils.buildCursorMeta(meta.getSchemaName(), meta.getSourceTableName());

            List<Row> insertKeys =
                changeSetData.insertKeys.computeIfAbsent(fullTableName, x -> new ArrayList<>());
            List<Row> deleteKeys =
                changeSetData.deleteKeys.computeIfAbsent(fullTableName, x -> new ArrayList<>());

            for (List<Object> row : rows) {
                // get pk
                String changeType = (String) row.get(0);
                row.remove(0);
                for (int i = 0; i < row.size(); ++i) {
                    row.set(i, ChangeSetUtils.convertObject(cursorMeta.getColumnMeta(i), row.get(i)));
                }
                ArrayRow realRow = new ArrayRow(cursorMeta, row.toArray());

                if (changeType.equalsIgnoreCase("INSERT")) {
                    insertKeys.add(realRow);
                } else if (changeType.equalsIgnoreCase("DELETE")) {
                    deleteKeys.add(realRow);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                        String.format("Unknown change type: %s", changeType));
                }
            }

            List<OrderByOption> orderBys = new ArrayList<>();
            for (int i = 0; i < rows.get(0).size(); ++i) {
                orderBys.add(new OrderByOption(i, RelFieldCollation.Direction.ASCENDING,
                    RelFieldCollation.NullDirection.UNSPECIFIED));
            }
            Comparator<Row> orderComparator = ExecUtils.getComparator(orderBys,
                cursorMeta.getColumns().stream().map(ColumnMeta::getDataType)
                    .collect(Collectors.toList())
            );
            insertKeys.sort(orderComparator);
            deleteKeys.sort(orderComparator);
        }

        public static int getFetchTimes(List<List<Object>> rows) {
            if (rows == null || rows.isEmpty()) {
                return 0;
            }

            assert rows.size() == 1;
            assert rows.get(0).size() == 2;

            return ((Long) rows.get(0).get(1)).intValue();
        }

        public void increaseFetchTimes() {
            this.fetchTimes++;
        }

        public void increaseReplayTimes() {
            this.replayTimes++;
        }

        public void increaseDeleteRowCount(long deleteRowCount) {
            this.deleteRowCount += deleteRowCount;
        }

        public void increaseReplaceRowCount(long replaceRowCount) {
            this.replaceRowCount += replaceRowCount;
        }

        public boolean isEmpty() {
            return MapUtils.isEmpty(this.deleteKeys) && MapUtils.isEmpty(this.insertKeys);
        }

        public void clear() {
            insertKeys.clear();
            deleteKeys.clear();
        }
    }

    @Data
    static class ChangeSetParams {
        public long taskMemoryLimitBytes;

        /**
         * Parameters of catchup
         */
        public int catchupBatchSize;
        public long catchupSpeedLimit;
        public long catchupSpeedMin;

        public int replayTimes;

        /**
         * when ddl leader is changed / restart
         */
        public boolean reCatchup;
        public boolean debugMode;

        public ChangeSetParams(ParamManager pm) {
            this.taskMemoryLimitBytes = pm.getLong(ConnectionParams.CHANGE_SET_MEMORY_LIMIT);
            this.catchupBatchSize = pm.getInt(ConnectionParams.CHANGE_SET_APPLY_BATCH);
            this.catchupSpeedLimit = pm.getLong(ConnectionParams.CHANGE_SET_APPLY_SPEED_LIMITATION);
            this.catchupSpeedMin = pm.getLong(ConnectionParams.CHANGE_SET_APPLY_SPEED_MIN);
            this.replayTimes = pm.getInt(ConnectionParams.CHANGE_SET_REPLAY_TIMES);
            this.debugMode = pm.getBoolean(ConnectionParams.CHANGE_SET_DEBUG_MODE);
            this.reCatchup = false;
        }
    }

    @Data
    static class ChangeSetTask {
        public ChangeSetData data;
        public ChangeSetParams params;

        public ChangeSetTaskStatus taskStatus = ChangeSetTaskStatus.INIT;

        public ChangeSetTask(ChangeSetData data, ChangeSetParams params) {
            this.data = data;
            this.params = params;
        }

        @Override
        public String toString() {
            ChangeSetMeta meta = getData().getMeta();
            return String.format("ChangeSetTask{Table=%s, taskStatus=%s, phyTableName:%s}",
                meta.getChangeSetName(), taskStatus, meta.getSourcePhysicalTable());
        }
    }

    public static Long getChangeSetId() {
        return ID_GENERATOR.nextId();
    }
}
