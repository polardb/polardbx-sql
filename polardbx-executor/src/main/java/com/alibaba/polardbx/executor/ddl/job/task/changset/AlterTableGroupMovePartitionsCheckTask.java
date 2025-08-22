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

package com.alibaba.polardbx.executor.ddl.job.task.changset;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.ddl.newengine.resource.ResourceContainer;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.fastchecker.CheckerBatch;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupChecker;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupReporter;
import com.alibaba.polardbx.executor.partitionmanagement.fastchecker.AlterTableGroupFastChecker;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TablesMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import io.airlift.slice.DataSize;
import lombok.Getter;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_CPU;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_IO;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.MOVE_PARTITION_BEFORE_CHECK;

@TaskName(name = "AlterTableGroupMovePartitionsCheckTask")
@Getter
public class AlterTableGroupMovePartitionsCheckTask extends BaseBackfillTask {
    final private String logicalTableName;
    final private Map<String, List<String>> ptbGroupMap;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private Map<String, Set<String>> targetPhyTableNames;
    final private Boolean stopDoubleWrite;
    final private List<String> relatedTables;
    final private long dataSize;
    final private String tableGroupName;
    final private List<String> physicalPartitionNames;

    public AlterTableGroupMovePartitionsCheckTask(String schemaName, String logicalTableName,
                                                  Map<String, org.apache.calcite.util.Pair<String, String>> ptbGroupMap,
                                                  Map<String, Set<String>> sourcePhyTableNames,
                                                  Map<String, Set<String>> targetPhyTableNames,
                                                  Boolean stopDoubleWrite,
                                                  List<String> relatedTables,
                                                  Set<String> sourceStorageInsts,
                                                  Set<String> targetStorageInsts,
                                                  long dataSize,
                                                  String tableGroupName,
                                                  List<String> physicalPartitionNames,
                                                  String nothing
    ) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.ptbGroupMap = new HashMap<>();
        if (ptbGroupMap != null) {
            for (Map.Entry<String, org.apache.calcite.util.Pair<String, String>> entry : ptbGroupMap.entrySet()) {
                this.ptbGroupMap.put(entry.getKey(), Lists.newArrayList(entry.getValue().getKey(),
                    entry.getValue().getValue()));
            }
        }
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.targetPhyTableNames = targetPhyTableNames;
        this.stopDoubleWrite = stopDoubleWrite;
        this.relatedTables = relatedTables;
        this.dataSize = dataSize;
        this.tableGroupName = tableGroupName;
        this.physicalPartitionNames = physicalPartitionNames;
        if (sourceStorageInsts != null && targetStorageInsts != null) {
            setResourceAcquired(buildResourceRequired(sourceStorageInsts, targetStorageInsts, dataSize, tableGroupName,
                physicalPartitionNames));
        }
        onExceptionTryRollback();
    }

    @JSONCreator
    public AlterTableGroupMovePartitionsCheckTask(String schemaName, String logicalTableName,
                                                  Map<String, List<String>> ptbGroupMap,
                                                  Map<String, Set<String>> sourcePhyTableNames,
                                                  Map<String, Set<String>> targetPhyTableNames,
                                                  Boolean stopDoubleWrite,
                                                  List<String> relatedTables,
                                                  Set<String> sourceStorageInsts,
                                                  Set<String> targetStorageInsts,
                                                  long dataSize,
                                                  String tableGroupName,
                                                  List<String> physicalPartitionNames
    ) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.ptbGroupMap = ptbGroupMap;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.targetPhyTableNames = targetPhyTableNames;
        this.stopDoubleWrite = stopDoubleWrite;
        this.relatedTables = relatedTables;
        this.dataSize = dataSize;
        this.tableGroupName = tableGroupName;
        this.physicalPartitionNames = physicalPartitionNames;
        if (sourceStorageInsts != null && targetStorageInsts != null) {
            setResourceAcquired(buildResourceRequired(sourceStorageInsts, targetStorageInsts, dataSize, tableGroupName,
                physicalPartitionNames));
        }
        onExceptionTryRollback();
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ROLLBACK_ON_CHECKER)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " force rollback on checker!");
        }
        if (executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_CHANGE_SET_CHECKER) ||
            !executionContext.getParamManager().getBoolean(ConnectionParams.TABLEGROUP_REORG_CHECK_AFTER_BACKFILL)) {
            if (stopDoubleWrite) {
                ChangeSetUtils.doChangeSetSchemaChange(
                    schemaName, logicalTableName,
                    relatedTables, this,
                    ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                    ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY
                );

                ChangeSetUtils.doChangeSetSchemaChange(
                    schemaName, logicalTableName,
                    relatedTables, this,
                    ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                    ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER
                );
            }
            return;
        }

        final boolean useFastChecker =
            FastChecker.isSupported(schemaName) &&
                executionContext.getParamManager()
                    .getBoolean(ConnectionParams.TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER);
        if (stopDoubleWrite) {
            checkWithStopDoubleWrite(executionContext, useFastChecker);
        } else {
            checkWithDoubleCheck(executionContext, useFastChecker);
        }
    }

    DdlEngineResources buildResourceRequired(Set<String> sourceStorageInsts, Set<String> targetStorageInsts,
                                             Long dataSize, String tableGroupName,
                                             List<String> physicalPartitionNames) {
        String owner = "CheckTask:" + logicalTableName;
        DdlEngineResources resourceRequired = new DdlEngineResources();
//        String subJobOwner = DdlEngineResources.concatSubJobOwner(schemaName, tableGroupName, physicalPartitionNames);
//        resourceRequired.requestPhaseLock(MOVE_PARTITION_BEFORE_CHECK, 100L, subJobOwner, ResourceContainer.PHASE_LOCK_END);
        for (String storageInst : sourceStorageInsts) {
            resourceRequired.request(storageInst + DN_IO, 25L, owner);
            resourceRequired.request(storageInst + DN_CPU, 25L, owner);
        }
        for (String storageInst : targetStorageInsts) {
            resourceRequired.request(storageInst + DN_IO, 25L, owner);
            resourceRequired.request(storageInst + DN_CPU, 25L, owner);
        }
        return resourceRequired;
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        if (stopDoubleWrite) {
            // sync to restore the status of table meta
            PreemptiveTime preemptiveTime = PreemptiveTime.getPreemptiveTimeFromExecutionContext(executionContext,
                ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
            SyncManagerHelper.sync(
                new TablesMetaChangePreemptiveSyncAction(schemaName, relatedTables, preemptiveTime), SyncScope.ALL);
        }
    }

    @Override
    protected void rollbackImpl(ExecutionContext executionContext) {
        if (stopDoubleWrite) {
            new DdlEngineAccessorDelegate<Integer>() {
                @Override
                protected Integer invoke() {
                    ComplexTaskMetaManager
                        .updateSubTasksStatusByJobIdAndObjName(getJobId(), schemaName, logicalTableName,
                            ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER,
                            ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                            getConnection());
                    ComplexTaskMetaManager
                        .updateSubTasksStatusByJobIdAndObjName(getJobId(), schemaName, logicalTableName,
                            ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                            ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                            getConnection());
                    try {
                        for (String tbName : relatedTables) {
                            TableInfoManager.updateTableVersionWithoutDataId(schemaName, tbName, getConnection());
                        }
                    } catch (Exception e) {
                        throw GeneralUtil.nestedException(e);
                    }
                    return null;
                }
            }.execute();

            LOGGER.info(String
                .format(
                    "Rollback table status[ schema:%s, table:%s, before state:%s, after state:%s]",
                    schemaName,
                    logicalTableName,
                    ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER.name(),
                    ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG.name()));
        }
    }

    private void checkWithStopDoubleWrite(ExecutionContext executionContext, boolean useFastChecker) {
        // check and unlock
        if (useFastChecker) {
            boolean fastCheck = fastCheckWithCatchEx(executionContext);
            if (!fastCheck) {
                throw GeneralUtil.nestedException(
                    "alter tableGroup checker found error. Please try to rollback/recover this job");
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                "alter tablegroup should use fastchecker to check");
        }
    }

    private void checkWithDoubleCheck(ExecutionContext executionContext, boolean useFastChecker) {
        if (useFastChecker && fastCheckWithCatchEx(executionContext)) {
            // ignore success
        } else {
            checkInCN(executionContext);
        }
    }

    protected boolean fastCheckWithCatchEx(ExecutionContext executionContext) {
        boolean fastCheckSucc = false;
        try {
            fastCheckSucc = fastCheck(executionContext);
        } catch (Throwable ex) {
            fastCheckSucc = false;
            String msg = String.format(
                "Failed to use fastChecker to check alter tablegroup backFill because of throwing exceptions,  so use old checker instead");
            SQLRecorderLogger.ddlLogger.warn(msg, ex);
        }
        return fastCheckSucc;
    }

    boolean fastCheck(ExecutionContext executionContext) {
        long startTime = System.currentTimeMillis();

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "FastChecker for alter tablegroup, schema [{0}] logical table [{1}] start",
            schemaName, logicalTableName));

        Map<Pair<String, String>, List<Pair<String, String>>> srcTarPhyTableMap = null;
        if (GeneralUtil.isNotEmpty(ptbGroupMap)) {
            // ptbGroupMap: table => (sourceGroup => targetGroups)
            // srcTargetGroupMap: sourceGroup => targetGroup
            srcTarPhyTableMap =
                ScaleOutPlanUtil.generateSrcTarPhyTableMapForMovePartition(sourcePhyTableNames, targetPhyTableNames,
                    ptbGroupMap);
        }
        FastChecker fastChecker = AlterTableGroupFastChecker
            .create(schemaName, logicalTableName,
                sourcePhyTableNames, targetPhyTableNames,
                srcTarPhyTableMap,
                true,
                executionContext);
        boolean fastCheckResult = false;

        try {
            fastCheckResult = fastChecker.checkWithChangeSet(executionContext, stopDoubleWrite, this, relatedTables);
        } catch (TddlNestableRuntimeException e) {
            //other exception, we simply throw out
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e,
                "alter tablegroup fastchecker failed to check");
        } finally {
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "FastChecker for alter tablegroup, schema [{0}] logical src table [{1}] finish, time use [{2}], check result [{3}]",
                schemaName, logicalTableName,
                (System.currentTimeMillis() - startTime) / 1000.0,
                fastCheckResult ? "pass" : "not pass")
            );
            if (!fastCheckResult) {
                EventLogger.log(EventType.DDL_WARN, "FastChecker failed");
            } else {
                EventLogger.log(EventType.DDL_INFO, "FastChecker succeed");
            }
        }

        return fastCheckResult;
    }

    private void checkInCN(ExecutionContext executionContext) {
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_BATCH_SIZE);
        final long speedLimit =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_SPEED_LIMITATION);
        final long speedMin =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_SPEED_MIN);
        final long parallelism =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_PARALLELISM);
        final long earlyFailNumber =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_EARLY_FAIL_NUMBER);
        final boolean useBinary = executionContext.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);

        Checker checker = AlterTableGroupChecker.create(schemaName,
            logicalTableName,
            logicalTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            SqlSelect.LockMode.UNDEF,
            SqlSelect.LockMode.UNDEF,
            executionContext,
            sourcePhyTableNames,
            targetPhyTableNames);
        checker.setInBackfill(true);

        if (null == executionContext.getDdlJobId() || 0 == executionContext.getDdlJobId()) {
            checker.setJobId(JOB_ID_GENERATOR.nextId());
        } else {
            checker.setJobId(executionContext.getDdlJobId());
        }

        // Run the simple check.
        final Reporter reporter = new AlterTableGroupReporter(earlyFailNumber);
        try {
            checker.check(executionContext, reporter);
        } catch (TddlNestableRuntimeException e) {
            if (e.getMessage().contains("Too many conflicts")) {
                throw GeneralUtil
                    .nestedException(
                        "alter tableGroup checker error limit exceeded. Please try to rollback/recover this job");
            } else {
                throw e;
            }
        }

        final List<CheckerManager.CheckerReport> checkerReports = reporter.getCheckerReports();
        if (!checkerReports.isEmpty()) {
            // Some error found.
            throw GeneralUtil.nestedException(
                "alter tableGroup checker found error after backfill. Please try to rollback/recover this job");
        }
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("|checker detail: ");
        sb.append("table_schema [");
        sb.append(schemaName);
        sb.append("] ");
        sb.append("table [");
        sb.append(logicalTableName);
        sb.append("] ");
        sb.append("source physical table info [");
        int j = 0;
        for (Map.Entry<String, Set<String>> entry : sourcePhyTableNames.entrySet()) {
            if (j > 0) {
                sb.append(", ");
            }
            j++;
            sb.append("(");
            sb.append(entry.getKey()).append(":");
            int i = 0;
            for (String tbName : entry.getValue()) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(tbName);
                i++;
            }
            sb.append(")");
        }
        sb.append("], target physical table info [");
        j = 0;
        for (Map.Entry<String, Set<String>> entry : targetPhyTableNames.entrySet()) {
            if (j > 0) {
                sb.append(", ");
            }
            j++;
            sb.append("(");
            sb.append(entry.getKey()).append(":");
            int i = 0;
            for (String tbName : entry.getValue()) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(tbName);
                i++;
            }
            sb.append(")");
        }
        sb.append("], size [");
        sb.append(DataSize.succinctBytes(dataSize));
        sb.append("]");
        return sb.toString();
    }

    @Override
    public List<String> explainInfo() {
        String backfillTask = "ALTERTABLEGROUP_MOVEPARTITIONS_CHECK_TASK(" + logicalTableName + ")";
        List<String> command = new ArrayList<>(1);
        command.add(backfillTask);
        return command;
    }
}
