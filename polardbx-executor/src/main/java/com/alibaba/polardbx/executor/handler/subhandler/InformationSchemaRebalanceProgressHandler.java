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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CloneTableDataFileTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DdlBackfillCostRecordTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaRebalanceProgress;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaRebalanceProgressHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaRebalanceProgressHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaRebalanceProgress;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        return buildRebalanceBackFillView(cursor);
    }

    public static Cursor buildRebalanceBackFillView(ArrayResultCursor cursor) {

        DdlJobManager ddlJobManager = new DdlJobManager();
        List<DdlEngineRecord> ddlRecordList = ddlJobManager.fetchRecords(DdlState.ALL_STATES);
        ddlRecordList = ddlRecordList.stream().filter(e -> !e.isSubJob()).collect(Collectors.toList());
        List<DdlEngineRecord> mayMovePartitionList = ddlRecordList.stream().filter(
            e -> DdlType.valueOf(e.ddlType) == DdlType.ALTER_TABLEGROUP
                || DdlType.valueOf(e.ddlType) == DdlType.ALTER_TABLE).collect(Collectors.toList());

        //filter out REBALANCE JOBs
        List<DdlEngineRecord> rebalanceList = ddlRecordList.stream().filter(
                e -> DdlType.valueOf(e.ddlType) == DdlType.REBALANCE || DdlType.valueOf(e.ddlType) == DdlType.MOVE_DATABASE)
            .collect(Collectors.toList());

        for (DdlEngineRecord ddlRecord : mayMovePartitionList) {
            List<ComplexTaskOutlineRecord> complexTaskOutlineRecords =
                ComplexTaskMetaManager.getMovePartitionTasksBySchJob(ddlRecord.schemaName, ddlRecord.jobId);
            if (GeneralUtil.isNotEmpty(complexTaskOutlineRecords)) {
                rebalanceList.add(ddlRecord);
            }
        }

        if (CollectionUtils.isEmpty(rebalanceList)) {
            return cursor;
        }

        for (DdlEngineRecord record : rebalanceList) {
            Long logicalTableCount = 0L;
            final Long jobId = record.jobId;
            List<DdlEngineTaskRecord> allTasks = ddlJobManager.fetchAllSuccessiveTaskByJobId(jobId);
            List<DdlEngineTaskRecord> allRootTasks =
                allTasks.stream().filter(e -> e.getJobId() == jobId).collect(Collectors.toList());
            for (DdlEngineTaskRecord taskRecord : allRootTasks) {
                if (StringUtils.isEmpty(taskRecord.getCost())) {
                    continue;
                }
                CostEstimableDdlTask.CostInfo costInfo = TaskHelper.decodeCostInfo(taskRecord.getCost());
                logicalTableCount += costInfo.tableCount;
            }

            List<DdlEngineTaskRecord> allImportTableSpaceTasks =
                allTasks.stream().filter(e -> StringUtils.containsIgnoreCase(e.getName(), "importTableSpaceDdlTask"))
                    .collect(Collectors.toList());
            if (GeneralUtil.isEmpty(allImportTableSpaceTasks)) {
                continue;
            }
            addBackfillRow(cursor, record, allTasks, logicalTableCount);
            addImportTableSpaceRow(cursor, record, allTasks, logicalTableCount);
            addDataValidationRow(cursor, record, allTasks, logicalTableCount);
        }

        return cursor;
    }

    private static void addBackfillRow(ArrayResultCursor cursor, DdlEngineRecord ddlRecord,
                                       List<DdlEngineTaskRecord> allTasks, Long logicalTableCount) {
        Long jobId = ddlRecord.jobId;
        PhysicalBackfillManager backfillManager = new PhysicalBackfillManager(SystemDbHelper.DEFAULT_DB_NAME);
        List<DdlEngineTaskRecord> allRootTasks =
            allTasks.stream().filter(e -> e.getJobId() == jobId).collect(Collectors.toList());
        //all submitted BackFill tasks, and yet there may be some BackFill tasks haven't been submitted
        List<DdlEngineTaskRecord> allBackFillTasks =
            allTasks.stream().filter(e -> (StringUtils.containsIgnoreCase(e.getName(), "PhysicalBackfillTask")))
                .collect(Collectors.toList());

        List<DdlEngineTaskRecord> allCloneTableDataFileTasks =
            allTasks.stream().filter(e -> (StringUtils.containsIgnoreCase(e.getName(), "CloneTableDataFileTask")))
                .collect(Collectors.toList());

        if (GeneralUtil.isEmpty(allCloneTableDataFileTasks)) {
            //ignore backfill?
        } else {
            //the physical_backfill_object use cloneTask's taskId as it's jobId
            List<PhysicalBackfillManager.BackFillAggInfo> backFillAggInfoList =
                backfillManager.queryBackFillAggInfoById(
                    allCloneTableDataFileTasks.stream().map(e -> e.taskId).collect(Collectors.toList()));

            long totalSize = 0L;
            for (DdlEngineTaskRecord taskRecord : allRootTasks) {
                if (StringUtils.isEmpty(taskRecord.getCost())) {
                    continue;
                }
                CostEstimableDdlTask.CostInfo costInfo = TaskHelper.decodeCostInfo(taskRecord.getCost());
                totalSize += costInfo.dataSize;
            }
            if (totalSize == 0L) {
                //this is trigger by alter tablegroup move partition or move database command
                //not trigger by rebalance database/cluster
                List<DdlEngineTaskRecord> costTasks = allTasks.stream()
                    .filter(e -> StringUtils.containsIgnoreCase(e.getName(), DdlBackfillCostRecordTask.getTaskName()))
                    .collect(Collectors.toList());
                for (DdlEngineTaskRecord costRecordTask : costTasks) {
                    if (StringUtils.isEmpty(costRecordTask.getCost())) {
                        continue;
                    }
                    CostEstimableDdlTask.CostInfo costInfo = TaskHelper.decodeCostInfo(costRecordTask.getCost());
                    totalSize += costInfo.dataSize;
                }
            }

            Timestamp startTime = null;
            Timestamp endTime = null;
            long successBufferSize = 0l;
            boolean allBackFillFinished = true;
            boolean backFillFailed = false;
            int successCount = 0;
            int failureCount = 0;
            int runningCount = 0;
            int initialCount = 0;
            for (PhysicalBackfillManager.BackFillAggInfo backFillAggInfo : backFillAggInfoList) {
                if (startTime == null) {
                    startTime = backFillAggInfo.getStartTime();
                    endTime = backFillAggInfo.getEndTime();
                } else {
                    if (backFillAggInfo.getStartTime().before(startTime)) {
                        startTime = backFillAggInfo.getStartTime();
                    }
                    if (backFillAggInfo.getEndTime().after(endTime)) {
                        endTime = backFillAggInfo.getEndTime();
                    }
                }
                successBufferSize += backFillAggInfo.getSuccessBufferSize();
                if (backFillAggInfo.getStatus() != PhysicalBackfillManager.BackfillStatus.SUCCESS.getValue()) {
                    allBackFillFinished = false;
                }
                if (backFillAggInfo.getStatus() == PhysicalBackfillManager.BackfillStatus.FAILED.getValue()) {
                    backFillFailed = true;
                }
                if (backFillAggInfo.getStatus() == PhysicalBackfillManager.BackfillStatus.SUCCESS.getValue()) {
                    successCount++;
                }
            }

            logicalTableCount =
                logicalTableCount < allBackFillTasks.size() ? allBackFillTasks.size() : logicalTableCount;

            successCount = 0;
            for (DdlEngineTaskRecord backfillTask : allBackFillTasks) {
                DdlTaskState state = DdlTaskState.valueOf(backfillTask.state);
                switch (state) {
                case READY:
                    initialCount++;
                    break;
                case DIRTY:
                    runningCount++;
                    break;
                case SUCCESS:
                    successCount++;
                    break;
                case ROLLBACK_SUCCESS:
                    failureCount++;
                    break;
                }
            }

            allBackFillFinished =
                !backFillFailed && allBackFillFinished && backFillAggInfoList.size() == logicalTableCount.intValue()
                    && (
                    successCount == logicalTableCount.intValue());
            double avgSpeed = 0;
            String status;
            if (GeneralUtil.isEmpty(backFillAggInfoList)) {
                //not start yet
                status = PhysicalBackfillManager.BackfillStatus.INIT.name();
            } else {
                long duration = Math.max(1, (endTime.getTime() - startTime.getTime()) / 1000);
                avgSpeed = (double) successBufferSize / 1024 / 1024 / duration;
                avgSpeed = Double.valueOf(String.format("%.2f", avgSpeed)).doubleValue();
                if (allBackFillFinished) {
                    status = PhysicalBackfillManager.BackfillStatus.SUCCESS.name();
                } else if (backFillFailed) {
                    status = PhysicalBackfillManager.BackfillStatus.FAILED.name();
                } else if (successBufferSize == 0) {
                    status = PhysicalBackfillManager.BackfillStatus.INIT.name();
                } else {
                    status = PhysicalBackfillManager.BackfillStatus.RUNNING.name();
                }
            }

            initialCount = Math.max(logicalTableCount.intValue() - successCount - failureCount - runningCount, 0);
            String info =
                String.format(
                    "estimate size:%s MB, finish:%s MB, speed:%s MB/s",
                    totalSize / 1024 / 1024, successBufferSize / 1024 / 1024,
                    avgSpeed);
            double progress = Math.min(1.0 * successBufferSize / Math.max(totalSize, 1.00), 1.0) * 100;
            progress = Double.valueOf(String.format("%.2f", progress)).doubleValue();
            addRow(cursor, ddlRecord.jobId, ddlRecord.schemaName, RebalanceStage.DATA_COPY.name(), status, progress,
                logicalTableCount.intValue(), successCount, runningCount, initialCount, failureCount, info, startTime,
                endTime,
                ddlRecord.ddlStmt);
        }
    }

    private static void addImportTableSpaceRow(ArrayResultCursor cursor, DdlEngineRecord ddlRecord,
                                               List<DdlEngineTaskRecord> allTasks, Long logicalTableCount) {
        //all submitted BackFill tasks, and yet there may be some BackFill tasks haven't been submitted
        List<DdlEngineTaskRecord> allImportTasks =
            allTasks.stream().filter(e -> StringUtils.containsIgnoreCase(e.getName(), "importTableSpaceDdlTask"))
                .collect(Collectors.toList());
        int successCount = 0;
        int failureCount = 0;
        int runningCount = 0;
        int initialCount = 0;
        if (GeneralUtil.isEmpty(allImportTasks)) {
            //ignore importtablespace
            assert false;
        } else {
            for (DdlEngineTaskRecord importTask : allImportTasks) {
                DdlTaskState state = DdlTaskState.valueOf(importTask.state);
                switch (state) {
                case READY:
                    initialCount++;
                    break;
                case DIRTY:
                    runningCount++;
                    break;
                case SUCCESS:
                    successCount++;
                    break;
                case ROLLBACK_SUCCESS:
                    failureCount++;
                    break;
                }
            }
        }
        logicalTableCount =
            logicalTableCount < allImportTasks.size() ? allImportTasks.size() : logicalTableCount;
        String status = "";
        if (successCount == logicalTableCount.intValue()) {
            status = "SUCCESS";
        } else if (failureCount > 0) {
            status = "FAILED";
        } else if (runningCount > 0) {
            status = "RUNNING";
        } else {
            if (successCount == 0) {
                status = "INIT";
            } else {
                status = "RUNNING";
            }
        }
        initialCount = Math.max(logicalTableCount.intValue() - successCount - failureCount - runningCount, 0);
        String info =
            String.format("total tasks:%s, finish:%s, running:%s, not start:%s, failed:%s", logicalTableCount,
                successCount, runningCount, initialCount, failureCount);
        double progress = Math.min(1.0 * successCount / Math.max(logicalTableCount, 1), 1.0) * 100;

        progress = Double.valueOf(String.format("%.2f", progress)).doubleValue();
        addRow(cursor, ddlRecord.jobId, ddlRecord.schemaName, RebalanceStage.DATA_IMPORT.name(), status, progress,
            logicalTableCount.intValue(), successCount, runningCount, initialCount, failureCount, info,
            null, null,
            "");
    }

    private static void addDataValidationRow(ArrayResultCursor cursor, DdlEngineRecord ddlRecord,
                                             List<DdlEngineTaskRecord> allTasks, Long logicalTableCount) {
        //all submitted BackFill tasks, and yet there may be some BackFill tasks haven't been submitted
        //AlterTableGroupMovePartitionsCheckTask MoveTableCheckTask
        List<DdlEngineTaskRecord> allDataValidationTasks =
            allTasks.stream().filter(
                    e -> StringUtils.containsIgnoreCase(e.getName(), "AlterTableGroupMovePartitionsCheckTask")
                        || StringUtils.containsIgnoreCase(e.getName(), "MoveTableCheckTask"))
                .collect(Collectors.toList());
        int successCount = 0;
        int failureCount = 0;
        int runningCount = 0;
        int initialCount = 0;
        if (GeneralUtil.isEmpty(allDataValidationTasks)) {
            //ignore checker
        } else {
            for (DdlEngineTaskRecord importTask : allDataValidationTasks) {
                DdlTaskState state = DdlTaskState.valueOf(importTask.state);
                switch (state) {
                case READY:
                    initialCount++;
                    break;
                case DIRTY:
                    runningCount++;
                    break;
                case SUCCESS:
                    successCount++;
                    break;
                case ROLLBACK_SUCCESS:
                    failureCount++;
                    break;
                }
            }
        }
        logicalTableCount =
            logicalTableCount < allDataValidationTasks.size() ? allDataValidationTasks.size() : logicalTableCount;
        String status = "";
        if (successCount == logicalTableCount.intValue()) {
            status = "SUCCESS";
        } else if (failureCount > 0) {
            status = "FAILED";
        } else if (runningCount > 0) {
            status = "RUNNING";
        } else {
            if (successCount == 0) {
                status = "INIT";
            } else {
                status = "RUNNING";
            }
        }
        initialCount = Math.max(logicalTableCount.intValue() - successCount - failureCount - runningCount, 0);
        String info =
            String.format("total tasks:%s, finish:%s, running:%s, not start:%s, failed:%s",
                logicalTableCount,
                successCount, runningCount, initialCount, failureCount);
        double progress = Math.min(1.0 * successCount / Math.max(logicalTableCount, 1), 1.0) * 100;
        progress = Double.valueOf(String.format("%.2f", progress)).doubleValue();
        addRow(cursor, ddlRecord.jobId, ddlRecord.schemaName, RebalanceStage.DATA_VALIDATION.name(), status, progress,
            logicalTableCount.intValue(), successCount, runningCount, initialCount, failureCount,
            info,
            null, null,
            "");
    }

    private static void addRow(ArrayResultCursor cursor, long jobId, String schemaName,
                               String stage,
                               String state,
                               double progress,
                               int totalTableCount,
                               int finishedTableCount,
                               int runningTableCount,
                               int notStartedTableCount,
                               int failedTableCount,
                               String info,
                               Timestamp startTime,
                               Timestamp endTime,
                               String ddlStmt) {
        cursor.addRow(new Object[] {
            jobId, schemaName, stage, state, progress, totalTableCount, finishedTableCount, runningTableCount,
            notStartedTableCount, failedTableCount, info, startTime, endTime, ddlStmt});
    }

    public enum RebalanceStage {
        DATA_COPY(0), DATA_IMPORT(1), DATA_VALIDATION(2);
        private long value;

        RebalanceStage(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        public static RebalanceStage of(long value) {
            switch ((int) value) {
            case 0:
                return DATA_COPY;
            case 1:
                return DATA_IMPORT;
            case 2:
                return DATA_VALIDATION;
            default:
                throw new IllegalArgumentException("Unsupported RebalanceStage value " + value);
            }
        }

        public static String display(long value) {
            switch ((int) value) {
            case 0:
                return DATA_COPY.name();
            case 1:
                return DATA_IMPORT.name();
            case 2:
                return DATA_VALIDATION.name();
            default:
                return "UNKNOWN";
            }
        }
    }
}
