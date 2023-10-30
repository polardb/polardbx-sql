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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.backfill.ThrottleInfo;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableDataMigrationBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalConvertSequenceTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalTableStructureMigrationTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlBackFillSpeedSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.migration.TableMigrationTaskInfo;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.lang.Math.min;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class InformationSchemaCreateDatabaseHandler extends BaseVirtualViewSubClassHandler {
    static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaCreateDatabaseHandler.class);

    public InformationSchemaCreateDatabaseHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCreateDatabase;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        return buildCreateDatabaseView(cursor);
    }

    public static Cursor buildCreateDatabaseView(ArrayResultCursor cursor) {
        final String sequenceTag = " ";
        DdlEngineSchedulerManager ddlEngineSchedulerManager = new DdlEngineSchedulerManager();
        List<DdlEngineTaskRecord> allCreateDatabaseTask = ddlEngineSchedulerManager.fetchAllTaskByTaskNames(
            ImmutableList.of(
                LogicalTableStructureMigrationTask.class.getSimpleName(),
                LogicalConvertSequenceTask.class.getSimpleName(),
                LogicalTableDataMigrationBackfillTask.class.getSimpleName()
            )
        );

        //Map<jobId, Map<tableName, Pair<drdsSql, autoSql>>>
        Map<Long, Map<String, Pair<String, String>>> createTableSqlSrcAndDst = new TreeMap<>();

        //Map<jobId, Map<TableName, List<TaskRecord>>>
        Map<Long, Map<String, List<DdlEngineTaskRecord>>> createDatabaseTaskByJobId = new TreeMap<>();
        for (DdlEngineTaskRecord record : allCreateDatabaseTask) {
            Long jobId = record.getJobId();
            if (!createDatabaseTaskByJobId.containsKey(jobId)) {
                createDatabaseTaskByJobId.put(jobId, new TreeMap<>());
                createTableSqlSrcAndDst.put(jobId, new TreeMap<>());
            }

            String tableName = null;
            DdlTask task = TaskHelper.fromDdlEngineTaskRecord(record);
            if (record.name.equalsIgnoreCase(LogicalTableStructureMigrationTask.class.getSimpleName())) {
                LogicalTableStructureMigrationTask migrationTask = (LogicalTableStructureMigrationTask) task;
                tableName = migrationTask.getTableName();
                if (!createTableSqlSrcAndDst.get(jobId).containsKey(tableName)) {
                    createTableSqlSrcAndDst.get(jobId).put(tableName,
                        Pair.of(migrationTask.getCreateTableSqlSrc(), migrationTask.getCreateTableSqlDst()));
                }
            } else if (record.name.equalsIgnoreCase(LogicalTableDataMigrationBackfillTask.class.getSimpleName())) {
                tableName = ((LogicalTableDataMigrationBackfillTask) task).getSrcTableName();
            } else if (record.name.equalsIgnoreCase(LogicalConvertSequenceTask.class.getSimpleName())) {
                tableName = sequenceTag;
            }
            if (!createDatabaseTaskByJobId.get(jobId).containsKey(tableName)) {
                createDatabaseTaskByJobId.get(jobId).put(tableName, new ArrayList<>());
            }
            createDatabaseTaskByJobId.get(jobId).get(tableName).add(record);
        }

        //collect backfill info
        //Map<jobId, Map<tableName, BackFillAggInfo>>
        Map<Long, Map<String, GsiBackfillManager.BackFillAggInfo>> backFillAggInfo = new TreeMap<>();
        GsiBackfillManager backfillManager = new GsiBackfillManager(SystemDbHelper.DEFAULT_DB_NAME);
        for (Map.Entry<Long, Map<String, List<DdlEngineTaskRecord>>> entry : createDatabaseTaskByJobId.entrySet()) {
            Long jobId = entry.getKey();
            if (!backFillAggInfo.containsKey(jobId)) {
                backFillAggInfo.put(jobId, new TreeMap<>());
            }
            for (Map.Entry<String, List<DdlEngineTaskRecord>> tableLevelEntry : entry.getValue().entrySet()) {
                String tableName = tableLevelEntry.getKey();
                List<DdlEngineTaskRecord> taskList = tableLevelEntry.getValue();
                List<DdlEngineTaskRecord> backfillRecords =
                    taskList.stream()
                        .filter(record -> record.name.equalsIgnoreCase(
                            LogicalTableDataMigrationBackfillTask.class.getSimpleName()))
                        .collect(Collectors.toList());
                if (!backfillRecords.isEmpty()) {
                    List<GsiBackfillManager.BackFillAggInfo> backFillAggInfoList =
                        backfillManager.queryCreateDatabaseBackFillAggInfoById(
                            backfillRecords.stream().map(e -> e.taskId).collect(
                                Collectors.toList()));
                    if (!backFillAggInfoList.isEmpty()) {
                        backFillAggInfo.get(jobId).put(tableName, backFillAggInfoList.get(0));
                    }
                }
            }
        }

        //throttle info
        Map<Long, ThrottleInfo> throttleInfoMap = collectThrottleInfoMap();

        Map<Long, Map<String, List<DdlEngineTaskRecord>>> resultRecord = new TreeMap<>();
        Map<Long, Set<String>> errorSequenceName = new TreeMap<>();
        for (Map.Entry<Long, Map<String, List<DdlEngineTaskRecord>>> entry : createDatabaseTaskByJobId.entrySet()) {
            Long jobId = entry.getKey();
            resultRecord.put(jobId, new TreeMap<>());
            errorSequenceName.put(jobId, new TreeSet<>(String.CASE_INSENSITIVE_ORDER));

            Map<String, List<DdlEngineTaskRecord>> records = entry.getValue();
            for (Map.Entry<String, List<DdlEngineTaskRecord>> tableRecordEntry : records.entrySet()) {
                String table = tableRecordEntry.getKey();
                resultRecord.get(jobId).put(table, new ArrayList<>());

                List<DdlEngineTaskRecord> newRecords = resultRecord.get(jobId).get(table);
                List<DdlEngineTaskRecord> oldRecords = tableRecordEntry.getValue();

                if (table.equalsIgnoreCase(sequenceTag)) {
                    newRecords.addAll(oldRecords);
                    continue;
                }

                //check if we have running tasks
                for (DdlEngineTaskRecord tableRecord : oldRecords) {
                    if (tableRecord.getState().equalsIgnoreCase("READY") || tableRecord.getExtra() == null) {
                        if (tableRecord.getName()
                            .equalsIgnoreCase(LogicalTableStructureMigrationTask.class.getSimpleName())) {
                            newRecords.clear();
                            newRecords.add(tableRecord);
                        } else {
                            if (newRecords.isEmpty()) {
                                newRecords.add(tableRecord);
                            }
                        }
                    }
                }
                if (!newRecords.isEmpty()) {
                    continue;
                }

                //if all finished, we check if have failed tasks
                for (DdlEngineTaskRecord tableRecord : oldRecords) {
                    if (!tableRecord.getState().equalsIgnoreCase("SUCCESS")) {
                        continue;
                    }
                    TableMigrationTaskInfo taskInfo =
                        JSON.parseObject(tableRecord.getExtra(), TableMigrationTaskInfo.class);
                    if (taskInfo.isSucceed()) {
                        continue;
                    }
                    if (tableRecord.getName()
                        .equalsIgnoreCase(LogicalTableStructureMigrationTask.class.getSimpleName())) {
                        newRecords.clear();
                        newRecords.add(tableRecord);
                        errorSequenceName.get(jobId).add(SequenceAttribute.AUTO_SEQ_PREFIX + table);
                    } else {
                        if (newRecords.isEmpty()) {
                            newRecords.add(tableRecord);
                        }
                    }
                }

                if (!newRecords.isEmpty()) {
                    continue;
                }

                //if newRecords is empty, it means all task succeed, we put a succeed record to log this
                for (DdlEngineTaskRecord tableRecord : oldRecords) {
                    if (tableRecord.getName()
                        .equalsIgnoreCase(LogicalTableDataMigrationBackfillTask.class.getSimpleName())) {
                        newRecords.clear();
                        newRecords.add(tableRecord);
                    } else {
                        if (newRecords.isEmpty()) {
                            newRecords.add(tableRecord);
                        }
                    }
                }
            }

        }
        //build result cursor
        for (Map.Entry<Long, Map<String, List<DdlEngineTaskRecord>>> entry : resultRecord.entrySet()) {
            Long jobId = entry.getKey();
            Map<String, List<DdlEngineTaskRecord>> records = entry.getValue();
            for (Map.Entry<String, List<DdlEngineTaskRecord>> tableRecordEntry : records.entrySet()) {
                String table = tableRecordEntry.getKey();
                List<DdlEngineTaskRecord> recordList = tableRecordEntry.getValue();
                if (recordList.isEmpty()) {
                    continue;
                }
                DdlEngineTaskRecord record = recordList.get(0);

                //handle sequence error
                if (table.equalsIgnoreCase(sequenceTag)) {
                    if (record.getExtra() != null) {
                        TableMigrationTaskInfo taskInfo =
                            JSON.parseObject(record.getExtra(), TableMigrationTaskInfo.class);
                        for (Map.Entry<String, String> sequenceErrEntry : taskInfo.getErrorInfoForSequence()
                            .entrySet()) {
                            if (!errorSequenceName.get(jobId).contains(sequenceErrEntry.getKey())) {
                                addRow(
                                    cursor,
                                    jobId,
                                    taskInfo.getTableSchemaSrc(),
                                    taskInfo.getTableSchemaDst(),
                                    sequenceErrEntry.getKey(),
                                    "CONVERT SEQUENCE",
                                    "FAIL",
                                    sequenceErrEntry.getValue(),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                );
                            }
                        }
                    }
                    continue;
                }

                if (record.getExtra() != null) {
                    TableMigrationTaskInfo taskInfo = JSON.parseObject(record.getExtra(), TableMigrationTaskInfo.class);
                    //succeed
                    if (taskInfo.isSucceed()) {
                        ThrottleInfo throttleInfo = throttleInfoMap.get(record.taskId);
                        String backfillStartTime = null;
                        Object currentSpeed = null;
                        Object avgSpeed = null;
                        Long finishedRows = null;
                        Long totalRow = null;
                        Long progress = null;
                        if (record.getName()
                            .equalsIgnoreCase(LogicalTableDataMigrationBackfillTask.class.getSimpleName())) {
                            DdlTask task = TaskHelper.fromDdlEngineTaskRecord(record);
                            GsiBackfillManager.BackFillAggInfo backfillInfo = backFillAggInfo.get(jobId).get(table);
                            LogicalTableDataMigrationBackfillTask tableDataMigrationBackfillTask =
                                (LogicalTableDataMigrationBackfillTask) task;
                            if (backfillInfo != null) {
                                long duration = backfillInfo.getDuration() == 0 ? 1L : backfillInfo.getDuration();
                                backfillStartTime = backfillInfo.getStartTime();
                                currentSpeed = throttleInfo == null ? "0" : throttleInfo.getSpeed();
                                avgSpeed = backfillInfo.getSuccessRowCount() / duration;
                                finishedRows = backfillInfo.getSuccessRowCount();
                                totalRow = tableDataMigrationBackfillTask.getCostInfo() == null ? 0 :
                                    tableDataMigrationBackfillTask.getCostInfo().rows;
                                Long tempProgress = (totalRow == 0 ? 100 : finishedRows * 100 / totalRow);
                                progress = min(100L, tempProgress);
                            }
                        }
                        addRow(
                            cursor,
                            jobId,
                            taskInfo.getTableSchemaSrc(),
                            taskInfo.getTableSchemaDst(),
                            taskInfo.getTableName(),
                            "ALL",
                            "SUCCESS",
                            null,
                            createTableSqlSrcAndDst.get(jobId).get(taskInfo.getTableName()).getKey(),
                            createTableSqlSrcAndDst.get(jobId).get(taskInfo.getTableName()).getValue(),
                            backfillStartTime,
                            currentSpeed,
                            avgSpeed,
                            finishedRows,
                            totalRow,
                            progress == null ? null : progress + "%"
                        );
                    } else {
                        //failed
                        addRow(
                            cursor,
                            jobId,
                            taskInfo.getTableSchemaSrc(),
                            taskInfo.getTableSchemaDst(),
                            taskInfo.getTableName(),
                            record.getName()
                                .equalsIgnoreCase(LogicalTableStructureMigrationTask.class.getSimpleName()) ?
                                "CREATE TABLE" : "BACKFILL",
                            "FAIL",
                            taskInfo.getErrorInfo(),
                            taskInfo.getCreateSqlSrc(),
                            taskInfo.getCreateSqlDst(),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null
                        );
                    }
                } else {
                    DdlTask task = TaskHelper.fromDdlEngineTaskRecord(record);
                    if (task instanceof LogicalTableStructureMigrationTask) {
                        LogicalTableStructureMigrationTask tableStructureMigrationTask =
                            (LogicalTableStructureMigrationTask) task;
                        addRow(
                            cursor,
                            tableStructureMigrationTask.getJobId(),
                            tableStructureMigrationTask.getTableSchemaNameSrc(),
                            tableStructureMigrationTask.getTableSchemaNameDst(),
                            tableStructureMigrationTask.getTableName(),
                            "CREATE TABLE",
                            "RUNNING",
                            null,
                            createTableSqlSrcAndDst.get(jobId).get(tableStructureMigrationTask.getTableName()).getKey(),
                            createTableSqlSrcAndDst.get(jobId).get(tableStructureMigrationTask.getTableName())
                                .getValue(),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null
                        );
                    } else {
                        GsiBackfillManager.BackFillAggInfo backfillInfo = backFillAggInfo.get(jobId).get(table);
                        ThrottleInfo throttleInfo = throttleInfoMap.get(task.getTaskId());
                        LogicalTableDataMigrationBackfillTask tableDataMigrationBackfillTask =
                            (LogicalTableDataMigrationBackfillTask) task;
                        String backfillStartTime = null;
                        Object currentSpeed = null;
                        Object avgSpeed = null;
                        Long finishedRows = null;
                        Long totalRow = null;
                        Long progress = null;
                        if (backfillInfo != null) {
                            long duration = backfillInfo.getDuration() == 0 ? 1L : backfillInfo.getDuration();
                            backfillStartTime = backfillInfo.getStartTime();
                            currentSpeed = throttleInfo == null ? "0" : throttleInfo.getSpeed();
                            avgSpeed = backfillInfo.getSuccessRowCount() / duration;
                            finishedRows = backfillInfo.getSuccessRowCount();
                            totalRow = tableDataMigrationBackfillTask.getCostInfo() == null ? 0 :
                                tableDataMigrationBackfillTask.getCostInfo().rows;
                            Long tempProgress = (totalRow == 0 ? 100 : finishedRows * 100 / totalRow);
                            progress = min(100L, tempProgress);
                        }

                        addRow(
                            cursor,
                            tableDataMigrationBackfillTask.getJobId(),
                            tableDataMigrationBackfillTask.getSrcSchemaName(),
                            tableDataMigrationBackfillTask.getDstSchemaName(),
                            tableDataMigrationBackfillTask.getSrcTableName(),
                            "BACKFILL",
                            "RUNNING",
                            null,
                            createTableSqlSrcAndDst.get(jobId).get(tableDataMigrationBackfillTask.getSrcTableName())
                                .getKey(),
                            createTableSqlSrcAndDst.get(jobId).get(tableDataMigrationBackfillTask.getDstTableName())
                                .getValue(),
                            backfillStartTime,
                            currentSpeed,
                            avgSpeed,
                            finishedRows,
                            totalRow,
                            progress == null ? null : progress + "%"
                        );
                    }
                }

            }
        }

        return cursor;
    }

    private static void addRow(ArrayResultCursor cursor, long jobId, String sourceSchema,
                               String targetSchema, String tableOrSeq, String stage, String status, String detail,
                               String sqlSrc, String sqlDst, String backfillStartTime, Object currentSpeed,
                               Object averageSpeed, Long finishedRows, Long totalRow, String backfillProgress) {
        cursor.addRow(new Object[] {
            jobId, sourceSchema, targetSchema, tableOrSeq, stage, status, detail, sqlSrc, sqlDst, backfillStartTime,
            currentSpeed, averageSpeed, finishedRows, totalRow, backfillProgress});
    }

    private static Map<Long, ThrottleInfo> collectThrottleInfoMap() {
        Map<Long, ThrottleInfo> throttleInfoMap = new HashMap<>();
        try {
            List<List<Map<String, Object>>> result = SyncManagerHelper.sync(
                new DdlBackFillSpeedSyncAction(), SystemDbHelper.DEFAULT_DB_NAME, SyncScope.MASTER_ONLY);
            for (List<Map<String, Object>> list : GeneralUtil.emptyIfNull(result)) {
                for (Map<String, Object> map : GeneralUtil.emptyIfNull(list)) {
                    throttleInfoMap.put(Long.parseLong(String.valueOf(map.get("BACKFILL_ID"))),
                        new ThrottleInfo(
                            Long.parseLong(String.valueOf(map.get("BACKFILL_ID"))),
                            Double.parseDouble(String.valueOf(map.get("SPEED"))),
                            Long.parseLong(String.valueOf(map.get("TOTAL_ROWS")))
                        ));
                }
            }
        } catch (Exception e) {
            LOGGER.error("collect ThrottleInfo from remote nodes error", e);
        }
        return throttleInfoMap;
    }

}
