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
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.meta.FileStorageBackFillAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.ArchiveOSSTableDataMppTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.ArchiveOSSTableDataWithPauseTask;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.scheduler.ScheduleDateTimeConverter;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.view.InformationSchemaArchive;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InformationSchemaArchiveHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaArchiveHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaArchive;
    }

    /**
     * @param virtualView the origin virtualView to be handled
     * @param executionContext context may be useful for some handler
     * @param cursor empty cursor with types defined
     */
    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        // Get all local_partition schedule jobs map from current schema.
        List<ScheduledJobsRecord> recordList = ScheduledJobsManager.queryScheduledJobsRecord();
        Map<Pair<String, String>, ScheduledJobsRecord> scheduleJobs = recordList.stream()
            .filter(rs -> ScheduledJobExecutorType.LOCAL_PARTITION.name().equalsIgnoreCase(rs.getExecutorType()))
            .filter(rs -> CanAccessTable.verifyPrivileges(rs.getTableSchema(), rs.getTableName(), executionContext))
            .collect(Collectors.toMap(rs -> Pair.of(rs.getTableSchema(), rs.getTableName()), Function.identity()));

        TableInfoManager tableInfoManager = new TableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            FileStorageBackFillAccessor fileStorageBackFillAccessor = new FileStorageBackFillAccessor();
            DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
            DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
            fileStorageBackFillAccessor.setConnection(metaDbConn);
            ddlEngineAccessor.setConnection(metaDbConn);
            ddlEngineTaskAccessor.setConnection(metaDbConn);
            tableInfoManager.setConnection(metaDbConn);
            List<TableLocalPartitionRecord> records = tableInfoManager.getAllLocalPartitionRecord();
            if (records != null) {
                for (TableLocalPartitionRecord localPartitionRecord : records) {
                    String currentSchema = localPartitionRecord.getTableSchema();
                    String currentTable = localPartitionRecord.getTableName();
                    String archiveTable = localPartitionRecord.getArchiveTableName();
                    String archiveSchema = localPartitionRecord.getArchiveTableSchema();

                    if (archiveTable == null || archiveSchema == null) {
                        continue;
                    }

                    // handle expired local partition info.
                    final List<String> expiredPartitionNameList =
                        getExpiredPartitionNameList(executionContext, localPartitionRecord.getTableSchema(),
                            currentTable);

                    ScheduledJobsRecord scheduledJobsRecord = scheduleJobs.get(Pair.of(currentSchema, currentTable));

                    if (scheduledJobsRecord == null) {
                        continue;
                    }
                    List<DdlEngineRecord> ddlEngineRecordList = ddlEngineAccessor.query(currentSchema, currentTable);
                    String lastSuccessArchiveTime = null;
                    String archiveStatus = null;
                    String archiveProgress = null;
                    String archiveJobProgress = null;
                    String archiveCurrentTask = null;
                    String archiveCurrentTaskProgress = null;
                    if (ddlEngineRecordList.isEmpty()) {
                        archiveStatus = "SUCCESS";
                    } else {
                        Optional<DdlEngineRecord> expireRecordOpt = ddlEngineRecordList.stream()
                            .filter(ddlEngineRecord -> ddlEngineRecord.ddlStmt.toLowerCase().contains("expire"))
                            .findFirst();
                        if (expireRecordOpt.isPresent()) {
                            DdlEngineRecord ddlEngineRecord = expireRecordOpt.get();
                            DdlState ddlState = DdlState.tryParse(ddlEngineRecord.state, DdlState.RUNNING);
                            archiveStatus = ddlState.name();

                            List<DdlEngineTaskRecord> taskRecordList =
                                ddlEngineTaskAccessor.query(ddlEngineRecord.jobId);
                            Pair<Integer, Integer> finishAndTotal = calculateTotalProgress(taskRecordList);

                            archiveProgress = (finishAndTotal.getKey() * 100 / finishAndTotal.getValue()) + "%";
                            archiveJobProgress =
                                String.format("(%s/%s)", finishAndTotal.getKey(), finishAndTotal.getValue());
                            Optional<DdlEngineTaskRecord> currentTask = findCurrentTask(taskRecordList);
                            if (currentTask.isPresent()) {
                                archiveCurrentTask = currentTask.get().name;
                                archiveCurrentTaskProgress =
                                    calculateCurrentBackFillProgress(currentTask.get(), fileStorageBackFillAccessor)
                                        + "%";
                            }
                        } else {
                            archiveStatus = "SUCCESS";
                        }
                    }

                    FilesRecord filesRecord =
                        tableInfoManager.queryLatestFileByLogicalSchemaTable(archiveSchema, archiveTable);

                    if (filesRecord != null) {
                        lastSuccessArchiveTime = filesRecord.getCreateTime();
                    }

                    cursor.addRow(new Object[] {
                        // binding info
                        archiveSchema,
                        archiveTable,
                        localPartitionRecord.getTableSchema(),
                        currentTable,

                        // local partition settings
                        localPartitionRecord.intervalCount,
                        localPartitionRecord.intervalUnit,
                        localPartitionRecord.expireAfterCount,
                        localPartitionRecord.preAllocateCount,
                        localPartitionRecord.pivotDateExpr,

                        // schedule info
                        scheduledJobsRecord.getScheduleId(),
                        scheduledJobsRecord.getStatus(),
                        scheduledJobsRecord.getScheduleExpr(),
                        scheduledJobsRecord.getScheduleComment(),
                        scheduledJobsRecord.getTimeZone(),
                        ScheduleDateTimeConverter.secondToZonedDateTime(scheduledJobsRecord.getLastFireTime(),
                            scheduledJobsRecord.getTimeZone()),
                        ScheduleDateTimeConverter.secondToZonedDateTime(scheduledJobsRecord.getNextFireTime(),
                            scheduledJobsRecord.getTimeZone()),

                        // next event
                        buildEventInfo(expiredPartitionNameList),

                        lastSuccessArchiveTime == null ? null : Timestamp.valueOf(lastSuccessArchiveTime),
                        archiveStatus,
                        archiveProgress,
                        archiveJobProgress,
                        archiveCurrentTask,
                        archiveCurrentTaskProgress
                    });
                }
            }

        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                "Schema build meta error.");
        } finally {
            tableInfoManager.setConnection(null);
        }

        return cursor;
    }

    @NotNull
    private List<String> getExpiredPartitionNameList(ExecutionContext executionContext, String currentSchema,
                                                     String currentTable) {
        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(currentSchema).getLatestSchemaManager().getTable(currentTable);
        final LocalPartitionDefinitionInfo definitionInfo =
            primaryTableMeta.getLocalPartitionDefinitionInfo();
        MyRepository myRepository = (MyRepository) ExecutorContext.getContext(currentSchema)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        List<TableDescription> tableDescriptionList =
            LocalPartitionManager.getLocalPartitionInfoList(myRepository,
                currentSchema, currentTable, true);
        TableDescription tableDescription = tableDescriptionList.get(0);
        MysqlDateTime pivotDate = definitionInfo.evalPivotDate(executionContext);
        final List<LocalPartitionDescription> expiredLocalPartitionDescriptionList =
            LocalPartitionManager.getExpiredLocalPartitionDescriptionList(definitionInfo, tableDescription,
                pivotDate);
        final List<String> expiredPartitionNameList =
            expiredLocalPartitionDescriptionList.stream().map(e -> e.getPartitionName())
                .collect(Collectors.toList());
        return expiredPartitionNameList;
    }

    private String buildEventInfo(List<String> expiredPartitionNameList) {
        String expireList = String.join(", ", expiredPartitionNameList);
        return String.format("expire local partition: %s", expireList);
    }

    private Optional<DdlEngineTaskRecord> findCurrentTask(List<DdlEngineTaskRecord> taskRecordList) {
        Optional<DdlEngineTaskRecord> optional = taskRecordList.stream()
            .filter(e -> DdlTaskState.valueOf(e.getState()) == DdlTaskState.DIRTY)
            .findFirst();
        return optional;
    }

    private int calculateCurrentBackFillProgress(DdlEngineTaskRecord currentTask,
                                                 FileStorageBackFillAccessor fileStorageBackFillAccessor) {
        if (currentTask == null) {
            return 0;
        }
        if (!StringUtils.containsIgnoreCase(currentTask.getName(),
            ArchiveOSSTableDataWithPauseTask.class.getSimpleName())
            && !StringUtils.containsIgnoreCase(currentTask.getName(),
            ArchiveOSSTableDataMppTask.class.getSimpleName())) {
            return 0;
        }
        try {
            List<GsiBackfillManager.BackfillObjectRecord> backfillObjectRecordList =
                fileStorageBackFillAccessor.selectBackfillObjectFromFileStorageById(currentTask.taskId);

            if (CollectionUtils.isEmpty(backfillObjectRecordList)) {
                return 0;
            }

            int progress = 0;

            for (GsiBackfillManager.BackfillObjectRecord backfillObjectRecord : backfillObjectRecordList) {
                long lastValue = safeParseLong(backfillObjectRecord.getLastValue());
                long maxValue = safeParseLong(backfillObjectRecord.getMaxValue());

                try {
                    final BigDecimal max = new BigDecimal(maxValue);
                    final BigDecimal last = new BigDecimal(lastValue);

                    progress += last.divide(max, 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100L)).intValue();
                } catch (Throwable e) {

                }
            }

            return progress / backfillObjectRecordList.size();
        } catch (Throwable e) {
            return 0;
        }
    }

    private Pair<Integer, Integer> calculateTotalProgress(List<DdlEngineTaskRecord> taskRecordList) {
        try {
            int totalCount = taskRecordList.size();
            int finishedCount = 0;
            for (DdlEngineTaskRecord record : taskRecordList) {
                if (StringUtils.equalsIgnoreCase(DdlTaskState.SUCCESS.name(), record.getState())) {
                    finishedCount++;
                }
            }
            if (totalCount == 0) {
                return Pair.of(0, 1);
            }
            return Pair.of(finishedCount, totalCount);
        } catch (Throwable e) {
            return Pair.of(0, 1);
        }
    }

    private long safeParseLong(String str) {
        try {
            return Long.valueOf(str);
        } catch (Throwable e) {
            return 0L;
        }
    }
}
