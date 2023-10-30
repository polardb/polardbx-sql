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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.ddl.tablegroup.AutoSplitPolicy;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.scheduler.SchedulerType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.AutoSplitTableGroupScheduledJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessorDelegate;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateSchedule;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author guxu
 */
public class LogicalCreateScheduleHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateScheduleHandler.class);

    public LogicalCreateScheduleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlCreateSchedule createSchedule = (SqlCreateSchedule) ((LogicalDal) logicalPlan).getNativeSqlNode();
        String executorType = createSchedule.getExecutorType();
        ScheduledJobExecutorType scheduledJobExecutorType = null;
        try {
            scheduledJobExecutorType = ScheduledJobExecutorType.valueOf(executorType);
        } catch (IllegalArgumentException e) {
            throw new TddlNestableRuntimeException(String.format(
                "unsupported schedule job : %s", executorType
            ));
        }

        switch (scheduledJobExecutorType) {
        case LOCAL_PARTITION:
        case REFRESH_MATERIALIZED_VIEW:
            return new AffectRowCursor(createLocalPartitionScheduledJob(createSchedule, executionContext));
        case AUTO_SPLIT_TABLE_GROUP:
            return new AffectRowCursor(createAutoSplitTableGroupScheduledJob(createSchedule, executionContext));
        default:
            throw new TddlNestableRuntimeException("unsupported schedule type: " + scheduledJobExecutorType);
        }
    }

    public int createLocalPartitionScheduledJob(SqlCreateSchedule createSchedule, ExecutionContext executionContext) {
        final String cronExpr = createSchedule.getCronExpr();
        String timeZone = createSchedule.getTimeZone();
        if (StringUtils.isEmpty(timeZone)) {
            timeZone = executionContext.getTimeZone().getMySqlTimeZoneName();
        }

        String executorType = createSchedule.getExecutorType();
        ScheduledJobExecutorType scheduledJobExecutorType = null;
        try {
            scheduledJobExecutorType = ScheduledJobExecutorType.valueOf(executorType);
        } catch (IllegalArgumentException e) {
            throw new TddlNestableRuntimeException(String.format(
                "unsupported schedule job : %s", executorType
            ));
        }
        final String tableSchema = createSchedule.getSchemaName();
        final String tableName = ((SqlIdentifier) createSchedule.getTableName()).getLastName();
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.DROP, executionContext);

        TableValidator.validateTableExistence(
            tableSchema,
            tableName,
            executionContext
        );
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.DROP, executionContext);

        if (scheduledJobExecutorType.equals(ScheduledJobExecutorType.LOCAL_PARTITION)) {
            final TableMeta primaryTableMeta =
                OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTable(tableName);
            if (primaryTableMeta.getLocalPartitionDefinitionInfo() == null) {
                throw new TddlNestableRuntimeException(String.format(
                    "table %s.%s is not a local partition table", tableSchema, tableName
                ));
            }
        }

        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
                scheduledJobExecutorType,
            cronExpr,
            timeZone,
            SchedulePolicy.WAIT
        );
        if (scheduledJobsRecord == null) {
            return 0;
        }
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.query(scheduledJobsRecord.getTableSchema(),
                        scheduledJobsRecord.getTableName(), scheduledJobsRecord.getExecutorType());
                list = list.stream()
                    .filter(e -> StringUtils.equalsIgnoreCase(e.getExecutorType(),
                        ScheduledJobExecutorType.LOCAL_PARTITION.name()))
                    .collect(Collectors.toList());
                if (list.size() > 0) {
                    throw new TddlNestableRuntimeException(
                        "Duplicate Scheduled Job For " + scheduledJobsRecord.getExecutorType());
                }
                return scheduledJobsAccessor.insert(scheduledJobsRecord);
            }
        }.execute();
    }

    public int createAutoSplitTableGroupScheduledJob(SqlCreateSchedule createSchedule,
                                                     ExecutionContext executionContext) {
        final String cronExpr = createSchedule.getCronExpr();
        String timeZone = createSchedule.getTimeZone();
        if (StringUtils.isEmpty(timeZone)) {
            timeZone = executionContext.getTimeZone().getMySqlTimeZoneName();
        }
        final String tableSchema = createSchedule.getSchemaName();
        final String tableGroupName = ((SqlIdentifier) createSchedule.getTableName()).getLastName();
        final Map<String, String> params = createSchedule.parseParams();
        final long maxSize = params.containsKey("max_size") ?
            Long.valueOf(params.get("max_size")) : BalanceOptions.DEFAULT_MAX_PARTITION_SIZE;

        //表组存在性检查
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(tableSchema).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            throw new TddlNestableRuntimeException(String.format(
                "table group %s.%s is not exist", tableSchema, tableGroupName
            ));
        }

        //权限检查
        List<TablePartRecordInfoContext> allTables = tableGroupConfig.getAllTables();
        if (CollectionUtils.isNotEmpty(allTables)) {
            for (TablePartRecordInfoContext tablePartRecordInfoContext : allTables) {
                final String tableName = tablePartRecordInfoContext.getTableName();
                PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.ALTER, executionContext);
            }
        }

        //修改元数据
        try (Connection connection = MetaDbUtil.getConnection()) {
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            tableGroupAccessor.setConnection(connection);
            tableGroupAccessor.updateAutoSplitPolicyById(tableGroupConfig.getTableGroupRecord().getId(),
                AutoSplitPolicy.AUTO_SPLIT);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
        //sync元数据
        new TableGroupSyncTask(tableSchema, tableGroupName).executeImpl(executionContext);

        //注册定时任务
        final String REBALANCE_TEMPLATE = "rebalance tablegroup %s policy='split_partition' max_size=%s";
        ScheduledJobsRecord scheduledJobsRecord = AutoSplitTableGroupScheduledJob.createAutoSplitTableGroupJob(
            tableSchema,
            tableGroupName,
            ScheduledJobExecutorType.AUTO_SPLIT_TABLE_GROUP,
            String.format(REBALANCE_TEMPLATE, tableGroupName, maxSize),
            cronExpr,
            timeZone,
            SchedulePolicy.SKIP
        );
        if (scheduledJobsRecord == null) {
            return 0;
        }
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.queryByTableGroupName(tableSchema, tableGroupName);
                list = list.stream()
                    .filter(e -> StringUtils.equalsIgnoreCase(e.getExecutorType(),
                        ScheduledJobExecutorType.AUTO_SPLIT_TABLE_GROUP.name()))
                    .collect(Collectors.toList());
                if (list.size() > 0) {
                    throw new TddlNestableRuntimeException(
                        String.format("Duplicate AUTO_SPLIT_TABLE_GROUP Scheduled Job For TableGroup %s",
                            tableGroupName)
                    );
                }
                return scheduledJobsAccessor.insert(scheduledJobsRecord);
            }
        }.execute();

    }

}
