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

package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowJobsHandler;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_OVERRIDE_NOW;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TTL_PAUSE;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.LOCAL_PARTITION;
import static com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo.before;

/**
 * 1. allocate new local partitions
 * 2. expire old local partitions
 *
 * @author guxu
 */
public class LocalPartitionScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(LocalPartitionScheduledJob.class);

    private static final String ALLOCATE_LOCAL_PARTITION = "ALTER TABLE %s ALLOCATE LOCAL PARTITION";
    private static final String EXPIRE_LOCAL_PARTITION = "ALTER TABLE %s EXPIRE LOCAL PARTITION %s";
    private static final String EXPIRE_LOCAL_PARTITION_PREFIX = "ALTER TABLE %s EXPIRE LOCAL PARTITION";

    private final ExecutableScheduledJob executableScheduledJob;

    public LocalPartitionScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        final String tableSchema = executableScheduledJob.getTableSchema();
        final String timeZoneStr = executableScheduledJob.getTimeZone();
        final String tableName = executableScheduledJob.getTableName();
        final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);
        final long scheduleId = executableScheduledJob.getScheduleId();
        final long fireTime = executableScheduledJob.getFireTime();
        final long startTime = ZonedDateTime.now().toEpochSecond();

        try {
            // mark as RUNNING
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {LOCAL_PARTITION + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

            MyRepository repository = (MyRepository) ExecutorContext.getContext(tableSchema).getTopologyHandler()
                .getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.toString());

            // local partition info before doing this job
            List<LocalPartitionDescription> preLocalPartitionList =
                getLocalPartitionList(repository, tableSchema, tableName);

            Pair<Boolean, String> needInterruption;

            List<Long> pausedDdlJobIdList =
                getCurrentDdlSqlList(tableSchema, tableName, new DdlState[] {DdlState.PAUSED});

            // If there are paused DDLs, continue them first
            for (Long jobId : pausedDdlJobIdList) {
                needInterruption = needInterrupted();
                if (needInterruption.getKey()) {
                    return interruptionExit(scheduleId, fireTime, needInterruption.getValue());
                }

                if (jobId != null) {
                    logger.warn(String.format("restart local partition ddl. table:[%s], jobId;[%d]", tableName, jobId));
                    executeBackgroundSql("continue ddl " + jobId, tableSchema, timeZone);
                }
            }

            List<String> ddlSqlList = getDdlSqlList(repository, tableSchema, tableName, timeZone);
            for (String ddlSql : ddlSqlList) {
                needInterruption = needInterrupted();
                if (needInterruption.getKey()) {
                    return interruptionExit(scheduleId, fireTime, needInterruption.getValue());
                }

                logger.warn(String.format("start local partition ddl. table:[%s], sql:[%s]", tableName, ddlSql));
                executeBackgroundSql(ddlSql, tableSchema, timeZone);
            }

            // local partition info after doing this job
            List<LocalPartitionDescription> postLocalPartitionList =
                getLocalPartitionList(repository, tableSchema, tableName);

            String remark = genRemark(preLocalPartitionList, postLocalPartitionList);

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();

            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    PROCESS_END,
                    new String[] {
                        LOCAL_PARTITION + "," + fireTime,
                        remark + ", consuming " + (finishTime - startTime) + " seconds"
                    },
                    NORMAL
                );

            return succeedExit(scheduleId, fireTime, remark);
        } catch (Throwable t) {
            // The schedule job may be interrupted asynchronously. e.g. Pause DDL by other thread,
            // throwing an exception which should be caught
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    UNEXPECTED,
                    new String[] {
                        LOCAL_PARTITION + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            logger.error(String.format(
                "process scheduled local partition job:[%s] error, fireTime:[%s]", scheduleId, fireTime), t);
            errorExit(scheduleId, fireTime, t.getMessage());
            return false;
        }
    }

    @Override
    public Pair<Boolean, String> needInterrupted() {
        if (FailPoint.isKeyEnable(FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR)) {
            return Pair.of(true, "fail point");
        }

        if (FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB)) {
            return Pair.of(false, "fail point");
        }

        AtomicBoolean isInjectedByFailPoint = new AtomicBoolean(false);
        FailPoint.inject(FP_TTL_PAUSE, (k, v) -> {
            long fireTime = executableScheduledJob.getFireTime();
            long nowTime = ZonedDateTime.now().toEpochSecond();
            // raw estimation, using for test case only
            isInjectedByFailPoint.set(nowTime - fireTime >= Long.parseLong(v));
        });
        if (isInjectedByFailPoint.get()) {
            return Pair.of(true, "fail point");
        }

        return Pair.of(!inMaintenanceWindow(), "Out of maintenance window");
    }

    @Override
    public boolean safeExit() {
        final String tableSchema = executableScheduledJob.getTableSchema();
        final String tableName = executableScheduledJob.getTableName();
        final MyRepository repository = (MyRepository) ExecutorContext.getContext(tableSchema).getTopologyHandler()
            .getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.toString());
        final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(executableScheduledJob.getTimeZone());
        boolean exitSuccess = true;
        for (Long jobId : getCurrentDdlSqlList(tableSchema, tableName,
            new DdlState[] {DdlState.RUNNING, DdlState.QUEUED})) {

            if (jobId != null) {
                logger.info("pause ddl " + jobId);
                try {
                    // pause ddl
                    executeBackgroundSql("pause ddl " + jobId, tableSchema, timeZone);
                } catch (Throwable t) {
                    logger.error(t);
                    exitSuccess = false;
                }
            }
        }
        return exitSuccess;
    }

    private boolean succeedExit(long scheduleId, long fireTime, String remark) {
        long finishTime = ZonedDateTime.now().toEpochSecond();
        return ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
    }

    private boolean interruptionExit(long scheduleId, long fireTime, String remark) {
        if (safeExit()) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    INTERRUPTED,
                    new String[] {
                        LOCAL_PARTITION + "," + scheduleId + "," + fireTime,
                        remark
                    },
                    NORMAL);
            return ScheduledJobsManager.updateState(scheduleId, fireTime, FiredScheduledJobState.INTERRUPTED,
                "interrupted by self-interruption check", "");
        }
        return false;
    }

    public boolean interrupt() {
        return interruptionExit(executableScheduledJob.getScheduleId(), executableScheduledJob.getFireTime(),
            "Interrupted by auto interrupter");
    }

    private void errorExit(long scheduleId, long fireTime, String error) {
        ScheduledJobsManager.casState(scheduleId, fireTime, RUNNING, FAILED, null, error);
    }

    private List<LocalPartitionDescription> getLocalPartitionList(MyRepository repository,
                                                                  String tableSchema,
                                                                  String tableName) {
        List<TableDescription> tableDescriptionList = LocalPartitionManager.getLocalPartitionInfoList(
            repository, tableSchema, tableName, true);
        TableDescription tableDescription = tableDescriptionList.get(0);
        return tableDescription.getPartitions();
    }

    private boolean needAllocateLocalPartition(TableDescription tableDescription,
                                               LocalPartitionDefinitionInfo definitionInfo,
                                               MysqlDateTime pivotDate) {

        MysqlDateTime newestPartitionDate = LocalPartitionManager.getNewestPartitionDate(tableDescription);
        if (newestPartitionDate == null) {
            newestPartitionDate = pivotDate;
        }
        MysqlDateTime nextPartitionDate = MySQLTimeCalculator.addInterval(
            newestPartitionDate, definitionInfo.getIntervalType(), definitionInfo.getPartitionInterval());
        MysqlDateTime endPartitionDate = MySQLTimeCalculator.addInterval(pivotDate, definitionInfo.getIntervalType(),
            definitionInfo.getPreAllocateInterval());
        return !before(endPartitionDate, nextPartitionDate);
    }

    private List<Long> getCurrentDdlSqlList(String tableSchema, String tableName, DdlState[] ddlStates) {
        String allocateSqlTemplate = String.format(ALLOCATE_LOCAL_PARTITION, tableName);
        String expireSqlTemplate = String.format(EXPIRE_LOCAL_PARTITION_PREFIX, tableName);
        List<Long> sqlList = new ArrayList<>();
        for (DdlEngineRecord record : DdlEngineShowJobsHandler.inspectDdlJobs(Pair.of(null, tableSchema),
            new DdlEngineSchedulerManager())) {
            for (DdlState ddlState : ddlStates) {
                if (ddlState.name().equalsIgnoreCase(record.state)) {
                    if (record.ddlStmt.startsWith(allocateSqlTemplate) || record.ddlStmt.startsWith(
                        expireSqlTemplate)) {
                        sqlList.add(record.jobId);
                    }
                    break;
                }
            }
        }
        return sqlList;
    }

    private List<String> getDdlSqlList(MyRepository repository,
                                       String tableSchema,
                                       String tableName,
                                       InternalTimeZone timeZone) {
        List<TableDescription> tableDescriptionList = LocalPartitionManager.getLocalPartitionInfoList(
            repository, tableSchema, tableName, true);
        TableDescription tableDescription = tableDescriptionList.get(0);
        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTable(tableName);
        final LocalPartitionDefinitionInfo definitionInfo = primaryTableMeta.getLocalPartitionDefinitionInfo();

        // Failed to fetch local partition definition info
        if (definitionInfo == null) {
            throw new TddlNestableRuntimeException(
                String.format("table %s.%s is not a local partition table", tableSchema, tableName));
        }

        // Create a temp ec for pivot evaluation
        ExecutionContext ec = new ExecutionContext(tableSchema);
        ec.setTimeZone(timeZone);
        MysqlDateTime pivotDate = definitionInfo.evalPivotDate(ec);

        FailPoint.injectFromHint(FP_OVERRIDE_NOW, ec, (k, v) -> {
            MysqlDateTime parseDatetime = StringTimeParser.parseDatetime(v.getBytes());
            pivotDate.setYear(parseDatetime.getYear());
            pivotDate.setMonth(parseDatetime.getMonth());
            pivotDate.setDay(parseDatetime.getDay());
        });

        List<String> ddlSqlList = new ArrayList<>();

        if (needAllocateLocalPartition(tableDescription, definitionInfo, pivotDate)) {
            ddlSqlList.add(String.format(ALLOCATE_LOCAL_PARTITION, tableName));
        }
        LocalPartitionManager.getExpiredLocalPartitionDescriptionList(
                definitionInfo, tableDescription, pivotDate
            )
            .stream()
            .map(
                e -> String.format(EXPIRE_LOCAL_PARTITION, tableName, e.getPartitionName())
            ).forEach(ddlSqlList::add);
        return ddlSqlList;
    }

    private String genRemark(List<LocalPartitionDescription> pre, List<LocalPartitionDescription> post) {
        String remark = "";
        if (CollectionUtils.isEmpty(pre) || CollectionUtils.isEmpty(post)) {
            return remark;
        }
        try {
            Set<String> preDesc = pre.stream().map(e -> e.getPartitionDescription()).collect(Collectors.toSet());
            Set<String> postDesc = post.stream().map(e -> e.getPartitionDescription()).collect(Collectors.toSet());
            Set<String> allocated = Sets.difference(postDesc, preDesc);
            Set<String> expired = Sets.difference(preDesc, postDesc);
            if (CollectionUtils.isNotEmpty(allocated)) {
                remark += "allocated:" + Joiner.on(",").join(allocated) + ";";
            }
            if (CollectionUtils.isNotEmpty(expired)) {
                remark += "expired:" + Joiner.on(",").join(expired) + ";";
            }
            return remark;
        } catch (Exception e) {
            return remark;
        }
    }
}