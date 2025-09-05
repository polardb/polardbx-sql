package com.alibaba.polardbx.executor.scheduler.executor.statistic;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.module.StatisticModuleLogUtil;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.NOT_ENABLED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.STATISTIC_INFO_SCHEMA_TABLES;

public class StatisticInfoSchemaTablesScheduleJob extends StatisticScheduleJob {
    private final ExecutableScheduledJob executableScheduledJob;
    private boolean fromScheduleJob = true;

    public StatisticInfoSchemaTablesScheduleJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public OptimizerAlertType getAlertType() {
        return OptimizerAlertType.STATISTIC_SCHEDULE_JOB_INFORMATION_TABLES_FAIL;
    }

    @Override
    public boolean doExecute() throws Exception{
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();
        String remark = "";
        try {
            boolean interruptedTest = InstConfUtil.getBool(ConnectionParams.ALERT_STATISTIC_INTERRUPT);
            if (interruptedTest) {
                throw new TddlRuntimeException(ErrorCode.ERR_STATISTIC_JOB_INTERRUPTED,
                    "statistic job (info_schema.tables) is interrupted by alert test");
            }

            if (fromScheduleJob) {
                //mark as RUNNING
                boolean casSuccess =
                    ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
                if (!casSuccess) {
                    ModuleLogInfo.getInstance()
                        .logRecord(
                            Module.SCHEDULE_JOB,
                            STATE_CHANGE_FAIL,
                            new String[] {STATISTIC_INFO_SCHEMA_TABLES + "," + fireTime, QUEUED.name(), RUNNING.name()},
                            WARNING);
                    return false;
                }
            }

            boolean enableStatisticBackground =
                InstConfUtil.getBool(ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION);
            if (fromScheduleJob && !enableStatisticBackground) {
                remark = "statistic background collection task (info_schema.tables) disabled.";
                StatisticModuleLogUtil.logNormal(NOT_ENABLED, new String[] {
                        ConnectionProperties.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION, STATISTIC_INFO_SCHEMA_TABLES + "," + fireTime + " exit"});
                return succeedExit(scheduleId, fireTime, remark);
            }
            if (fromScheduleJob && !inMaintenanceWindow()) {
                remark = "statistic background collection task (info_schema.tables) not in maintenance window.";
                StatisticModuleLogUtil.logNormal(NOT_ENABLED, new String[] {
                        ConnectionProperties.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION, STATISTIC_INFO_SCHEMA_TABLES + "," + fireTime + " exit"});
                return succeedExit(scheduleId, fireTime, remark);
            }

            List<String> schemas = DbInfoManager.getInstance().getDbList();
            StatisticModuleLogUtil.logNormal(PROCESS_START, new String[] {STATISTIC_INFO_SCHEMA_TABLES.name(), "schemas:" + schemas});

            for (String schema : schemas) {
                if (StringUtils.isEmpty(schema)) {
                    continue;
                }
                if (SystemDbHelper.isDBBuildInExceptCdc(schema)) {
                    continue;
                }
                if (!OptimizerContext.getActiveSchemaNames().contains(schema)) {
                    continue;
                }

                // interrupted judge
                Pair<Boolean, String> pair = needInterrupted();
                if (pair.getKey()) {
                    StatisticModuleLogUtil.logNormal(INTERRUPTED, new String[] { STATISTIC_INFO_SCHEMA_TABLES + "," + fireTime, pair.getValue()});
                    return succeedExit(scheduleId, fireTime, "being interrupted");
                }

                Set<String> logicalTableSet = Sets.newHashSet();
                for (TableMeta tableMeta : Objects.requireNonNull(OptimizerContext.getContext(schema))
                    .getLatestSchemaManager().getAllUserTables()) {
                    logicalTableSet.add(tableMeta.getTableName().toLowerCase());
                }
                long start = System.currentTimeMillis();
                if (fromScheduleJob) {
                    // Only consider tables whose last updated time > 1 day.
                    try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                        TablesAccessor accessor = new TablesAccessor();
                        accessor.setConnection(metaDbConn);
                        Set<String> longTimeNotUpdatedTables = accessor.getLongTimeNotUpdatedTables(schema);
                        logicalTableSet.retainAll(longTimeNotUpdatedTables);
                    }
                }
                logicalTableSet = logicalTableSet.stream().filter(t ->
                    Objects.requireNonNull(OptimizerContext.getContext(schema))
                        .getLatestSchemaManager().getTableWithNull(t) != null).collect(Collectors.toSet());

                // Process these tables.
                StatisticUtils.updateMetaDbInformationSchemaTables(schema, logicalTableSet);

                long end = System.currentTimeMillis();
                StatisticModuleLogUtil.logNormal(PROCESS_END, new String[] {
                        "auto collect " + STATISTIC_INFO_SCHEMA_TABLES + "," + schema + ",table size " + logicalTableSet.size(),
                        " consuming " + (end - start) / 1000.0 + " seconds"});
            }

            StatisticModuleLogUtil.logNormal(PROCESS_END, new String[] {
                    "auto " + STATISTIC_INFO_SCHEMA_TABLES,
                    " consuming " + (System.currentTimeMillis() - startTime * 1000) / 1000.0 + " seconds"});

            return succeedExit(scheduleId, fireTime, remark);
        } catch (Throwable t) {
            StatisticModuleLogUtil.logCritical(UNEXPECTED, new String[] {"auto analyze " + STATISTIC_INFO_SCHEMA_TABLES + "," + fireTime, t.getMessage()}, t);
            errorExit(scheduleId, fireTime, t.getMessage());
            throw t;
        }
    }

    private boolean succeedExit(long scheduleId, long fireTime, String remark) {
        if (!fromScheduleJob) {
            return true;
        }
        long finishTime = System.currentTimeMillis() / 1000;
        //mark as SUCCESS
        return ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
    }

    private void errorExit(long scheduleId, long fireTime, String error) {
        if (fromScheduleJob) {
            //mark as fail
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, error);
        }

        OptimizerAlertUtil.statisticErrorAlert();
    }

    @Override
    public Pair<Boolean, String> needInterrupted() {
        if (!fromScheduleJob) {
            return Pair.of(false, "not from schedule job");
        }
        if (FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB)) {
            return Pair.of(false, "fail point");
        }
        boolean enableStatisticBackground =
            InstConfUtil.getBool(ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION);
        if (!enableStatisticBackground) {
            return Pair.of(true, "ENABLE_BACKGROUND_STATISTIC_COLLECTION not enabled");
        }
        return Pair.of(!inMaintenanceWindow(), "maintenance window");
    }

    public void setFromScheduleJob(boolean fromScheduleJob) {
        this.fromScheduleJob = fromScheduleJob;
    }
}
