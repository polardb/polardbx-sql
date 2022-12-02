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

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowJobsHandler;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TTL_PAUSE;

/**
 * 1. allocate new local partitions
 * 2. expire old local partitions
 *
 * @author guxu
 */
public class LocalPartitionScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(LocalPartitionScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public LocalPartitionScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        String tableSchema = executableScheduledJob.getTableSchema();
        String timeZoneStr = executableScheduledJob.getTimeZone();
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();
        ScheduledThreadPoolExecutor scheduler = null;

        try {
            //mark as RUNNING
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                return false;
            }
            final String tableName = executableScheduledJob.getTableName();
            final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);
            String allocateSql = String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", tableName);
            String expireSql = String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", tableName);

            // prepare the timeout pause task
            scheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r, "Local Partition pause ddl scheduler");
                    thread.setDaemon(true);
                    return thread;
                }
            });
            long pauseTime = getTimeInMaintenanceWindow(tableSchema);
            scheduler.scheduleWithFixedDelay(new pauseAfterTimeout(tableSchema, expireSql, timeZone),
                pauseTime, 24 * 3600 * 1000, TimeUnit.MILLISECONDS);

            // If there is a paused expire ddl, continue if first
            Long jobID = getExpectedDdl(tableSchema, expireSql);
            if (jobID != null) {
                FailPoint.injectException("FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR");

                logger.warn(String.format("restart expire local partition. table:[%s]", tableName));
                executeBackgroundSql("continue ddl " + jobID, tableSchema, timeZone);
            }

            //execute
            FailPoint.injectException("FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR");

            IRepository repository = ExecutorContext.getContext(tableSchema).getTopologyHandler()
                .getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.toString());
            List<LocalPartitionDescription> preLocalPartitionList =
                getLocalPartitionList((MyRepository) repository, tableSchema, tableName);

            getTimeInMaintenanceWindow(tableSchema);
            logger.warn(String.format("start allocating local partition. table:[%s]", tableName));
            executeBackgroundSql(allocateSql, tableSchema, timeZone);

            getTimeInMaintenanceWindow(tableSchema);
            logger.warn(String.format("start expiring local partition. table:[%s]", tableName));
            executeBackgroundSql(expireSql, tableSchema, timeZone);

            List<LocalPartitionDescription> postLocalPartitionList =
                getLocalPartitionList((MyRepository) repository, tableSchema, tableName);

            String remark = genRemark(preLocalPartitionList, postLocalPartitionList);

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            casSuccess =
                ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
            return casSuccess;
        } catch (Throwable t) {
            logger.error(String.format(
                "process scheduled local partition job:[%s] error, fireTime:[%s]", scheduleId, fireTime), t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        } finally {
            // stop the pause ddl
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        }
    }

    /**
     * get the end time, check that the time is not out of maintenance window
     *
     * @param tableSchema the schema of table
     * @return time to pause the schedule job
     */
    long getTimeInMaintenanceWindow(String tableSchema) {
        long pauseTime = getPauseTime(tableSchema);
        Preconditions.checkArgument(pauseTime > 5 * 1000L,
            String.format("Out of maintenance window, current time is %s, while the end time is %s",
                java.time.LocalDateTime.now(),
                OptimizerContext.getContext(tableSchema).getParamManager()
                    .getString(ConnectionParams.BACKGROUND_TTL_EXPIRE_END_TIME)));
        return pauseTime;
    }

    long getPauseTime(String tableSchema) {
        String endTime = OptimizerContext.getContext(tableSchema).getParamManager()
            .getString(ConnectionParams.BACKGROUND_TTL_EXPIRE_END_TIME);
        long endTimeInMs = GeneralUtil.stringToMs(endTime);
        long now = System.currentTimeMillis();
        long todayMs = now - GeneralUtil.startOfToday(now);
        AtomicLong next = new AtomicLong(endTimeInMs - todayMs);
        FailPoint.inject(FP_TTL_PAUSE, (k, v) -> {
            next.set(Long.parseLong(v) * 1000L);
        });
        return next.get();
    }

    class pauseAfterTimeout implements Runnable {
        String schemaName;
        String sql;
        InternalTimeZone timeZone;

        public pauseAfterTimeout(String schemaName, String sql,
                                 InternalTimeZone timeZone) {
            this.schemaName = schemaName;
            this.sql = sql;
            this.timeZone = timeZone;
        }

        @Override
        public void run() {
            logger.info("try to get ddl id");
            Long jobId = getExpectedDdl(schemaName, sql);

            if (jobId != null) {
                logger.info("pause ddl " + jobId);
                // pause ddl
                executeBackgroundSql("pause ddl " + jobId, schemaName, timeZone);
            }
        }
    }

    Long getExpectedDdl(String schemaName, String sql) {
        for (DdlEngineRecord record : DdlEngineShowJobsHandler.inspectDdlJobs(Pair.of(null, schemaName),
            new DdlEngineSchedulerManager())) {
            if (record.ddlStmt.equalsIgnoreCase(sql)) {
                return record.jobId;
            }
        }
        return null;
    }

    private void executeBackgroundSql(String sql, String schemaName, InternalTimeZone timeZone) {
        IServerConfigManager serverConfigManager = getServerConfigManager();
        serverConfigManager.executeBackgroundSql(sql, schemaName, timeZone);
    }

    private IServerConfigManager getServerConfigManager() {
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(null);
        }
        return serverConfigManager;
    }

    private List<LocalPartitionDescription> getLocalPartitionList(MyRepository repository,
                                                                  String tableSchema,
                                                                  String tableName) {
        List<TableDescription> tableDescriptionList = LocalPartitionManager.getLocalPartitionInfoList(
            repository, tableSchema, tableName, true);
        TableDescription tableDescription = tableDescriptionList.get(0);
        List<LocalPartitionDescription> localPartitionDescriptionList = tableDescription.getPartitions();
        return localPartitionDescriptionList;
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