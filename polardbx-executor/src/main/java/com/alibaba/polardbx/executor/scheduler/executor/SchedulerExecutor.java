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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.scheduler.executor.spm.SPMBaseLineSyncScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticHllScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticInfoSchemaTablesScheduleJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticRowCountCollectionScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticSampleCollectionScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.trx.GenerateColumnarSnapshotScheduledJob;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Random;

public abstract class SchedulerExecutor {

    public static SchedulerExecutor createSchedulerExecutor(ExecutableScheduledJob job) {
        if (job == null || StringUtils.isEmpty(job.getExecutorType())) {
            return null;
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.LOCAL_PARTITION.name())) {
            return new LocalPartitionScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.TTL_JOB.name())) {
            return new TtlArchivedDataScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.PURGE_OSS_FILE.name())) {
            return new PurgeOssFileScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.PURGE_OSS_FILE.name())) {
            return new PurgeOssFileScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.PARTITION_VISUALIZER.name())) {
            return new PartitionVisualizerScheduledJob(job);
        }
        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.OPTIMIZER_ALERT.name())) {
            return new OptimizerAlertScheduledJob(job);
        }
        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.REFRESH_MATERIALIZED_VIEW.name())) {
            return new RefreshMaterializedViewScheduledJob(job);
        }
        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.AUTO_SPLIT_TABLE_GROUP.name())) {
            return new AutoSplitTableGroupScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.BASELINE_SYNC.name())) {
            return new SPMBaseLineSyncScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.STATISTIC_ROWCOUNT_COLLECTION.name())) {
            return new StatisticRowCountCollectionScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.STATISTIC_SAMPLE_SKETCH.name())) {
            return new StatisticSampleCollectionScheduledJob(job);
        }
        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.STATISTIC_HLL_SKETCH.name())) {
            return new StatisticHllScheduledJob(job);
        }
        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.PERSIST_GSI_STATISTICS.name())) {
            return new GsiStatisticScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.CLEAN_LOG_TABLE_V2.name())) {
            return new CleanLogTableScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.CHECK_CCI.name())) {
            return new CheckCciScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.GENERATE_COLUMNAR_SNAPSHOT.name())) {
            return new GenerateColumnarSnapshotScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.STATISTIC_INFO_SCHEMA_TABLES.name())) {
            return new StatisticInfoSchemaTablesScheduleJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(),
            ScheduledJobExecutorType.LOG_SYSTEM_METRICS.name())) {
            return new LogSystemMetricsScheduledJob(job);
        }
        return null;
    }

    public void executeBackgroundSql(String sql, String schemaName, InternalTimeZone timeZone) {
        IServerConfigManager serverConfigManager = getServerConfigManager();
        serverConfigManager.executeBackgroundSql(sql, schemaName, timeZone);
    }

    public List<Map<String, Object>> executeInternalQuery(String sql, String schemaName, InternalTimeZone timeZone) {
        IServerConfigManager serverConfigManager = getServerConfigManager();
        return serverConfigManager.executeQuerySql(sql, schemaName, timeZone);
    }

    public IServerConfigManager getServerConfigManager() {
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(null);
        }
        return serverConfigManager;
    }

    /**
     * invoked by SchedulerExecutorRunner
     */
    public abstract boolean execute();

    public Pair<Boolean, String> needInterrupted() {
        return Pair.of(false, "default");
    }

    public boolean safeExit() {
        // do nothing default
        return true;
    }

    /**
     * invoked by ScheduledJobsAutoInterrupter
     *
     * @return if interruption succeeds
     */
    public boolean interrupt() {
        return false;
    }

    public boolean inMaintenanceWindow() {
        // TODO support timezone
        return InstConfUtil.isInMaintenanceTimeWindow();
    }
}