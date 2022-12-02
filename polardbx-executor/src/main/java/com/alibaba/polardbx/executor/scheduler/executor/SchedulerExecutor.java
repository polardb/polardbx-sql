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

import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.scheduler.executor.spm.SPMBaseLineSyncScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticRowCountCollectionScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticSampleCollectionScheduledJob;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.util.Calendar;

public abstract class SchedulerExecutor {

    public static SchedulerExecutor createSchedulerExecutor(ExecutableScheduledJob job) {
        if (job == null || StringUtils.isEmpty(job.getExecutorType())) {
            return null;
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.LOCAL_PARTITION.name())) {
            return new LocalPartitionScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.PURGE_OSS_FILE.name())) {
            return new PurgeOssFileScheduledJob(job);
        }

        if (StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.PARTITION_VISUALIZER.name())) {
            return new PartitionVisualizerScheduledJob(job);
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
        if(StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.AUTO_SPLIT_TABLE_GROUP.name())){
            return new AutoSplitTableGroupScheduledJob(job);
        }
        return null;
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

    public boolean inMaintenanceWindow() {
        // TODO support timezone
        try {
            return InstConfUtil.isInMaintenanceTimeWindow();
        } catch (ParseException e) {
            // ignore
        }
        return true;
    }
}