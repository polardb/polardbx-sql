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

import java.time.ZonedDateTime;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.partitionvisualizer.PartitionHeatCollector;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;

import org.apache.commons.lang3.StringUtils;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;

/**
 * data of KeyVisualizer collection
 * doc: https://yuque.antfin-inc.com/coronadb/design/rwrd5x#YnD52
 *
 * @author ximing.yd
 */
public class PartitionVisualizerScheduledJob extends SchedulerExecutor {

    private static final Logger logger = LoggerFactory.getLogger(PartitionVisualizerScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    private final PartitionHeatCollector partitionHeatCollector = new PartitionHeatCollector();

    public PartitionVisualizerScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        try {
            if (isEnablePartitionsHeatmapCollection()) {
                partitionHeatCollector.collectPartitionHeat();
            }

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            boolean casSuccess = ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, QUEUED, SUCCESS,
                finishTime, null);
            if (!casSuccess) {
                return false;
            }
            return true;
        } catch (Throwable t) {
            logger.error(String.format(
                "process scheduled partitions heatmap collect job:[%s] error, fireTime:[%s]", scheduleId, fireTime), t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        }
    }

    private boolean isEnablePartitionsHeatmapCollection() {
        boolean defaultVal = Boolean.parseBoolean(ConnectionParams.ENABLE_PARTITIONS_HEATMAP_COLLECTION.getDefault());
        try {
            String val = MetaDbInstConfigManager.getInstance().getInstProperty(
                ConnectionProperties.ENABLE_PARTITIONS_HEATMAP_COLLECTION);
            if (StringUtils.isEmpty(val)) {
                return defaultVal;
            }
            return Boolean.parseBoolean(val);
        } catch (Exception e) {
            logger.error(String.format("parse param:[ENABLE_PARTITIONS_HEATMAP_COLLECTION=%s] error", defaultVal), e);
            return defaultVal;
        }
    }
}
