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

import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.partitionvisualizer.VisualConstants;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessorDelegate;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;

import java.util.List;

/**
 * @author fangwu
 */
public class ScheduleJobStarter {
    private static final Logger logger = LoggerFactory.getLogger(ScheduleJobStarter.class);

    public static void launchAll() {
        initBaselineSyncJob();
        initPartitionsHeatmapJob();
        initStatisticSampleSketchJob();
    }

    private static void initBaselineSyncJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0 * * * ?";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.BASELINE_SYNC,
            cronExpr,
            timeZone,
            SchedulePolicy.SKIP
        );
        int count = new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.queryByExecutorType(scheduledJobsRecord.getExecutorType());
                if (list.size() > 0) {
                    logger.warn("Scheduled Job For BASELINE_SYNC Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insert(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init BASELINE_SYNC Success %s", count));
    }

    private static void initStatisticSampleSketchJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0 0 */7 * ?";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.STATISTIC_SAMPLE_SKETCH,
            cronExpr,
            timeZone,
            SchedulePolicy.SKIP
        );
        int count = new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.queryByExecutorType(scheduledJobsRecord.getExecutorType());
                if (list.size() > 0) {
                    logger.warn("Scheduled Job For STATISTIC_SAMPLE_SKETCH Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insert(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }

    private static void initPartitionsHeatmapJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.VISUAL_TABLE_NAME;
        String cronExpr = "0/1 * * * * ? ";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.PARTITION_VISUALIZER,
            cronExpr,
            timeZone,
            SchedulePolicy.WAIT
        );
        int count = new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.queryByExecutorType(scheduledJobsRecord.getExecutorType());
                if (list.size() > 0) {
                    logger.warn("Scheduled Job For Partition Visualizer Table Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insert(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init Partitions Heatmap Job Success %s", count));
    }
}
