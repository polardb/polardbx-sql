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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.partitionvisualizer.VisualConstants;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessorDelegate;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

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
        initStatisticHllSketchJob();
        initCleanLogTableJob();
        initPersistGsiStatisticJob();
        initOptimizerAlertJob();
        initStatisticInfoSchemaTablesJob();
        initLogMetricJob();
        initGenerateColumnarSnapshotJob();
        initCheckCciJob();
    }

    private static void initLogMetricJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0 0 * * ?";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.LOG_SYSTEM_METRICS,
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
                    logger.warn("Scheduled Job For LOG_SYSTEM_METRICS Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
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
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init BASELINE_SYNC Success %s", count));
    }

    private static void initStatisticHllSketchJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0 0 */7 * ?";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.STATISTIC_HLL_SKETCH,
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
                    logger.warn("Scheduled Job For STATISTIC_HLL_SKETCH Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }

    protected static void initStatisticSampleSketchJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0 0 * * ?";
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
                    // check scheduleExpr
                    ScheduledJobsRecord tmp =  list.get(0);
                    String scheduleExpr = tmp.getScheduleExpr();
                    if (!scheduleExpr.equals(scheduledJobsRecord.getScheduleExpr())) {
                        scheduledJobsRecord.setScheduleId(tmp.getScheduleId());
                        return scheduledJobsAccessor.updateScheduledJobsRecordByScheduleId(scheduledJobsRecord);
                    }
                    logger.warn("Scheduled Job For STATISTIC_SAMPLE_SKETCH Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }

    private static void initPersistGsiStatisticJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0 1 * * ?";
        String timeZone = "+08:00";

        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.PERSIST_GSI_STATISTICS,
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
                    logger.warn("Scheduled Job For PERSIST_GSI_STATISTICS Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();

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
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init Partitions Heatmap Job Success %s", count));
    }

    private static void initOptimizerAlertJob() {
        String tableSchema = SystemDbHelper.DEFAULT_DB_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "20 20 10 * * ? ";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.OPTIMIZER_ALERT,
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
                    logger.warn("Scheduled Job For OPTIMIZER_ALERT Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }

    private static void initCleanLogTableJob() {
        // Delete old clean job.
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 45 * * * ? ";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.CLEAN_LOG_TABLE,
            cronExpr,
            timeZone,
            SchedulePolicy.SKIP
        );
        int count = new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.queryByExecutorType(scheduledJobsRecord.getExecutorType());
                if (list.isEmpty()) {
                    logger.warn("Scheduled Job For CLEAN_LOG_TABLE not exists. Ignore deleting.");
                    return 0;
                }
                return scheduledJobsAccessor.deleteById(list.get(0).getScheduleId());
            }
        }.execute();
        logger.info(String.format("Delete %s Success %s", scheduledJobsRecord.getExecutorType(), count));
        if (count > 1) {
            EventLogger.log(EventType.TRX_LOG_ERR, "Delete old CLEAN_LOG_TABLE.");
        }

        // Init new clean job.
        cronExpr = "0 0/1 * * * ? ";
        timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecordV2 = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.CLEAN_LOG_TABLE_V2,
            cronExpr,
            timeZone,
            SchedulePolicy.SKIP
        );
        count = new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.queryByExecutorType(scheduledJobsRecordV2.getExecutorType());
                if (list.size() > 0) {
                    logger.warn("Scheduled Job For CLEAN_LOG_TABLE_V2 Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecordV2);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }

    private static void initCheckCciJob() {
        // Init new clean job.
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0/1 * * * ? ";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.CHECK_CCI,
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
                    logger.warn("Scheduled Job For CHECK_CCI Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }

    private static void initGenerateColumnarSnapshotJob() {
        // Init new clean job.
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 0/1 * * * ? ";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.GENERATE_COLUMNAR_SNAPSHOT,
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
                    logger.warn("Scheduled Job For GENERATE_COLUMNAR_SNAPSHOT Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }

    private static void initStatisticInfoSchemaTablesJob() {
        String tableSchema = VisualConstants.VISUAL_SCHEMA_NAME;
        String tableName = VisualConstants.DUAL_TABLE_NAME;
        String cronExpr = "0 15 * * * ?";
        String timeZone = "+08:00";
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            null,
            tableName,
            ScheduledJobExecutorType.STATISTIC_INFO_SCHEMA_TABLES,
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
                    logger.warn("Scheduled Job For STATISTIC_INFO_SCHEMA_TABLES Has Exist");
                    return 0;
                }
                return scheduledJobsAccessor.insertIgnoreFail(scheduledJobsRecord);
            }
        }.execute();
        logger.info(String.format("Init %s Success %s", scheduledJobsRecord.getExecutorType(), count));
    }
}
