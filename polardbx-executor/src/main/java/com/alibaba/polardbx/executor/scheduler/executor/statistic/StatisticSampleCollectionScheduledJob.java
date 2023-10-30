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

package com.alibaba.polardbx.executor.scheduler.executor.statistic;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.collectRowCount;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.sampleTable;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.sketchTable;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.NOT_ENABLED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESSING;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogPattern.REMOVE;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.STATISTIC_SAMPLE_SKETCH;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas;

/**
 * statistic sample job
 *
 * @author fangwu
 */
public class StatisticSampleCollectionScheduledJob extends SchedulerExecutor {

    public static final int DATA_MAX_LEN = 128;

    private final ExecutableScheduledJob executableScheduledJob;

    public StatisticSampleCollectionScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();
        String remark = "";
        try {
            // check conf
            boolean enableStatisticBackground =
                InstConfUtil.getBool(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION);
            if (!enableStatisticBackground) {
                remark = "statistic background collection task not enabled";
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        NOT_ENABLED,
                        new String[] {
                            ConnectionProperties.ENABLE_BACKGROUND_STATISTIC_COLLECTION,
                            STATISTIC_SAMPLE_SKETCH + "," + fireTime + " exit"
                        },
                        NORMAL);
                return succeedExit(scheduleId, fireTime, remark);
            }

            //mark as RUNNING
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {STATISTIC_SAMPLE_SKETCH + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }
            List<String> schemas = DbInfoManager.getInstance().getDbList();
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    PROCESS_START,
                    new String[] {
                        STATISTIC_SAMPLE_SKETCH.name(),
                        "schemas:" + schemas
                    },
                    NORMAL);

            List<Throwable> criticalExceptions = new ArrayList<>();
            for (String schema : schemas) {
                if (SystemDbHelper.isDBBuildIn(schema)) {
                    continue;
                }

                Set<String> logicalTableSet = StatisticManager.getInstance().getTableNamesCollected(schema);
                long start = System.currentTimeMillis();
                List<String> toRemoveList = Lists.newLinkedList();
                for (String logicalTableName : logicalTableSet) {
                    try {
                        // check table if exists
                        if (OptimizerContext.getContext(schema).getLatestSchemaManager()
                            .getTableWithNull(logicalTableName) == null) {
                            if (logicalTableName != null) {
                                toRemoveList.add(logicalTableName);
                            }
                            continue;
                        }

                        // skip oss table sample
                        if (StatisticUtils.isFileStore(schema, logicalTableName)) {
                            continue;
                        }

                        Pair<Boolean, String> pair = needInterrupted();
                        if (pair.getKey()) {
                            ModuleLogInfo.getInstance()
                                .logRecord(
                                    Module.STATISTICS,
                                    INTERRUPTED,
                                    new String[] {
                                        STATISTIC_SAMPLE_SKETCH + "," + fireTime,
                                        pair.getValue()
                                    },
                                    NORMAL);
                            return succeedExit(scheduleId, fireTime, "being interrupted");
                        }
                        long startPerTable = System.currentTimeMillis();
                        StatisticManager.CacheLine c =
                            StatisticManager.getInstance().getCacheLine(schema, logicalTableName);
                        if (c.hasExpire() || testSamplePointCheck()) {
                            // sample
                            collectRowCount(schema, logicalTableName);
                            sampleTable(schema, logicalTableName);
                            // check cache line status to make sure sample process was succ
                            StatisticManager.CacheLine
                                cacheLine = StatisticManager.getInstance().getCacheLine(schema, logicalTableName);
                            if ((cacheLine.getHistogramMap() == null || cacheLine.getHistogramMap().isEmpty())
                                && CollectionUtils.isEmpty(cacheLine.getTopNColumns())) {
                                continue;
                            }
                            // persist
                            StatisticUtils.persistStatistic(schema, logicalTableName, true);
                            // sync other nodes
                            SyncManagerHelper.syncWithDefaultDB(
                                new UpdateStatisticSyncAction(
                                    schema,
                                    logicalTableName,
                                    cacheLine
                                ));
                            OptimizerAlertUtil.statisticsAlert(schema, logicalTableName,
                                StatisticManager.getInstance().getCacheLine(schema, logicalTableName));
                        }

                        long endPerTable = System.currentTimeMillis();
                        ModuleLogInfo.getInstance()
                            .logRecord(
                                Module.STATISTICS,
                                PROCESS_END,
                                new String[] {
                                    "auto analyze " + STATISTIC_SAMPLE_SKETCH + "," + schema + "," + logicalTableName,
                                    " consuming " + (endPerTable - startPerTable) / 1000.0 + " seconds"
                                },
                                NORMAL);
                    } catch (Throwable t) {
                        criticalExceptions.add(new TddlNestableRuntimeException(
                            String.format("%s.%s failed to finish sample job", schema, logicalTableName), t));
                    }
                }
                // remove table statistic info if not exists
                StatisticManager.getInstance().removeLogicalTableList(schema, toRemoveList);

                long end = System.currentTimeMillis();
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        PROCESS_END,
                        new String[] {
                            "auto analyze " + STATISTIC_SAMPLE_SKETCH + "," + schema + ",table size "
                                + logicalTableSet.size(),
                            " consuming " + (end - start) / 1000.0 + " seconds"
                        },
                        NORMAL);
            }
            if (!criticalExceptions.isEmpty()) {
                throw GeneralUtil.mergeException(criticalExceptions);
            }
            return succeedExit(scheduleId, fireTime, remark);
        } catch (Throwable t) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    UNEXPECTED,
                    new String[] {
                        "auto analyze " + STATISTIC_SAMPLE_SKETCH + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t);
            errorExit(scheduleId, fireTime, t.getMessage());
            return false;
        }
    }

    private int testSampleTime = 1;

    private boolean testSamplePointCheck() {
        if (!FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB)) {
            return false;
        }
        testSampleTime++;
        if (testSampleTime >= 10) {
            return false;
        } else {
            return true;
        }

    }

    private int testSketchTime = 1;

    private boolean testSketchPointCheck() {
        if (!FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB)) {
            return false;
        }
        testSketchTime++;
        if (testSketchTime >= 10) {
            return false;
        } else {
            return true;
        }
    }

    private boolean succeedExit(long scheduleId, long fireTime, String remark) {
        long finishTime = System.currentTimeMillis() / 1000;
        //mark as SUCCESS
        return ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
    }

    private void errorExit(long scheduleId, long fireTime, String error) {
        //mark as fail
        ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, error);
    }

    @Override
    public Pair<Boolean, String> needInterrupted() {
        if (FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB)) {
            return Pair.of(false, "fail point");
        }
        boolean enableStatisticBackground =
            InstConfUtil.getBool(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION);
        if (!enableStatisticBackground) {
            return Pair.of(true, "ENABLE_BACKGROUND_STATISTIC_COLLECTION not enabled");
        }
        return Pair.of(!inMaintenanceWindow(), "maintenance window");
    }
}
