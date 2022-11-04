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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.sampleTable;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.sketchTable;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogPattern.REMOVE;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.STATISTIC_SAMPLE_SKETCH;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;

/**
 * statistic sample and sketch ndv job
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
                    Module.STATISTIC,
                    PROCESS_START,
                    new String[] {
                        STATISTIC_SAMPLE_SKETCH.name(),
                        "schemas:" + schemas
                    },
                    NORMAL);

            for (String schema : schemas) {
                if (SystemDbHelper.isDBBuildIn(schema)) {
                    continue;
                }

                Set<String> logicalTableSet = StatisticManager.getInstance().getTableNamesCollected(schema);
                long start = System.currentTimeMillis();

                for (String logicalTableName : logicalTableSet) {
                    Pair<Boolean, String> pair = needInterrupted();
                    if (pair.getKey()) {
                        ModuleLogInfo.getInstance()
                            .logRecord(
                                Module.STATISTIC,
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
                        ModuleLogInfo.getInstance()
                            .logRecord(
                                Module.STATISTIC,
                                PROCESS_START,
                                new String[] {
                                    "sample table ",
                                    schema + "," + logicalTableName
                                },
                                NORMAL);
                        sampleTable(schema, logicalTableName);
                        // persist
                        StatisticUtils.persistStatistic(schema, logicalTableName, true);
                        // sync other nodes
                        SyncManagerHelper.sync(
                            new UpdateStatisticSyncAction(
                                schema,
                                logicalTableName,
                                StatisticManager.getInstance().getCacheLine(schema, logicalTableName)),
                            schema);
                    }
                    // small table use cache_line
                    if (c.getRowCount() > DEFAULT_SAMPLE_SIZE || testSketchPointCheck()) {
                        // hll
                        ModuleLogInfo.getInstance()
                            .logRecord(
                                Module.STATISTIC,
                                PROCESS_START,
                                new String[] {
                                    "hll scan table ",
                                    schema + "," + logicalTableName
                                },
                                NORMAL);
                        //sketchTable(schema, logicalTableName, false);
                    } else if (c.getRowCount() < DEFAULT_SAMPLE_SIZE &&
                        StatisticManager.getInstance().hasNdvSketch(schema, logicalTableName)) {
                        // remove ndv info if table rowcount less than DEFAULT_SAMPLE_SIZE
                        ModuleLogInfo.getInstance()
                            .logRecord(
                                Module.STATISTIC,
                                REMOVE,
                                new String[] {
                                    STATISTIC_SAMPLE_SKETCH + "," + fireTime,
                                    schema + "," + logicalTableName
                                },
                                NORMAL);
                        StatisticManager.getInstance().removeNdvLogicalTable(schema, logicalTableName);
                    }
                    long endPerTable = System.currentTimeMillis();
                    ModuleLogInfo.getInstance()
                        .logRecord(
                            Module.STATISTIC,
                            PROCESS_END,
                            new String[] {
                                "auto analyze " + STATISTIC_SAMPLE_SKETCH + "," + schema + "," + logicalTableName,
                                " consuming " + (endPerTable - startPerTable) / 1000.0 + " seconds"
                            },
                            NORMAL);
                }
                long end = System.currentTimeMillis();
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTIC,
                        PROCESS_END,
                        new String[] {
                            "auto analyze " + STATISTIC_SAMPLE_SKETCH + "," + schema + ",table size "
                                + logicalTableSet.size(),
                            " consuming " + (end - start) / 1000.0 + " seconds"
                        },
                        NORMAL);
            }
            return succeedExit(scheduleId, fireTime, remark);
        } catch (Throwable t) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTIC,
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
        testSampleTime++;
        if (testSampleTime >= 10) {
            return false;
        }
        return FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB);
    }

    private int testSketchTime = 1;

    private boolean testSketchPointCheck() {
        testSketchTime++;
        if (testSketchTime >= 10) {
            return false;
        }
        return FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB);
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
}
