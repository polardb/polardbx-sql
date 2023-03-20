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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.google.common.collect.Maps;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.collectRowCount;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.collectRowCountAll;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getTopology;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.sumRowCount;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.isFileStore;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getFileStoreStatistic;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.NOT_ENABLED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.STATISTIC_ROWCOUNT_COLLECTION;

/**
 * row count collection, process once per day by default
 *
 * @author fangwu
 */
@Deprecated
public class StatisticRowCountCollectionScheduledJob extends SchedulerExecutor {

    private final ExecutableScheduledJob executableScheduledJob;

    public StatisticRowCountCollectionScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
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
                        new String[] {STATISTIC_ROWCOUNT_COLLECTION + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

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
                            STATISTIC_ROWCOUNT_COLLECTION + "," + fireTime + " exit"
                        },
                        NORMAL);
                return succeedExit(scheduleId, fireTime, remark);
            }

            // do the job
            long start = System.currentTimeMillis();

            Set<String> dnIds =
                StorageHaManager.getInstance().getMasterStorageList().stream().filter(s -> !s.isMetaDb())
                    .map(StorageInstHaContext::getStorageInstId).collect(
                        Collectors.toSet());
            Map<String, Map<String, Long>> rowCountMap = Maps.newHashMap();
            for (String dnId : dnIds) {
                // safe exit point
                Pair<Boolean, String> interruptPair = needInterrupted();
                if (interruptPair.getKey()) {
                    remark = "statistic background collection task interrupted";
                    ModuleLogInfo.getInstance()
                        .logRecord(
                            Module.SCHEDULE_JOB,
                            INTERRUPTED,
                            new String[] {
                                STATISTIC_ROWCOUNT_COLLECTION + "," + fireTime,
                                interruptPair.getValue()
                            },
                            NORMAL);
                    return succeedExit(scheduleId, fireTime, remark);
                }
                try {
                    Map<String, Map<String, Long>> rowRs = collectRowCountAll(dnId, null);
                    if (rowRs != null) {
                        rowCountMap.putAll(rowRs);
                    }
                } catch (Throwable e) {
                    remark = "statistic background collection task error: " + e.getMessage();
                    return errorExit(scheduleId, fireTime, remark, "dn visit error");
                }
            }

            int count = 0;
            for (Map.Entry<String, Map<String, StatisticManager.CacheLine>> entry : StatisticManager.getInstance()
                .getStatisticCache().entrySet()) {
                String schema = entry.getKey();
                if (SystemDbHelper.isDBBuildIn(schema)) {
                    continue;
                }
                Map<String, StatisticManager.CacheLine> tbCacheLine = entry.getValue();
                for (Map.Entry<String, StatisticManager.CacheLine> entry1 : tbCacheLine.entrySet()) {
                    String tbName = entry1.getKey();
                    StatisticManager.CacheLine cl = entry1.getValue();
                    Map<String, Set<String>> topologyMap = getTopology(schema, tbName);
                    if (topologyMap == null) {
                        continue;
                    }
                    long sum = 0;
                    if (isFileStore(schema, tbName)) {
                        try {
                            sum = getFileStoreStatistic(schema, tbName).get("TABLE_ROWS");
                        } catch (Throwable e) {
                            remark = "statistic background collection task error: " + e.getMessage();
                            return errorExit(scheduleId, fireTime, remark, "file storage info access error");
                        }
                    } else {
                        sum = sumRowCount(topologyMap, rowCountMap);
                    }
                    cl.setRowCount(sum);
                    count++;
                }
            }

            long end = System.currentTimeMillis();
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    PROCESS_END,
                    new String[] {
                        STATISTIC_ROWCOUNT_COLLECTION + "," + fireTime,
                        "collectRowCount :" + dnIds.size() + "," + count + " tables statistics consuming "
                            + (end - start) / 1000.0 + " seconds"
                    },
                    NORMAL
                );
            persistRowCountStatistic();
            return succeedExit(scheduleId, fireTime, remark);
        } catch (Throwable t) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    UNEXPECTED,
                    new String[] {
                        STATISTIC_ROWCOUNT_COLLECTION + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            remark = "statistic background collection task error: " + t.getMessage();
            errorExit(scheduleId, fireTime, remark, t.getMessage());
            return false;
        }
    }

    private void persistRowCountStatistic() {
        long start = System.currentTimeMillis();
        ArrayList<SystemTableTableStatistic.Row> rowList = new ArrayList<>();

        for (Map.Entry<String, Map<String, StatisticManager.CacheLine>> entry : StatisticManager.getInstance()
            .getStatisticCache().entrySet()) {
            String schema = entry.getKey();
            Map<String, StatisticManager.CacheLine> tbCacheLine = entry.getValue();
            for (Map.Entry<String, StatisticManager.CacheLine> entry1 : tbCacheLine.entrySet()) {
                String tbName = entry1.getKey();
                StatisticManager.CacheLine cl = entry1.getValue();
                if (cl.getRowCount() > 0) {
                    rowList.add(new SystemTableTableStatistic.Row(schema, tbName.toLowerCase(), cl.getRowCount(),
                        cl.getLastModifyTime()));
                }
            }
        }

        StatisticManager.getInstance().getSds().batchReplace(rowList);
        long end = System.currentTimeMillis();
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                PROCESS_END,
                new String[] {
                    "persist tables statistics",
                    rowList.size() + " tables consuming " + (end - start) / 1000.0 + " seconds"
                },
                NORMAL
            );
    }

    private boolean succeedExit(long scheduleId, long fireTime, String remark) {
        long finishTime = System.currentTimeMillis() / 1000;
        //mark as SUCCESS
        return ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
    }

    private boolean errorExit(long scheduleId, long fireTime, String remark, String error) {
        //mark as fail
        return ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, remark, error);
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
