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

package com.alibaba.polardbx.executor.scheduler.executor.spm;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.executor.sync.BaselineLoadSyncAction;
import com.alibaba.polardbx.executor.sync.BaselineQueryAllSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoAccessor;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_SPM;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.NOT_ENABLED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.BASELINE_SYNC;

/**
 * load baseline job
 * Started with SpmScheduleJobLoader
 *
 * @author fangwu
 */
public class SPMBaseLineSyncScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SPMBaseLineSyncScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public SPMBaseLineSyncScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();
        StringBuilder remark = new StringBuilder();
        try {
            //mark as RUNNING
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {BASELINE_SYNC + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

            // check conf
            boolean enableSpm = InstConfUtil.getBool(ENABLE_SPM);
            boolean enableSpmTask = InstConfUtil.getBool(ConnectionParams.ENABLE_SPM_BACKGROUND_TASK);
            if (!enableSpmTask || !enableSpm) {
                remark = new StringBuilder("spm task not enabled:" + enableSpm + "," + enableSpmTask);
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        NOT_ENABLED,
                        new String[] {
                            ConnectionProperties.ENABLE_SPM + " or " + ConnectionProperties.ENABLE_SPM_BACKGROUND_TASK,
                            BASELINE_SYNC + "," + fireTime + " exit"
                        },
                        NORMAL);
                return succeedExit(scheduleId, fireTime, remark.toString());
            }

            // do the job
            logger.info("plan manager async load data");
            // merge&prune baseline from cluster
            Map<String, Map<String, Map<String, BaselineInfo>>> fullBaseline = queryBaselineFromCluster();

            // persist baseline
            try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
                // for each inst
                for (String instId : fullBaseline.keySet()) {
                    Map<String, Map<String, BaselineInfo>> instBaseline = fullBaseline.get(instId);
                    StringBuilder logStr = new StringBuilder(instId + " ");
                    // for each schema
                    for (Map.Entry<String, Map<String, BaselineInfo>> e : instBaseline.entrySet()) {
                        String schema = e.getKey();

                        // for each baseline
                        for (BaselineInfo baselineInfo : e.getValue().values()) {

                            boolean persistPlanStats = false;
                            if (InstConfUtil.isInMaintenanceTimeWindow()) {
                                persistPlanStats = true;
                            }

                            baselineInfoAccessor.persist(schema,
                                baselineInfo.buildBaselineRecord(schema, instId),
                                baselineInfo.buildPlanRecord(schema, instId),
                                persistPlanStats);

                        }
                        logStr.append(schema).append(":").append(e.getValue().size()).append(" ");
                        if (e.getValue().size() != 0) {
                            remark.append(schema).append(":").append(e.getValue().size()).append(";");
                        }
                    }
                    ModuleLogInfo.getInstance()
                        .logRecord(Module.SPM, LogPattern.PROCESS_END,
                            new String[] {"spm merge baseline", logStr.toString()},
                            LogLevel.NORMAL);
                }
            } catch (Exception e) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        UNEXPECTED,
                        new String[] {BASELINE_SYNC + "," + fireTime, e.getMessage()},
                        CRITICAL,
                        e);
            }

            // sync merged baseline to cluster
            SyncManagerHelper.syncWithDefaultDB(new BaselineLoadSyncAction(), SyncScope.ALL);
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SPM,
                    PROCESS_END,
                    new String[] {BASELINE_SYNC + "," + fireTime, remark.toString()},
                    LogLevel.NORMAL);
            return succeedExit(scheduleId, fireTime, remark.toString());
        } catch (Throwable t) {
            remark = new StringBuilder(
                String.format("process load baseline job :[%s] error, fireTime:[%s]", scheduleId, fireTime)
                    + t.getMessage());
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SPM,
                    UNEXPECTED,
                    new String[] {BASELINE_SYNC + "," + fireTime, t.getMessage()},
                    CRITICAL,
                    t);
            errorExit(scheduleId, fireTime, remark.toString(), t.getMessage());
            return false;
        }
    }

    private Map<String, Map<String, Map<String, BaselineInfo>>> queryBaselineFromCluster() {
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncWithDefaultDB(new BaselineQueryAllSyncAction(),
            SyncScope.ALL);

        Map<String, Map<String, Map<String, BaselineInfo>>> instSchemaSqlBaselineMap = Maps.newConcurrentMap();
        // Node
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            Map<String, Object> row = nodeRows.get(0);
            if (!row.containsKey("inst_id")) {
                // some cluster might not update to this version yet
                continue;
            }
            String instId = (String) row.get("inst_id");
            String baselines = (String) row.get("baselines");
            Map<String, Map<String, BaselineInfo>> temp = PlanManager.getBaselineFromJson(baselines);

            if (instSchemaSqlBaselineMap.containsKey(instId)) {
                Map<String, Map<String, BaselineInfo>> current = instSchemaSqlBaselineMap.get(instId);
                mergeBaseline(current, temp);
            } else {
                instSchemaSqlBaselineMap.put(instId, temp);
            }

        }
        return instSchemaSqlBaselineMap;
    }

    /**
     * merge temp baseline info to current
     * temp -> current
     */
    private void mergeBaseline(Map<String, Map<String, BaselineInfo>> current,
                               Map<String, Map<String, BaselineInfo>> temp) {
        for (Map.Entry<String, Map<String, BaselineInfo>> e : temp.entrySet()) {
            String schema = e.getKey().toLowerCase(Locale.ROOT);
            if (!DbInfoManager.getInstance().getDbList().contains(schema)) {
                // remove schema that not being registered
                continue;
            }
            if (!current.containsKey(schema)) {
                current.put(schema, temp.get(schema));
            } else {
                Map<String, BaselineInfo> currentMap = current.get(schema);
                Map<String, BaselineInfo> tempMap = temp.get(schema);
                mergeSubBaseline(schema, currentMap, tempMap);
            }
        }
    }

    private void mergeSubBaseline(String schema,
                                  Map<String, BaselineInfo> currentMap,
                                  Map<String, BaselineInfo> tempMap) {
        for (Map.Entry<String, BaselineInfo> e : tempMap.entrySet()) {
            String sql = e.getKey();
            if (!currentMap.containsKey(sql)) {
                final int maxBaselineSize = InstConfUtil.getInt(ConnectionParams.SPM_MAX_BASELINE_SIZE);
                if (currentMap.size() < maxBaselineSize &&
                    e.getValue().getAcceptedPlans().size() > 0) {
                    currentMap.put(sql, e.getValue());
                }
            } else {
                BaselineInfo c = currentMap.get(sql);
                BaselineInfo t = e.getValue();

                if (c.isRebuildAtLoad()) {
                    // do nothing
                } else if (t.isRebuildAtLoad()) {
                    currentMap.put(sql, t);
                } else {
                    c.merge(schema, t);
                }
            }
        }

        cleanEmptyBaseline(currentMap);
    }

    /**
     * if baseline info has no accepted plan, consider to remove it
     */
    protected static void cleanEmptyBaseline(Map<String, BaselineInfo> currentMap) {
        // clean empty baselines
        Set<String> toRemove = Sets.newHashSet();
        for (Map.Entry<String, BaselineInfo> entry : currentMap.entrySet()) {
            String sqlTmp = entry.getKey();
            BaselineInfo baselineInfo = entry.getValue();

            if (baselineInfo.getAcceptedPlans().isEmpty() &&
                !baselineInfo.isRebuildAtLoad()) {
                toRemove.add(sqlTmp);
            }
        }
        for (String emptySql : toRemove) {
            currentMap.remove(emptySql);
        }
    }

    private boolean succeedExit(long scheduleId, long fireTime, String remark) {
        long finishTime = System.currentTimeMillis() / 1000;
        //mark as SUCCESS
        return ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
    }

    private void errorExit(long scheduleId, long fireTime, String remark, String error) {
        //mark as fail
        ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, remark, error);
    }

    private void syncBaseLineInfoAndPlanInfo() {
    }
}
