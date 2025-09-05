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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.OptimizerAlertScheduleSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.optimizeralert.statisticalert.StatisticAlertLoggerBaseImpl;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;

public class OptimizerAlertScheduledJob extends SchedulerExecutor {

    public static final String NO_ALERT = "No alert found since last schedule job";

    public static final String HAS_OPTIMIZER_ALERT = " optimizer alerts found:";

    public static final String HAS_STATISTIC_ALERT = " statistic alerts found:";
    private static final Logger logger = LoggerFactory.getLogger(OptimizerAlertScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public OptimizerAlertScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();
        try {
            //mark as RUNNING
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                return false;
            }
            String remark;
            if (!DynamicConfig.getInstance().optimizerAlert()) {
                remark = "Skipped because OPTIMIZER_ALERT is disabled";
            } else {
                // enable OPTIMIZER_ALERT
                List<List<Map<String, Object>>> results = SyncManagerHelper.syncWithDefaultDB(
                    new OptimizerAlertScheduleSyncAction(), SyncScope.CURRENT_ONLY);

                StringBuilder optimizeAlertSb = new StringBuilder();
                long optimizeAlertCountSum = 0L;
                Set<String> optimizeAlertSets = Sets.newHashSet();
                StringBuilder statisticAlertSb = new StringBuilder();
                long statisticAlertCountSum = 0L;
                Set<String> statisticAlertSets = Sets.newHashSet();

                for (List<Map<String, Object>> nodeRows : results) {
                    if (CollectionUtils.isEmpty(nodeRows)) {
                        continue;
                    }
                    optimizeAlertSb.append(DataTypes.StringType.convertFrom(nodeRows.get(0).get("COMPUTE_NODE"))).append("{");
                    statisticAlertSb.append(DataTypes.StringType.convertFrom(nodeRows.get(0).get("COMPUTE_NODE"))).append("{");
                    for (Map<String, Object> row : nodeRows) {
                        long count = DataTypes.LongType.convertFrom(row.get("COUNT"));
                        String type = DataTypes.StringType.convertFrom(row.get("ALERT_TYPE"));
                        if (StatisticAlertLoggerBaseImpl.isStatisticAlertType(type)){
                            statisticAlertSets.add(type);
                            statisticAlertSb.append(type).append(":").append(count).append(",");
                            statisticAlertCountSum += count;
                        }else{
                            optimizeAlertSets.add(type);
                            optimizeAlertSb.append(type).append(":").append(count).append(",");
                            optimizeAlertCountSum += count;
                        }
                    }
                    optimizeAlertSb.append("},");
                    statisticAlertSb.append("},");
                }

                StringJoiner remarkSj = new StringJoiner(",");
                if (optimizeAlertCountSum > 0){
                    EventLogger.log(EventType.OPTIMIZER_ALERT, optimizeAlertSb.toString());
                    remarkSj.add(optimizeAlertCountSum + HAS_OPTIMIZER_ALERT + String.join(",", optimizeAlertSets));
                }
                if (statisticAlertCountSum > 0){
                    EventLogger.log(EventType.STATISTIC_ALERT, statisticAlertSb.toString());
                    remarkSj.add(statisticAlertCountSum + HAS_STATISTIC_ALERT + String.join(",", statisticAlertSets));
                }
                if (optimizeAlertCountSum + statisticAlertCountSum > 0){
                    remark = remarkSj.toString();
                }else{
                    // no alter found
                    remark = NO_ALERT;
                }
            }
            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();

            casSuccess =
                ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
            if (!casSuccess) {
                return false;
            }
            return true;
        } catch (Throwable t) {
            logger.error(String.format(
                "process scheduled optimizer alert job:[%s] error, fireTime:[%s]", scheduleId, fireTime), t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        }
    }
}