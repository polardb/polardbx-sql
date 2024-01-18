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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;

public class OptimizerAlertScheduledJob extends SchedulerExecutor {

    public static final String NO_ALERT = "No optimizer alert found since last schedule job";

    public static final String HAS_ALERT = " optimizer alerts found:";
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
                    new OptimizerAlertScheduleSyncAction());
                StringBuilder sb = new StringBuilder();
                long countSum = 0L;
                Set<String> alertSets = Sets.newHashSet();
                for (List<Map<String, Object>> nodeRows : results) {
                    if (CollectionUtils.isEmpty(nodeRows)) {
                        continue;
                    }
                    sb.append(DataTypes.StringType.convertFrom(nodeRows.get(0).get("COMPUTE_NODE"))).append("{");
                    for (Map<String, Object> row : nodeRows) {
                        long count = DataTypes.LongType.convertFrom(row.get("COUNT"));
                        String type = DataTypes.StringType.convertFrom(row.get("ALERT_TYPE"));
                        alertSets.add(type);
                        sb.append(type)
                            .append(":").append(count).append(",");
                        countSum += count;
                    }
                    sb.append("},");
                }
                if (countSum == 0) {
                    // no alter found
                    remark = NO_ALERT;
                } else {
                    EventLogger.log(EventType.OPTIMIZER_ALERT, sb.toString());
                    remark = countSum + HAS_ALERT + String.join(",", alertSets);
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