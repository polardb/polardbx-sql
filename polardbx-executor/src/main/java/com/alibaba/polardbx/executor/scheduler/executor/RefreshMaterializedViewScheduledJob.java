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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;

import java.time.ZonedDateTime;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;

public class RefreshMaterializedViewScheduledJob extends SchedulerExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RefreshMaterializedViewScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public RefreshMaterializedViewScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        String tableSchema = executableScheduledJob.getTableSchema();
        String timeZoneStr = executableScheduledJob.getTimeZone();
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
            //execute
            FailPoint.injectException("FP_REFRESH_MATERIALIZED_VIEW_JOB_ERROR");

            final String tableName = executableScheduledJob.getTableName();
            final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);

            logger.info(String.format("start refresh materialized view. table:[%s]", tableName));
            executeBackgroundSql(String.format("REFRESH MATERIALIZED VIEW %s ", tableName), tableSchema, timeZone);

            String remark = "";

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            casSuccess =
                ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
            return casSuccess;
        } catch (Throwable t) {
            logger.error(String.format(
                "process scheduled refresh materialized view job:[%s] error, fireTime:[%s]", scheduleId, fireTime), t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        }

    }
}
