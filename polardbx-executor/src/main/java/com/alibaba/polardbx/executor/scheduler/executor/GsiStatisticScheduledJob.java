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
import com.alibaba.polardbx.executor.sync.GsiStatisticsSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;

import java.time.ZonedDateTime;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class GsiStatisticScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GsiStatisticScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public GsiStatisticScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();

        try {
            final String tableSchema = executableScheduledJob.getTableSchema();
            final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(executableScheduledJob.getTimeZone());
            final String executorContents = executableScheduledJob.getExecutorContents();

            logger.info(String.format("start to execute persist gsi statistics job, scheduleId:[%s]", scheduleId));

            //mark as RUNNING
            boolean casRunningSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casRunningSuccess) {
                return false;
            }

            persistGsiStatistics();

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            boolean casSuccess =
                ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime,
                    "");
            return casSuccess;

        } catch (Throwable t) {
            logger.info(String.format("persist gsi statistics error. jobId[%s], fireTime:[%s]", scheduleId, fireTime),
                t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        }

    }

    protected void persistGsiStatistics() {
        SyncManagerHelper.sync(
            new GsiStatisticsSyncAction(null, null, null, GsiStatisticsSyncAction.WRITE_BACK_ALL_SCHEMA));
    }

}
