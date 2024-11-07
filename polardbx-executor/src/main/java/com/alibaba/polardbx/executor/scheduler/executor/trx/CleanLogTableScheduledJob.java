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

package com.alibaba.polardbx.executor.scheduler.executor.trx;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.Map;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.CLEAN_LOG_TABLE_V2;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class CleanLogTableScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CleanLogTableScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public CleanLogTableScheduledJob(ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();

        StringBuffer remark = new StringBuffer();
        try {
            // Mark as RUNNING.
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {CLEAN_LOG_TABLE_V2 + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

            final Map savedMdcContext = MDC.getCopyOfContextMap();
            long purge = 0;
            try {
                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                purge = CleanLogTableTask.run(false, remark);
            } finally {
                MDC.setContextMap(savedMdcContext);
            }

            long finishTime = System.currentTimeMillis() / 1000;
            remark.append("Cost time ")
                .append(finishTime - startTime)
                .append("s. ")
                .append("Purged rows ")
                .append(purge)
                .append(".");
            return ScheduledJobsManager
                .casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark.toString());
        } catch (Throwable t) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.TRX,
                    UNEXPECTED,
                    new String[] {
                        CLEAN_LOG_TABLE_V2 + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            remark.append("Clean log table task error: ")
                .append(t.getMessage());
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, remark.toString(), t.getMessage());
            return false;
        }
    }

}
