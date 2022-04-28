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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Calendar;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;

/**
 * @author chenzilin
 */
public class PurgeOssFileScheduledJob implements SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(PurgeOssFileScheduledJob.class);
    private final ExecutableScheduledJob executableScheduledJob;

    public PurgeOssFileScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
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

            FileSystemGroup group = FileSystemManager.getFileSystemGroup(Engine.OSS);
            if (group == null) {
                return false;
            }

            //execute
            FailPoint.injectException("FP_PURGE_OSS_FILE_SCHEDULED_JOB_ERROR");
            final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);
            int purgeOssFileBeforeDay = getInstConfigAsInt(ConnectionProperties.PURGE_OSS_FILE_BEFORE_DAY,
                Integer.valueOf(ConnectionParams.PURGE_OSS_FILE_BEFORE_DAY.getDefault()));
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(timeZone.getTimeZone());
            LocalTime nextStartTime = LocalTime.of(2, 0, 0);
            calendar.add(Calendar.DAY_OF_MONTH, -1 * purgeOssFileBeforeDay);
            calendar.set(Calendar.HOUR_OF_DAY, nextStartTime.getHour());
            calendar.set(Calendar.MINUTE, nextStartTime.getMinute());
            calendar.set(Calendar.SECOND, nextStartTime.getSecond());
            LocalDateTime localDateTime = LocalDateTime.ofInstant(calendar.toInstant(), timeZone.getZoneId());
            String sql = String.format("alter fileStorage 'oss' purge before timestamp '%s'",
                String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    localDateTime.getYear(),
                    localDateTime.getMonthValue(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    localDateTime.getMinute(),
                    localDateTime.getSecond()));
            executeBackgroundSql(sql, tableSchema, timeZone);
            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            String remark = "execute: " + sql;
            casSuccess =
                ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
            if (!casSuccess) {
                return false;
            }
            return true;
        } catch (Throwable t) {
            logger.error(String.format(
                "process scheduled local partition job:[%s] error, fireTime:[%s]", scheduleId, fireTime), t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        }
    }

    private void executeBackgroundSql(String sql, String schemaName, InternalTimeZone timeZone) {
        IServerConfigManager serverConfigManager = getServerConfigManager();
        serverConfigManager.executeBackgroundSql(sql, schemaName, timeZone);
    }

    private IServerConfigManager getServerConfigManager() {
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(null);
        }
        return serverConfigManager;
    }

    int getInstConfigAsInt(String key, int defaultVal) {
        String val = MetaDbInstConfigManager.getInstance().getInstProperty(key);
        if (StringUtils.isEmpty(val)) {
            return defaultVal;
        }
        try {
            return Integer.valueOf(val);
        } catch (Exception e) {
            logger.error(String.format("parse param:[%s=%s] error", key, val), e);
            return defaultVal;
        }
    }
}