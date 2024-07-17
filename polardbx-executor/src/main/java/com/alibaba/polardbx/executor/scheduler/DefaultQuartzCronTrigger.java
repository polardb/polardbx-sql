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

package com.alibaba.polardbx.executor.scheduler;

import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsRecord;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.common.base.Preconditions;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import static com.alibaba.polardbx.gms.scheduler.ScheduleDateTimeConverter.secondToZonedDateTime;
import static com.cronutils.model.CronType.QUARTZ;

/**
 * generate next fire time based on Quartz Cron Expression
 * Misfire Policy: IGNORE
 */
public class DefaultQuartzCronTrigger implements ScheduledJobsTrigger {

    private final ScheduledJobsRecord record;
    private final ScheduledJobsAccessor scheduledJobsAccessor;
    private final FiredScheduledJobsAccessor firedScheduledJobsScanner;

    DefaultQuartzCronTrigger(ScheduledJobsRecord record,
                             ScheduledJobsAccessor scheduledJobsAccessor,
                             FiredScheduledJobsAccessor firedScheduledJobsScanner) {
        Preconditions.checkNotNull(record);
        Preconditions.checkNotNull(record.getScheduleExpr());
        Preconditions.checkNotNull(record.getTimeZone());
        this.record = record;
        this.scheduledJobsAccessor = scheduledJobsAccessor;
        this.firedScheduledJobsScanner = firedScheduledJobsScanner;
    }

    @Override
    public Optional<ZonedDateTime> getNextFireTime() {
        Cron cron = parseCronExpr(record.getScheduleExpr());
        ExecutionTime executionTime = ExecutionTime.forCron(cron);
        ZoneId zoneId = TimeZoneUtils.zoneIdOf(record.getTimeZone());
        //Misfire Policy: IGNORE
        ZonedDateTime now = ZonedDateTime.now(zoneId);
        ZonedDateTime referenceDateTime = record.getNextFireTime() <= 0L ?
            now : secondToZonedDateTime(record.getNextFireTime(), zoneId);
        if (referenceDateTime.isBefore(now)) {
            referenceDateTime = now;
        }
        Optional<ZonedDateTime> optionalNewNextExecutionTime = executionTime.nextExecution(referenceDateTime);
        return optionalNewNextExecutionTime;
    }

    @Override
    public boolean fire() {
        Optional<ZonedDateTime> optionalNewNextExecutionTime = getNextFireTime();
        long epochSeconds = optionalNewNextExecutionTime.isPresent() ?
            optionalNewNextExecutionTime.get().toEpochSecond() :
            0L;
        scheduledJobsAccessor.fire(epochSeconds, record.getScheduleId());
        if (epochSeconds > 0L) {
            FiredScheduledJobsRecord firedScheduledJobsRecord = new FiredScheduledJobsRecord();
            firedScheduledJobsRecord.setScheduleId(record.getScheduleId());
            firedScheduledJobsRecord.setTableSchema(record.getTableSchema());
            firedScheduledJobsRecord.setTableGroupName(record.getTableGroupName());
            firedScheduledJobsRecord.setTableName(record.getTableName());
            firedScheduledJobsRecord.setFireTime(epochSeconds);
            firedScheduledJobsRecord.setState(FiredScheduledJobState.QUEUED.name());
            firedScheduledJobsScanner.fire(firedScheduledJobsRecord);
            return true;
        }
        return false;
    }

    @Override
    public boolean fireOnceNow() {
        ZoneId zoneId = TimeZoneUtils.zoneIdOf(record.getTimeZone());
        ZonedDateTime now = ZonedDateTime.now(zoneId);
        long epochSeconds = now.toEpochSecond();
        FiredScheduledJobsRecord firedScheduledJobsRecord = new FiredScheduledJobsRecord();
        firedScheduledJobsRecord.setScheduleId(record.getScheduleId());
        firedScheduledJobsRecord.setTableSchema(record.getTableSchema());
        firedScheduledJobsRecord.setTableGroupName(record.getTableGroupName());
        firedScheduledJobsRecord.setTableName(record.getTableName());
        firedScheduledJobsRecord.setFireTime(epochSeconds);
        firedScheduledJobsRecord.setState(FiredScheduledJobState.QUEUED.name());
        firedScheduledJobsScanner.fire(firedScheduledJobsRecord);
        return true;
    }

    public static Cron parseCronExpr(final String cronExprString) {
        CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        Cron cron = quartzCronParser.parse(cronExprString);
        return cron;
    }

}