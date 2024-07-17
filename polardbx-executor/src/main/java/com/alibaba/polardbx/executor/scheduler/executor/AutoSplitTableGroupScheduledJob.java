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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.scheduler.SchedulerJobStatus;
import com.alibaba.polardbx.common.scheduler.SchedulerType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.apache.commons.collections.CollectionUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.cronutils.model.CronType.QUARTZ;

/**
 * @author guxu.ygh
 */
public class AutoSplitTableGroupScheduledJob extends SchedulerExecutor {

    private static final Logger logger = SQLRecorderLogger.ddlEngineLogger;

    private final ExecutableScheduledJob executableScheduledJob;

    public AutoSplitTableGroupScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
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

            logger.info(String.format("start to execute auto split job, scheduleId:[%s], sql:[%s]", scheduleId,
                executorContents));

            //mark as RUNNING
            boolean casRunningSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casRunningSuccess) {
                return false;
            }

            List<Map<String, Object>> executionResult =
                DdlHelper.getServerConfigManager().executeQuerySql(executorContents, tableSchema, timeZone);
            if (CollectionUtils.isEmpty(executionResult)) {
                throw new TddlNestableRuntimeException(
                    String.format("execute auto split job error, scheduleId:[%s], sql:[%s]",
                        scheduleId, executorContents));
            }

            String jobId = String.valueOf(executionResult.get(0).get("JOB_ID"));
            logger.info(
                String.format("submit auto split ddl job success, scheduleId:[%s], job_id:[%s]", scheduleId, jobId));
            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            boolean casSuccess =
                ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime,
                    "job_id:" + jobId);
            if (!casSuccess) {
                return false;
            }
            return true;

        } catch (Throwable t) {
            logger.error(
                String.format("process AutoSplitTableGroupScheduledJob error. jobId:[%s], fireTime:[%s]", scheduleId,
                    fireTime), t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        }
    }

    public static ScheduledJobsRecord createAutoSplitTableGroupJob(
        String tableSchema,
        String tableGroupName,
        ScheduledJobExecutorType executorType,
        String executorContents,
        String scheduleExpr,
        String timeZone,
        SchedulePolicy schedulePolicy) {
        ScheduledJobsRecord record = new ScheduledJobsRecord();
        record.setTableSchema(tableSchema);
        record.setTableGroupName(tableGroupName);
        record.setScheduleName(tableSchema + "." + tableGroupName);
        CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        Cron cron = quartzCronParser.parse(scheduleExpr);
        cron.validate();
        CronDescriptor descriptor = CronDescriptor.instance(Locale.US);
        record.setScheduleComment(descriptor.describe(cron));
        record.setExecutorType(executorType.name());
        record.setExecutorContents(executorContents);
        record.setStatus(SchedulerJobStatus.ENABLED.name());
        record.setScheduleType(SchedulerType.QUARTZ_CRON.name());
        record.setScheduleExpr(scheduleExpr);
        record.setTimeZone(timeZone);
        record.setSchedulePolicy(schedulePolicy.name());
        return record;
    }

}
