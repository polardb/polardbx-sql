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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaScheduleJobs;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * @author fangwu
 */
public class InformationSchemaScheduleJobsHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaScheduleJobsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaScheduleJobs;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<ScheduledJobsRecord> jobs = ScheduledJobsManager.queryScheduledJobsRecord();
        for (ScheduledJobsRecord scheduledJob : jobs) {
            ScheduledJobExecutorType executorType =
                ScheduledJobExecutorType.valueOf(scheduledJob.getExecutorType());
            List<ExecutableScheduledJob> eJobList =
                ScheduledJobsManager.getScheduledJobResult(scheduledJob.getScheduleId());
            ExecutableScheduledJob lastEJob = findLastJob(eJobList);

            String lastFireTime =
                TimeZoneUtils.convertToDateTimeWithMills(new Timestamp(scheduledJob.getLastFireTime() * 1000),
                    TimeZone.getTimeZone(scheduledJob.getTimeZone()));

            String nextFireTime =
                TimeZoneUtils.convertToDateTimeWithMills(new Timestamp(scheduledJob.getNextFireTime() * 1000),
                    TimeZone.getTimeZone(scheduledJob.getTimeZone()));

            String lastJobStartTime = null;
            if (lastEJob != null) {
                lastJobStartTime =
                    TimeZoneUtils.convertToDateTimeWithMills(new Timestamp(lastEJob.getStartTime() * 1000),
                        TimeZone.getTimeZone(scheduledJob.getTimeZone()));
            }

            cursor.addRow(new Object[] {
                executorType.module().name(),
                executorType.name(),
                lastFireTime,
                nextFireTime,
                lastJobStartTime,
                lastEJob == null ? null : lastEJob.getState(),
                lastEJob == null ? null : lastEJob.getResult(),
                lastEJob == null ? null : lastEJob.getRemark(),
                scheduledJob.getStatus(),
                scheduledJob.getScheduleExpr(),
                scheduledJob.getScheduleComment()
            });
        }
        return cursor;
    }

    private ExecutableScheduledJob findLastJob(List<ExecutableScheduledJob> eJobList) {
        if (eJobList == null || eJobList.size() == 0) {
            return null;
        }
        return eJobList.stream().max(Comparator.comparing(ExecutableScheduledJob::getStartTime)).get();
    }
}
