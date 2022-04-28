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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;

import java.sql.Connection;
import java.util.List;

public class PurgeOssFileScheduleTask {

    private static volatile PurgeOssFileScheduleTask instance;

    private boolean init = false;

    public static PurgeOssFileScheduleTask getInstance() {
        if (instance == null) {
            synchronized (FileSystemManager.class) {
                if (instance == null) {
                    instance = new PurgeOssFileScheduleTask();
                }
            }
        }
        return instance;
    }

    private PurgeOssFileScheduleTask() {

    }

    public synchronized void init(ParamManager paramManager) {
        if (!init) {
            String cronExpr = paramManager.getString(ConnectionParams.PURGE_OSS_FILE_CRON_EXPR);
            ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
                    DefaultDbSchema.NAME,
                    "purge_oss_file_schedule_task",
                    ScheduledJobExecutorType.PURGE_OSS_FILE,
                    cronExpr,
                    "+08:00",
                    SchedulePolicy.WAIT
            );
            try (Connection metaDbConnection = MetaDbDataSource.getInstance().getConnection()) {

                ScheduledJobsAccessor scheduledJobsAccessor = new ScheduledJobsAccessor();
                scheduledJobsAccessor.setConnection(metaDbConnection);
                List<ScheduledJobsRecord> scheduledJobsRecordList = scheduledJobsAccessor.query(DefaultDbSchema.NAME, "purge_oss_file_schedule_task");
                if (scheduledJobsRecordList.isEmpty()) {
                    TableMetaChanger.replaceScheduledJob(metaDbConnection, scheduledJobsRecord);
                } else if (!scheduledJobsRecordList.get(0).getScheduleExpr().equalsIgnoreCase(cronExpr)) {
                    TableMetaChanger.replaceScheduledJob(metaDbConnection, scheduledJobsRecord);
                }
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
            init = true;
        }
    }
}
