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

package com.alibaba.polardbx.executor.ddl.newengine;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.lease.impl.LeaseManagerImpl;
import com.alibaba.polardbx.gms.metadb.lease.LeaseRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_LEADER_TTL_IN_MILLIS;

public class DdlEngineRemoteTaskExecutor {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    public static void executeRemoteTask(String schemaName, Long jobId, Long taskId, ExecutionContext executionContext){
        LoggerUtil.buildMDC(schemaName);
        LOGGER.info(String.format("start execute/rollback remote DDL TASK, jobId:%s, taskId:%s", jobId, taskId));

        //try to acquire lease
        Optional<LeaseRecord> leaseRecordOptional = new LeaseManagerImpl().acquire(
            schemaName, String.valueOf(taskId), DDL_LEADER_TTL_IN_MILLIS);
        final DdlEngineDagExecutor dagExecutor;
        if(leaseRecordOptional.isPresent()){
            dagExecutor = DdlEngineDagExecutor.create(jobId, executionContext);
            dagExecutor.getJobLease().set(leaseRecordOptional.get());
        }else {
            final String errMsg = "failed to acquire DDL TASK lease. task_id:" + taskId;
            LOGGER.error(errMsg);
            throw new TddlNestableRuntimeException(errMsg);
        }
        //start lease heartbeat
        final ScheduledExecutorService jobLeaseSchedulerThread = ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("DDL_TASK_LEASE_SCHEDULER"),
            new ThreadPoolExecutor.DiscardPolicy());

        try {
            jobLeaseSchedulerThread.scheduleAtFixedRate(
                AsyncTask.build(()->{
                    Optional<LeaseRecord> optional = new LeaseManagerImpl().extend(String.valueOf(taskId));
                    if(optional.isPresent()){
                        dagExecutor.getJobLease().compareAndSet(dagExecutor.getJobLease().get(), optional.get());
                    }else {
                        //extend job lease failed, so shutdown the scheduler thread
                        jobLeaseSchedulerThread.shutdown();
                    }
                }),
                0L,
                DDL_LEADER_TTL_IN_MILLIS / 2,
                TimeUnit.MILLISECONDS
            );

            //execute task
            dagExecutor.executeSingleTask(taskId);
            LOGGER.info(String.format("execute/rollback remote DDL TASK success, jobId:%s, taskId:%s", jobId, taskId));
        }finally {
            new LeaseManagerImpl().release(String.valueOf(taskId));
            jobLeaseSchedulerThread.shutdown();
        }
    }

}