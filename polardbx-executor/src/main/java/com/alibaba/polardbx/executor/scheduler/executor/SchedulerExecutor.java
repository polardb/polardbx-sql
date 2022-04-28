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

import com.alibaba.polardbx.common.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import org.apache.commons.lang3.StringUtils;

public interface SchedulerExecutor {

    static SchedulerExecutor createSchedulerExecutor(ExecutableScheduledJob job){
        if(job == null || StringUtils.isEmpty(job.getExecutorType())){
            return null;
        }
        if(StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.LOCAL_PARTITION.name())){
            return new LocalPartitionScheduledJob(job);
        }
        if(StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.REBALANCE.name())){
            return new RebalanceScheduledJob(job);
        }

        if(StringUtils.equalsIgnoreCase(job.getExecutorType(), ScheduledJobExecutorType.PURGE_OSS_FILE.name())){
            return new PurgeOssFileScheduledJob(job);
        }

        return null;
    }

    /**
     * invoked by SchedulerExecutorRunner
     * @return: whether successfully executed the job
     */
    boolean execute();

}