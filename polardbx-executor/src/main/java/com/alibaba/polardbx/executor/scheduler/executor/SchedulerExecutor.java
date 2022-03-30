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

        return null;
    }

    /**
     * invoked by SchedulerExecutorRunner
     * @return: whether successfully executed the job
     */
    boolean execute();

}