package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;

public class RebalanceScheduledJob implements SchedulerExecutor{

    private static final Logger logger = LoggerFactory.getLogger(LocalPartitionScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public RebalanceScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {

        //1. fetch rebalance schedule table
        String ddlPlanId = executableScheduledJob.getExecutorContents();


        return false;
    }

    private void executeDdlPlan(long ddlPlanId){

    }


}