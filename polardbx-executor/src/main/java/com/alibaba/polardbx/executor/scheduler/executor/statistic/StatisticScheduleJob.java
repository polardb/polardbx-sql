package com.alibaba.polardbx.executor.scheduler.executor.statistic;

import com.alibaba.polardbx.common.utils.ExceptionUtils;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertManager;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;

public abstract class StatisticScheduleJob extends SchedulerExecutor {
    @Override
    public boolean execute() {
        try{
            boolean success = doExecute();
            if (!success){
                OptimizerAlertManager.getInstance().log(getAlertType(), null, "schedule job failed : " + this.getClass());
            }
            return success;
        }catch (Throwable e){
            OptimizerAlertManager.getInstance().log(getAlertType(), null, ExceptionUtils.exceptionStackTrace(e));
            return false;
        }
    }

    public abstract boolean doExecute() throws Exception;

    public abstract OptimizerAlertType getAlertType();

}
