package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;

public class OptimizerAlertLoggerStatisticJobInterruptImpl extends OptimizerAlertLoggerBaseImpl {
    public OptimizerAlertLoggerStatisticJobInterruptImpl() {
        super();
        this.optimizerAlertType = OptimizerAlertType.STATISTIC_JOB_INTERRUPT;
    }

    @Override
    public boolean logDetail(ExecutionContext ec) {
        // do nothing
        return false;
    }
}
