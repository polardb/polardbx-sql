package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;

public class OptimizerAlertLoggerStatisticInconsistentImpl extends OptimizerAlertLoggerBaseImpl {
    public OptimizerAlertLoggerStatisticInconsistentImpl() {
        super();
        this.optimizerAlertType = OptimizerAlertType.STATISTIC_INCONSISTENT;
    }

    @Override
    public boolean logDetail(ExecutionContext ec) {
        // do nothing
        return false;
    }
}
