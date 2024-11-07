package com.alibaba.polardbx.optimizer.optimizeralert;

public class OptimizerAlertLoggerSelectivityImpl extends OptimizerAlertLoggerStackBaseImpl {
    public OptimizerAlertLoggerSelectivityImpl() {
        this.optimizerAlertType = OptimizerAlertType.SELECTIVITY_ERR;
    }
}
