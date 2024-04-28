package com.alibaba.polardbx.executor.operator.scan.metrics;

/**
 * The method of accumulation in derived counter node.
 */
public enum ProfileAccumulatorType {
    MIN,
    MAX,
    AVG,
    SUM,
    NONE // don't accumulate.
}
