package com.alibaba.polardbx.executor.operator.scan.metrics;

/**
 * Type of metrics.
 */
public enum ProfileType {
    /**
     * An incrementing and decrementing counter metric.
     */
    COUNTER,

    /**
     * A timer metric which aggregates timing durations and provides duration statistics.
     */
    TIMER,

    /**
     * A meter metric which measures mean throughput and one-, five-, and fifteen-minute
     * moving average throughput.
     */
    METER,

    /**
     * A metric which calculates the distribution of a value.
     */
    HISTOGRAM,

    /**
     * A gauge metric is an instantaneous reading of a particular value.
     */
    GAUGE
}
