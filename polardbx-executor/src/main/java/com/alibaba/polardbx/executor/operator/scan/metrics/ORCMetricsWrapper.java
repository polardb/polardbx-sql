package com.alibaba.polardbx.executor.operator.scan.metrics;

import com.codahale.metrics.Counter;
import org.apache.orc.customized.ORCProfile;

/**
 * Wrapper of Runtime Metrics and adapt to ORC Profile.
 */
public class ORCMetricsWrapper implements ORCProfile {
    private final String name;
    private final Counter counter;
    private final RuntimeMetrics runtimeMetrics;

    public ORCMetricsWrapper(String name, String parentName, ProfileUnit unit, RuntimeMetrics runtimeMetrics) {
        this.name = name;
        this.runtimeMetrics = runtimeMetrics;
        this.counter = runtimeMetrics.addCounter(name, parentName, unit);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void update(long delta) {
        counter.inc(delta);
    }
}
