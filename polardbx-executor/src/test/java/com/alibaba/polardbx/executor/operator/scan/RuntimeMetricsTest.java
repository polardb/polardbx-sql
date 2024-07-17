package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetricsImpl;
import com.codahale.metrics.Counter;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RuntimeMetricsTest {
    @Test
    public void test() {
        Random random = new Random();
        RuntimeMetrics metrics = buildMetrics(random);

        String reported = metrics.reportAll();
        System.out.println(reported);
    }

    @Test
    public void testMerge() {
        Random random = new Random();

        List<RuntimeMetrics> metricsList = IntStream.range(0, 10)
            .mapToObj(i -> buildMetrics(random))
            .collect(Collectors.toList());

        RuntimeMetrics summary = metricsList.get(0);
        for (int i = 1; i < metricsList.size(); i++) {
            summary.merge(metricsList.get(i));
        }

        String reported = summary.reportAll();
        System.out.println(reported);
    }

    @NotNull
    private RuntimeMetrics buildMetrics(Random random) {
        RuntimeMetrics metrics = new RuntimeMetricsImpl("TEST");

        metrics.addDerivedCounter("EXECUTION_TIMER", null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.MAX);

        metrics.addDerivedCounter("MEMORY", null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        metrics.addDerivedCounter("IO_TIMER", null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);

        metrics.addDerivedCounter("IO_BYTES", null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        Counter c1 = metrics.addCounter("method1.timer", "EXECUTION_TIMER", ProfileUnit.NANO_SECOND);
        Counter c2 = metrics.addCounter("method2.timer", "EXECUTION_TIMER", ProfileUnit.NANO_SECOND);
        Counter c3 = metrics.addCounter("method3.timer", "EXECUTION_TIMER", ProfileUnit.NANO_SECOND);

        Counter c4 = metrics.addCounter("method1.memory", "MEMORY", ProfileUnit.BYTES);
        Counter c5 = metrics.addCounter("method2.memory", "MEMORY", ProfileUnit.BYTES);
        Counter c6 = metrics.addCounter("method3.memory", "MEMORY", ProfileUnit.BYTES);
        metrics.addDerivedCounter("SUB_MEMORY_1", "MEMORY", ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter("SUB_MEMORY_2", "MEMORY", ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        Counter c7 = metrics.addCounter("subMemory1.counter1", "SUB_MEMORY_1", ProfileUnit.BYTES);
        Counter c8 = metrics.addCounter("subMemory1.counter2", "SUB_MEMORY_1", ProfileUnit.BYTES);
        Counter c9 = metrics.addCounter("subMemory1.counter3", "SUB_MEMORY_1", ProfileUnit.BYTES);

        Counter c10 = metrics.addCounter("subMemory2.counter1", "SUB_MEMORY_2", ProfileUnit.BYTES);
        Counter c11 = metrics.addCounter("subMemory2.counter2", "SUB_MEMORY_2", ProfileUnit.BYTES);
        Counter c12 = metrics.addCounter("subMemory2.counter3", "SUB_MEMORY_2", ProfileUnit.BYTES);
        Counter c13 = metrics.addCounter("subMemory2.counter4", "SUB_MEMORY_2", ProfileUnit.BYTES);

        Counter c14 = metrics.addCounter("ioTimer.column1", "IO_TIMER", ProfileUnit.NANO_SECOND);
        Counter c15 = metrics.addCounter("ioBytes.column1", "IO_BYTES", ProfileUnit.BYTES);

        c1.inc(random.nextInt(10000));
        c2.inc(random.nextInt(10000));
        c3.inc(random.nextInt(10000));
        c4.inc(random.nextInt(10000));
        c5.inc(random.nextInt(10000));
        c6.inc(random.nextInt(10000));
        c7.inc(random.nextInt(10000));
        c8.inc(random.nextInt(10000));
        c9.inc(random.nextInt(10000));
        c10.inc(random.nextInt(10000));
        c11.inc(random.nextInt(10000));
        c12.inc(random.nextInt(10000));
        c13.inc(random.nextInt(10000));
        c14.inc(random.nextInt(10000));
        c15.inc(random.nextInt(10000));

        c1.inc(random.nextInt(10000));
        c2.inc(random.nextInt(10000));
        c3.inc(random.nextInt(10000));
        c4.inc(random.nextInt(10000));
        c5.inc(random.nextInt(10000));
        c6.inc(random.nextInt(10000));
        c7.inc(random.nextInt(10000));
        c8.inc(random.nextInt(10000));
        c9.inc(random.nextInt(10000));
        c10.inc(random.nextInt(10000));
        c11.inc(random.nextInt(10000));
        c12.inc(random.nextInt(10000));
        c13.inc(random.nextInt(10000));
        c14.inc(random.nextInt(10000));
        c15.inc(random.nextInt(10000));
        return metrics;
    }
}
