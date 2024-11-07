package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.selectivity.TableScanSelectivityEstimator;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OptimizerAlertLoggerSelectivityImplTest {
    @Test
    public void testEstimator() {
        long interval = 1000L;
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.OPTIMIZER_ALERT_LOG_INTERVAL, String.valueOf(interval));
        OptimizerAlertManager.getInstance();

        long beforeCount = OptimizerAlertManager.getInstance().collectByView()
            .stream().filter(x -> x.getKey().equals(OptimizerAlertType.SELECTIVITY_ERR)).mapToLong(Pair::getValue)
            .sum();

        ExecutionContext ec = new ExecutionContext("tmp");
        ec.setTraceId("test");

        TableScanSelectivityEstimator mockEstimator = mock(TableScanSelectivityEstimator.class);
        when(mockEstimator.getExecutionContext()).thenReturn(ec);
        when(mockEstimator.evaluateInside(any())).thenAnswer(
            (Answer<Double>) invocation -> {
                throw new RuntimeException();
            });
        doCallRealMethod().when(mockEstimator).evaluate(any());
        mockEstimator.evaluate(null);

        long afterCount = OptimizerAlertManager.getInstance().collectByView()
            .stream().filter(x -> x.getKey().equals(OptimizerAlertType.SELECTIVITY_ERR)).mapToLong(Pair::getValue)
            .sum();
        Assert.assertTrue(beforeCount < afterCount, String.format("before %d, after %d", beforeCount, afterCount));
        TableScanIOEstimator mockIOEstimator = mock(TableScanIOEstimator.class);
        when(mockIOEstimator.getExecutionContext()).thenReturn(null);
        when(mockIOEstimator.evaluateInside(any())).thenAnswer(
            (Answer<Double>) invocation -> {
                throw new RuntimeException();
            });
        doCallRealMethod().when(mockEstimator).evaluate(any());
        mockEstimator.evaluate(null);
        long lastCount = OptimizerAlertManager.getInstance().collectByView()
            .stream().filter(x -> x.getKey().equals(OptimizerAlertType.SELECTIVITY_ERR)).mapToLong(Pair::getValue)
            .sum();
        Assert.assertTrue(afterCount < lastCount, String.format("before %d, after %d", afterCount, lastCount));
    }

}
