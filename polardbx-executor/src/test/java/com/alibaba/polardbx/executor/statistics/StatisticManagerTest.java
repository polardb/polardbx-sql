package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

//@RunWith(MockitoJUnitRunner.class)
@Ignore
public class StatisticManagerTest {
    @Before
    public void setMetaDBInfo() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Ignore
    public void statisticStringTruncatedTest() {
        final int DATA_MAX_LEN = StatisticUtils.DATA_MAX_LEN;

        try (MockedStatic<ConfigDataMode> mockConfigDataMode =
            Mockito.mockStatic(ConfigDataMode.class, Mockito.CALLS_REAL_METHODS);
            MockedStatic<DataTypeUtil> mockDataTypeUtil =
                Mockito.mockStatic(DataTypeUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mockConfigDataMode.when(ConfigDataMode::isFastMock).thenReturn(true);
            StatisticManager statisticManager = Mockito.spy(StatisticManager.getInstance());
            mockDataTypeUtil.when(() -> DataTypeUtil.isStringType(any())).thenReturn(true);
            Mockito.when(
                    statisticManager.getFrequencyInner(anyString(), anyString(), anyString(), anyString(), anyBoolean()))
                .thenAnswer(
                    (Answer<StatisticResult>) invocation -> {
                        StatisticResult statisticResult = Mockito.mock(StatisticResult.class);
                        String fourthParameter = invocation.getArgument(3);
                        if (fourthParameter.length() > DATA_MAX_LEN) {
                            Mockito.when(statisticResult.getValue()).thenAnswer(
                                invocation1 -> Boolean.TRUE
                            );
                        } else {
                            Mockito.when(statisticResult.getValue()).thenAnswer(
                                invocation1 -> Boolean.FALSE
                            );
                        }
                        return statisticResult;
                    }
                );
            Assert.assertEquals(Boolean.FALSE,
                statisticManager.getFrequency("", "mock", "mock", "test", false).getValue());
            Assert.assertEquals(Boolean.FALSE, statisticManager.getFrequency("", "mock", "mock",
                "f6b443278141106364666dfe890e8cc93f2660d0142590acd42ca1054135ce12a90ab8ca12bdb390733837825f36643c00a91eb34ae8349a2e1d673b607b44d71cab141e7e38745f081940ef",
                false).getValue());
        }
    }
}
