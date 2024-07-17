package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RunWith(MockitoJUnitRunner.class)
public class PlanExecutorTest {
    @Test
    public void testCompatibleSwitchSession() {
        // default case
        ExecutionContext ec = new ExecutionContext();
        testCompatibleSwitchSession(ec, true, false, false, true);

        // set enable_oss_compatible = true
        ec = new ExecutionContext();
        Map<String, Object> params = new HashMap<>();
        params.put(ConnectionProperties.ENABLE_OSS_COMPATIBLE, true);
        ec.putAllHintCmdsWithDefault(params);
        testCompatibleSwitchSession(ec, false, false, true, false);

        // set enable_oss_compatible = false
        ec = new ExecutionContext();
        params.clear();
        params.put(ConnectionProperties.ENABLE_OSS_COMPATIBLE, false);
        ec.putAllHintCmdsWithDefault(params);
        testCompatibleSwitchSession(ec, false, false, false, false);

        // set ENABLE_COLUMNAR_SLICE_DICT = true
        ec = new ExecutionContext();
        params.clear();
        params.put(ConnectionProperties.ENABLE_OSS_COMPATIBLE, true);
        params.put(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT, true);
        ec.putAllHintCmdsWithDefault(params);
        testCompatibleSwitchSession(ec, false, false, false, false);
    }

    @Test
    public void testCompatibleSwitchGlobal() {
        MetaDbInstConfigManager instance = Mockito.mock(MetaDbInstConfigManager.class);
        try (MockedStatic<MetaDbInstConfigManager> mockedStatic = Mockito.mockStatic(MetaDbInstConfigManager.class)) {
            mockedStatic.when(MetaDbInstConfigManager::getInstance).thenAnswer(invocationOnMock -> instance);
            Properties properties = new Properties();
            properties.setProperty(ConnectionProperties.ENABLE_OSS_COMPATIBLE, "true");
            Mockito.when(instance.getCnVariableConfigMap()).thenReturn(properties);
            ExecutionContext ec = new ExecutionContext();
            Map<String, Object> params = PlanExecutor.getColumnarParams(ec);
            Assert.assertTrue(!params.containsKey(ConnectionProperties.ENABLE_OSS_COMPATIBLE));
            Assert.assertTrue(!((Boolean) params.get(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT)));
        }
    }

    private void testCompatibleSwitchSession(ExecutionContext ec, boolean containsOssComp, boolean ossCompatible,
                                             boolean containsSliceDict, boolean columnarSliceDict) {
        MetaDbInstConfigManager instance = Mockito.mock(MetaDbInstConfigManager.class);
        try (MockedStatic<MetaDbInstConfigManager> mockedStatic = Mockito.mockStatic(MetaDbInstConfigManager.class)) {
            mockedStatic.when(MetaDbInstConfigManager::getInstance).thenAnswer(invocationOnMock -> instance);
            Properties properties = new Properties();
            Mockito.when(instance.getCnVariableConfigMap()).thenReturn(properties);
            Map<String, Object> params = PlanExecutor.getColumnarParams(ec);
            if (containsOssComp) {
                Assert.assertTrue(params.containsKey(ConnectionProperties.ENABLE_OSS_COMPATIBLE));
                Assert.assertTrue(((Boolean) params.get(ConnectionProperties.ENABLE_OSS_COMPATIBLE)) == ossCompatible);
            } else {
                Assert.assertTrue(!params.containsKey(ConnectionProperties.ENABLE_OSS_COMPATIBLE));
            }
            if (containsSliceDict) {
                Assert.assertTrue(params.containsKey(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT));
                Assert.assertTrue(
                    ((Boolean) params.get(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT)) == columnarSliceDict);
            } else {
                Assert.assertTrue(!params.containsKey(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT));
            }
        }
    }
}
