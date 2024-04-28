package com.alibaba.polardbx.common.properties;

import org.junit.Test;

/**
 * @author fangwu
 */
public class DynamicConfigTest {
    @Test
    public void testLoadInDegradationNum() {
        assert DynamicConfig.getInstance().getInDegradationNum() == 100L;
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.STATISTIC_IN_DEGRADATION_NUMBER, "1357");
        assert DynamicConfig.getInstance().getInDegradationNum() == 1357L;
    }
}
