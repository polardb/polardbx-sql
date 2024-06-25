package com.alibaba.polardbx.common.properties;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.constants.ServerVariables;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author fangwu
 */
public class DynamicConfigTest {
    @Test
    public void testLoadInDegradationNum() {
        assertTrue(DynamicConfig.getInstance().getInDegradationNum() == 100L);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.STATISTIC_IN_DEGRADATION_NUMBER, "1357");
        assertTrue(DynamicConfig.getInstance().getInDegradationNum() == 1357L);
    }

    @Test
    public void testBlackListConf() {
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 0);
        DynamicConfig.getInstance().loadValue(null, TddlConstants.BLACK_LIST_CONF, "");
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 0);

        DynamicConfig.getInstance().loadValue(null, TddlConstants.BLACK_LIST_CONF, "x1,y1");
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 2);
        assertTrue(ServerVariables.isGlobalBlackList("x1"));
        assertTrue(ServerVariables.isGlobalBlackList("y1"));
        assertFalse(ServerVariables.isGlobalBlackList("y1,x1"));
    }
}
