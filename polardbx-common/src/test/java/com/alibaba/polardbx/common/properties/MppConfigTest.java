package com.alibaba.polardbx.common.properties;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MppConfigTest {

    final private Logger logger = LoggerFactory.getLogger(MppConfigTest.class);

    @Test
    public void testEnableMppUI() {
        assertTrue(MppConfig.getInstance().isEnableMppUI());
        MppConfig.getInstance().loadValue(logger, ConnectionProperties.MPP_ENABLE_UI, "false");
        assertFalse(MppConfig.getInstance().isEnableMppUI());
        MppConfig.getInstance().loadValue(logger, ConnectionProperties.MPP_ENABLE_UI, "true");
        assertTrue(MppConfig.getInstance().isEnableMppUI());
    }
}
