package com.alibaba.polardbx.common.constants;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static com.alibaba.polardbx.common.constants.ServerVariables.ENABLE_POLARX_SYNC_POINT;

public class ServerVariablesTest {
    @Test
    public void testMODIFIABLE_SYNC_POINT_PARAM() {
        Set<String> expected = ImmutableSet.of(
            ConnectionProperties.ENABLE_SYNC_POINT,
            ConnectionProperties.SYNC_POINT_TASK_INTERVAL);

        Set<String> actual = ServerVariables.MODIFIABLE_SYNC_POINT_PARAM;

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testVariables() {
        Assert.assertTrue(ServerVariables.variables.contains(ENABLE_POLARX_SYNC_POINT));
        Assert.assertTrue(ServerVariables.variables.contains("polarx_sync_point_timeout"));

        Assert.assertTrue(ServerVariables.mysqlGlobalVariables.contains(ENABLE_POLARX_SYNC_POINT));
        Assert.assertTrue(ServerVariables.mysqlGlobalVariables.contains("polarx_sync_point_timeout"));

        Assert.assertTrue(ServerVariables.mysqlDynamicVariables.contains(ENABLE_POLARX_SYNC_POINT));
        Assert.assertTrue(ServerVariables.mysqlDynamicVariables.contains("polarx_sync_point_timeout"));
    }
}
