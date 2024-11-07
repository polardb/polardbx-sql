package com.alibaba.polardbx.common.properties;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectionParamsTest {
    @Test
    public void testENABLE_XA_TSO() {
        // Assert that ENABLE_XA_TSO is an instance of BooleanConfigParam
        assertTrue("ENABLE_XA_TSO should be an instance of BooleanConfigParam",
            ConnectionParams.ENABLE_XA_TSO instanceof BooleanConfigParam);

        // Assert that the default value of ENABLE_XA_TSO is true
        assertTrue("ENABLE_XA_TSO default value should be true",
            Boolean.parseBoolean(ConnectionParams.ENABLE_XA_TSO.getDefault()));

        // Assert that ENABLE_XA_TSO is editable
        assertTrue("ENABLE_XA_TSO should be editable", ConnectionParams.ENABLE_XA_TSO.isMutable());
    }

    @Test
    public void testENABLE_AUTO_COMMIT_TSO() {
        assertTrue("ENABLE_AUTO_COMMIT_TSO should be an instance of BooleanConfigParam",
            ConnectionParams.ENABLE_AUTO_COMMIT_TSO instanceof BooleanConfigParam);

        assertFalse("ENABLE_AUTO_COMMIT_TSO default value should be false",
            Boolean.parseBoolean(ConnectionParams.ENABLE_AUTO_COMMIT_TSO.getDefault()));

        assertTrue("ENABLE_AUTO_COMMIT_TSO should be editable",
            ConnectionParams.ENABLE_AUTO_COMMIT_TSO.isMutable());
    }
}
