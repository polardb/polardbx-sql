package com.alibaba.polardbx.common.utils;

import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

public class LockUtilTest {
    @Test
    public void testThrowException() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        Executor socketTimeoutExecutor = Mockito.mock(Executor.class);
        Mockito.when(connection.getNetworkTimeout()).thenReturn(10000);
        Mockito.doThrow(new RuntimeException("test test")).when(connection)
            .setNetworkTimeout(Mockito.any(), Mockito.anyInt());
        try {
            LockUtil.wrapWithSocketTimeout(connection, 1000, socketTimeoutExecutor, () -> null);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("test test"));
        }
    }
}
