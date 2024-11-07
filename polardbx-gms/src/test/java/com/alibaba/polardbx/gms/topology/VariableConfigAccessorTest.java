package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VariableConfigAccessorTest {
    @Test
    public void testAddVariableConfigs() throws Exception {
        String instId = "testInstId";
        Properties properties = new Properties();
        properties.setProperty("prop1", "val1");
        properties.setProperty("prop2", "val2");

        VariableConfigAccessor accessor = new VariableConfigAccessor();
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(Mockito.anyInt(), Mockito.anyString());
        doNothing().when(preparedStatement).addBatch();
        when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1});
        accessor.setConnection(connection);
        accessor.addVariableConfigs(instId, properties, false);

        try (MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic =
            Mockito.mockStatic(MetaDbConfigManager.class)) {
            MetaDbConfigManager metaDbConfigManager = mock(MetaDbConfigManager.class);
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);
            when(metaDbConfigManager.notify(any(), any())).thenReturn(1L);
            accessor.addVariableConfigs(instId, properties, true);
        }

        when(preparedStatement.executeBatch()).thenThrow(new SQLException("test_variable_config_accessor"));
        try {
            accessor.addVariableConfigs(instId, properties, false);
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("test_variable_config_accessor"));
        }
    }

}
