package com.alibaba.polardbx;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CobarServerTest {
    @Test
    public void tryInitServerVariablesTest() throws SQLException {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        Connection mockConnection = mock(Connection.class, RETURNS_DEEP_STUBS);
        MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
        when(mockMetaDbDataSource.getConnection()).thenReturn(mockConnection);
        PreparedStatement prepareStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(Mockito.anyString())).thenReturn(prepareStatement);
        doNothing().when(prepareStatement).setString(anyInt(), anyString());
        doNothing().when(prepareStatement).addBatch();
        when(prepareStatement.executeBatch()).thenReturn(new int[] {1, 1});
        MetaDbConfigManager configManager = mock(MetaDbConfigManager.class);
        when(configManager.notify(anyString(), any())).thenReturn(1L);

        try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic =
            Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic =
                Mockito.mockStatic(MetaDbConfigManager.class);) {
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(configManager);

            Statement statement = mock(Statement.class);
            when(mockConnection.createStatement()).thenReturn(statement);
            ResultSet resultSet = mock(ResultSet.class);
            when(statement.executeQuery(anyString())).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getInt(1)).thenReturn(1);

            CobarServer.tryInitServerVariables();

            when(prepareStatement.executeBatch()).thenThrow(new SQLException("test"));
            CobarServer.tryInitServerVariables();
        }
        ConfigDataMode.setMode(mode);
    }
}
