package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerInfoAccessorTest {

    @Test
    public void testGetAllColumnarInstIdList() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("prop1", "val1");
        properties.setProperty("prop2", "val2");

        ServerInfoAccessor accessor = new ServerInfoAccessor();
        Connection connection = mock(Connection.class);
        ResultSet resultSet = mock(ResultSet.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(Mockito.anyInt(), Mockito.anyString());
        doNothing().when(preparedStatement).addBatch();
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        try (MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic =
            Mockito.mockStatic(MetaDbConfigManager.class)) {
            MetaDbConfigManager metaDbConfigManager = mock(MetaDbConfigManager.class);
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);
            when(metaDbConfigManager.notify(any(), any())).thenReturn(1L);
            Set<String> allColumnarInstIdList = accessor.loadColumnarInstIdAndUpdate(connection);
            Assert.assertTrue(allColumnarInstIdList.isEmpty());
        }
    }
}
