package com.alibaba.polardbx.group.utils;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.Mockito.when;

public class VariableProxyTest {

    @Test
    public void getSessionVariablesForJDBC() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {

            VariableProxy proxy = new VariableProxy(null);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            // Arrange
            Connection mockConnection = Mockito.mock(Connection.class);
            Statement mockStatement = Mockito.mock(Statement.class);
            ResultSet mockResultSet = Mockito.mock(ResultSet.class);

            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.executeQuery("SHOW SESSION VARIABLES")).thenReturn(mockResultSet);

            when(mockResultSet.next()).thenReturn(true, false);  // One record
            when(mockResultSet.getString("Variable_name")).thenReturn("max_connections");
            when(mockResultSet.getString("Value")).thenReturn("100");

            // Add the connection mock
            staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockConnection);

            // Act
            ImmutableMap<String, Object> result = proxy.getSessionVariables();

            // Assert
            ImmutableMap<String, Object> expected = ImmutableMap.of("max_connections", "100");
            Assert.assertEquals(expected, result);
        }

    }

    @Test
    public void getSessionVariablesForXProtocol() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {

            VariableProxy proxy = new VariableProxy(null);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            // Arrange
            // Arrange for X protocol mode
            Connection mockConnection = Mockito.mock(Connection.class);
            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> expectedMap = ImmutableMap.of("max_connections", "100");
            when(mockXClient.getSessionVariablesL()).thenReturn(expectedMap);

            // Add the connection mock
            staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockConnection);

            // Act
            ImmutableMap<String, Object> result = proxy.getSessionVariables();

            // Assert
            ImmutableMap<String, Object> expected = ImmutableMap.of("max_connections", "100");
            Assert.assertEquals(expected, result);
        }

    }
}
