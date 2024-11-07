package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.when;

public class MetaBuilderTest {

    @SneakyThrows
    @Test
    public void testGetPhyConnection() {
        try (MockedStatic<ExecutorContext> contextMockedStatic = Mockito.mockStatic(ExecutorContext.class)) {
            ExecutorContext context = Mockito.mock(ExecutorContext.class);
            TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
            IGroupExecutor groupExecutor = Mockito.mock(IGroupExecutor.class);
            TGroupDataSource dataSource = Mockito.mock(TGroupDataSource.class);
            TGroupDirectConnection connection = new TGroupDirectConnection(dataSource, Mockito.mock(Connection.class));

            contextMockedStatic.when(() -> ExecutorContext.getContext("test")).thenReturn(context);
            when(context.getTopologyHandler()).thenReturn(topologyHandler);
            when(topologyHandler.get(Mockito.anyString())).thenReturn(groupExecutor);
            when(groupExecutor.getDataSource()).thenReturn(dataSource);
            when(dataSource.getConnection()).thenReturn(connection);
            when(dataSource.getDbGroupKey()).thenReturn("dgk");

            MetaBuilder.getPhyConnection("test", "group");

            Assert.assertEquals("", connection.getServerVariables().get("sql_mode"));
        }
    }

    @Test
    public void testPreparePhyConnection() throws SQLException {
        TGroupDataSource groupDataSource = Mockito.mock(TGroupDataSource.class);
        when(groupDataSource.getDbGroupKey()).thenReturn("db_group_1");
        Connection connection = Mockito.mock(Connection.class);
        TGroupDirectConnection groupDirectConnection = new TGroupDirectConnection(groupDataSource, connection);
        MetaBuilder.preparePhyConnection(groupDirectConnection);
        Assert.assertEquals("", groupDirectConnection.getServerVariables().get("sql_mode"));
    }
}
