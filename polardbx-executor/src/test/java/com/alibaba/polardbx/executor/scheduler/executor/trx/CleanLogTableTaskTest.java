package com.alibaba.polardbx.executor.scheduler.executor.trx;

import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.rpc.compatible.ArrayResultSet;
import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.QUERY_TABLE_ROWS_TX_TABLE_V2;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CleanLogTableTaskTest {
    @Test
    public void test0() throws SQLException {
        Set<String> dnIds = new HashSet<>();
        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = mockStatic(InstConfUtil.class)) {
            instConfUtilMockedStatic.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(true);
            Timestamp t0 = new Timestamp(2024, 1, 1, 0, 0, 0, 0);
            Timestamp t1 = new Timestamp(2024, 1, 2, 1, 0, 0, 0);
            Assert.assertTrue(CleanLogTableTask.shouldCleanTrxLog(dnIds, t0.getTime(), t1.getTime()));
        }
    }

    @Test
    public void test1() throws SQLException {
        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = mockStatic(InstConfUtil.class);
            MockedStatic<DbTopologyManager> dbTopologyManagerMockedStatic = mockStatic(DbTopologyManager.class)) {
            instConfUtilMockedStatic.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(true);
            Timestamp t0 = new Timestamp(2024, 1, 1, 0, 0, 0, 0);
            Timestamp t1 = new Timestamp(2024, 1, 1, 1, 0, 0, 0);

            ExecutorContext executorContext = new ExecutorContext();
            ExecutorContext.setContext(DEFAULT_DB_NAME, executorContext);
            ITopologyExecutor topologyExecutor = mock(ITopologyExecutor.class);
            executorContext.setTopologyExecutor(topologyExecutor);
            ServerThreadPool serverThreadPool = mock(ServerThreadPool.class);
            when(topologyExecutor.getExecutorService()).thenReturn(serverThreadPool);
            when(serverThreadPool.submit(Mockito.any(), Mockito.any(), Mockito.any(Runnable.class))).then(
                invocation -> {
                    Runnable runnable = invocation.getArgument(2);
                    runnable.run();
                    return Futures.immediateFuture(null);
                });

            Connection connection = mock(Connection.class);
            dbTopologyManagerMockedStatic.when(
                    () -> DbTopologyManager.getConnectionForStorage(Mockito.any(String.class)))
                .thenReturn(connection);
            ArrayResultSet rs = new ArrayResultSet();
            rs.getColumnName().add("count");
            rs.getRows().add(new Object[] {50000});
            Statement stmt = mock(Statement.class);
            when(connection.createStatement()).thenReturn(stmt);
            when(stmt.executeQuery(Mockito.anyString())).thenReturn(rs);

            Set<String> dnIds = new HashSet<>();
            dnIds.add("test");
            Assert.assertFalse(CleanLogTableTask.shouldCleanTrxLog(dnIds, t0.getTime(), t1.getTime()));
        }
    }

    @Test
    public void test2() throws SQLException {
        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = mockStatic(InstConfUtil.class);
            MockedStatic<DbTopologyManager> dbTopologyManagerMockedStatic = mockStatic(DbTopologyManager.class)) {
            instConfUtilMockedStatic.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(true);
            Timestamp t0 = new Timestamp(2024, 1, 1, 0, 0, 0, 0);
            Timestamp t1 = new Timestamp(2024, 1, 1, 1, 0, 0, 0);

            ExecutorContext executorContext = new ExecutorContext();
            ExecutorContext.setContext(DEFAULT_DB_NAME, executorContext);
            ITopologyExecutor topologyExecutor = mock(ITopologyExecutor.class);
            executorContext.setTopologyExecutor(topologyExecutor);
            ServerThreadPool serverThreadPool = mock(ServerThreadPool.class);
            when(topologyExecutor.getExecutorService()).thenReturn(serverThreadPool);
            when(serverThreadPool.submit(Mockito.any(), Mockito.any(), Mockito.any(Runnable.class))).then(
                invocation -> {
                    Runnable runnable = invocation.getArgument(2);
                    runnable.run();
                    return Futures.immediateFuture(null);
                });

            Connection connection = mock(Connection.class);
            dbTopologyManagerMockedStatic.when(
                    () -> DbTopologyManager.getConnectionForStorage(Mockito.any(String.class)))
                .thenReturn(connection);
            ArrayResultSet rs = new ArrayResultSet();
            rs.getColumnName().add("count");
            rs.getRows().add(new Object[] {100001});
            Statement stmt = mock(Statement.class);
            when(connection.createStatement()).thenReturn(stmt);
            when(stmt.executeQuery(QUERY_TABLE_ROWS_TX_TABLE_V2)).thenReturn(rs);
            ResultSet rs0 = mock(ResultSet.class);
            when(stmt.executeQuery(Mockito.anyString())).then(
                invocation -> {
                    String sql = invocation.getArgument(0);
                    if (sql.equals(QUERY_TABLE_ROWS_TX_TABLE_V2)) {
                        return rs;
                    }
                    return rs0;
                }
            );

            Set<String> dnIds = new HashSet<>();
            dnIds.add("test");
            Assert.assertTrue(CleanLogTableTask.shouldCleanTrxLog(dnIds, t0.getTime(), t1.getTime()));
        }
    }
}
