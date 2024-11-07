package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.cdc.CdcTableUtil;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.trx.SyncPointTransaction;
import com.google.common.util.concurrent.Futures;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.cdc.CdcTableUtil.CDC_TABLE_SCHEMA;
import static com.alibaba.polardbx.server.handler.SyncPointExecutor.SHOW_SYNC_POINT_VAR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SyncPointExecutorTest {
    @Test
    public void testExecute() throws SQLException {
        try (MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic =
            mockStatic(InnerConnectionManager.class);
            MockedStatic<ExecUtils> execUtilsMockedStatic = mockStatic(ExecUtils.class);
            MockedStatic<TransactionManager> transactionManagerMockedStatic =
                mockStatic(TransactionManager.class);
            MockedStatic<MetaDbUtil> metaDbUtil = mockStatic(MetaDbUtil.class);) {
            Result result =
                prepare(innerConnectionManagerMockedStatic, execUtilsMockedStatic, transactionManagerMockedStatic,
                    metaDbUtil);
            List<TGroupDataSource> groupDataSources0;

            // Should success.
            Assert.assertTrue(SyncPointExecutor.getInstance().execute());
            for (Map.Entry<String, Object> entry : result.extraServerVariables.entrySet()) {
                if (ConnectionProperties.MARK_SYNC_POINT.equals(entry.getKey())) {
                    Assert.assertEquals("true", entry.getValue());
                }
            }
            Assert.assertTrue(result.inTrx.get());

            // Empty group, should fail.
            result.groupDataSources0.clear();
            Assert.assertFalse(SyncPointExecutor.getInstance().execute());

            groupDataSources0 = new ArrayList<>();
            groupDataSources0.add(result.ds0);
            when(result.showTablesRs.next()).thenReturn(false);
        }
    }

    @Test
    public void testEmptyGroup() throws SQLException {
        try (MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic =
            mockStatic(InnerConnectionManager.class);
            MockedStatic<ExecUtils> execUtilsMockedStatic = mockStatic(ExecUtils.class);
            MockedStatic<TransactionManager> transactionManagerMockedStatic =
                mockStatic(TransactionManager.class);
            MockedStatic<MetaDbUtil> metaDbUtil = mockStatic(MetaDbUtil.class);) {
            Result result =
                prepare(innerConnectionManagerMockedStatic, execUtilsMockedStatic, transactionManagerMockedStatic,
                    metaDbUtil);

            // Empty group, should fail.
            result.groupDataSources0.clear();
            Assert.assertFalse(SyncPointExecutor.getInstance().execute());
        }
    }

    @Test
    public void testNoPhyTable() throws SQLException {
        try (MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic =
            mockStatic(InnerConnectionManager.class);
            MockedStatic<ExecUtils> execUtilsMockedStatic = mockStatic(ExecUtils.class);
            MockedStatic<TransactionManager> transactionManagerMockedStatic =
                mockStatic(TransactionManager.class);
            MockedStatic<MetaDbUtil> metaDbUtil = mockStatic(MetaDbUtil.class);) {
            Result result =
                prepare(innerConnectionManagerMockedStatic, execUtilsMockedStatic, transactionManagerMockedStatic,
                    metaDbUtil);
            when(result.showTablesRs.next()).thenReturn(false);

            // Should success.
            Assert.assertTrue(SyncPointExecutor.getInstance().execute());
            for (Map.Entry<String, Object> entry : result.extraServerVariables.entrySet()) {
                if (ConnectionProperties.MARK_SYNC_POINT.equals(entry.getKey())) {
                    Assert.assertEquals("true", entry.getValue());
                }
            }
            Assert.assertTrue(result.inTrx.get());
        }
    }

    @Test
    public void testDnVarOff() throws SQLException {
        try (MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic =
            mockStatic(InnerConnectionManager.class);
            MockedStatic<ExecUtils> execUtilsMockedStatic = mockStatic(ExecUtils.class);
            MockedStatic<TransactionManager> transactionManagerMockedStatic =
                mockStatic(TransactionManager.class);
            MockedStatic<MetaDbUtil> metaDbUtil = mockStatic(MetaDbUtil.class);) {
            Result result =
                prepare(innerConnectionManagerMockedStatic, execUtilsMockedStatic, transactionManagerMockedStatic,
                    metaDbUtil);
            when(result.showSyncPointVarRs.getString(2)).thenReturn("OFF");
            Assert.assertFalse(SyncPointExecutor.getInstance().execute());

            when(result.showSyncPointVarRs.next()).thenReturn(false);
            Assert.assertFalse(SyncPointExecutor.getInstance().execute());
        }
    }

    @Test
    public void testTwoGroups() throws SQLException {
        try (MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic =
            mockStatic(InnerConnectionManager.class);
            MockedStatic<ExecUtils> execUtilsMockedStatic = mockStatic(ExecUtils.class);
            MockedStatic<TransactionManager> transactionManagerMockedStatic =
                mockStatic(TransactionManager.class);
            MockedStatic<MetaDbUtil> metaDbUtil = mockStatic(MetaDbUtil.class);) {
            Result result =
                prepare(innerConnectionManagerMockedStatic, execUtilsMockedStatic, transactionManagerMockedStatic,
                    metaDbUtil);

            TGroupDataSource ds1 = mock(TGroupDataSource.class);
            String group1 = "group1";
            List<TGroupDataSource> groupDataSources1 = result.groups.get("group0");
            result.groups.put(group1, groupDataSources1);

            // Should success.
            Assert.assertTrue(SyncPointExecutor.getInstance().execute());
            for (Map.Entry<String, Object> entry : result.extraServerVariables.entrySet()) {
                if (ConnectionProperties.MARK_SYNC_POINT.equals(entry.getKey())) {
                    Assert.assertEquals("true", entry.getValue());
                }
            }
            Assert.assertTrue(result.inTrx.get());
        }
    }

    @Test
    public void testError() throws SQLException {
        try (MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic =
            mockStatic(InnerConnectionManager.class);
            MockedStatic<ExecUtils> execUtilsMockedStatic = mockStatic(ExecUtils.class);
            MockedStatic<TransactionManager> transactionManagerMockedStatic =
                mockStatic(TransactionManager.class);
            MockedStatic<MetaDbUtil> metaDbUtil = mockStatic(MetaDbUtil.class);) {
            Result result =
                prepare(innerConnectionManagerMockedStatic, execUtilsMockedStatic, transactionManagerMockedStatic,
                    metaDbUtil);
            result.trxCount.getAndDecrement();

            // Empty group, should fail.
            Assert.assertFalse(SyncPointExecutor.getInstance().execute());
        }
    }

    @NotNull
    private static Result prepare(MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic,
                                  MockedStatic<ExecUtils> execUtilsMockedStatic,
                                  MockedStatic<TransactionManager> transactionManagerMockedStatic,
                                  MockedStatic<MetaDbUtil> metaDbUtil) throws SQLException {
        // Mock inner connection.
        InnerConnectionManager manager = mock(InnerConnectionManager.class);
        innerConnectionManagerMockedStatic.when(InnerConnectionManager::getInstance)
            .thenReturn(manager);
        InnerConnection mockInnerConnection = mock(InnerConnection.class);
        when(manager.getConnection(any())).thenReturn(mockInnerConnection);
        doNothing().when(mockInnerConnection).commit();

        // Mock setting TConnection.extraServerVariables
        Map<String, Object> extraServerVariables = new HashMap<>();
        doAnswer(invocation -> {
            extraServerVariables.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(mockInnerConnection).setExtraServerVariables(any(), any());

        // Mock get metadb connection
        Connection metadbConnection = mock(Connection.class);
        metaDbUtil.when(MetaDbUtil::getConnection).thenReturn(metadbConnection);
        metaDbUtil.when(() -> MetaDbUtil.delete(any(), any())).thenReturn(1);
        metaDbUtil.when(() -> MetaDbUtil.insert(any(), any(Map.class), any())).thenReturn(1);

        // Mock setting auto commit
        AtomicBoolean inTrx = new AtomicBoolean(false);
        doAnswer(invocation -> {
            inTrx.set(true);
            return null;
        }).when(mockInnerConnection).setAutoCommit(false);

        // Mock db operations.
        Statement mockStatement = mock(Statement.class);
        when(mockInnerConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(CdcTableUtil.QUERY_CDC_DDL_RECORD_LIMIT_1)).thenReturn(null);

        // Mock sync point trx.
        SyncPointTransaction trx = mock(SyncPointTransaction.class);
        when(mockInnerConnection.getTransaction()).thenReturn(trx);
        when(trx.getCommitTso()).thenReturn(1024L);

        // Mock getting all cdc groups.
        TGroupDataSource ds0 = mock(TGroupDataSource.class);
        String group0 = "group0";
        List<TGroupDataSource> groupDataSources0 = new ArrayList<>();
        groupDataSources0.add(ds0);
        Map<String, List<TGroupDataSource>> groups = new HashMap<>();
        groups.put(group0, groupDataSources0);
        execUtilsMockedStatic.when(() -> ExecUtils.getInstId2GroupList(CDC_TABLE_SCHEMA))
            .thenReturn(groups);

        // Mock transaction manager.
        TransactionManager transactionManager = mock(TransactionManager.class);
        transactionManagerMockedStatic.when((() -> TransactionManager.getInstance(any())))
            .thenReturn(transactionManager);
        TransactionExecutor executor = mock(TransactionExecutor.class);
        when(transactionManager.getTransactionExecutor()).thenReturn(executor);
        ServerThreadPool threadPool = mock(ServerThreadPool.class);
        when(executor.getExecutorService()).thenReturn(threadPool);
        AtomicInteger trxCount = new AtomicInteger();
        doAnswer(
            invocation -> {
                trxCount.getAndIncrement();
                return Futures.immediateFuture(((Callable<Boolean>) invocation.getArgument(2)).call());
            }
        ).when(threadPool).submit(any(), any(), any(Callable.class));
        doAnswer(
            invocation -> trxCount.get() + 1
        ).when(trx).getNumberOfPartitions();

        TGroupDirectConnection mockPhyConn = mock(TGroupDirectConnection.class);
        when(ds0.getConnection(MasterSlave.MASTER_ONLY)).thenReturn(mockPhyConn);
        Statement phyStatement = mock(Statement.class);
        when(mockPhyConn.createStatement()).thenReturn(phyStatement);
        ResultSet showTablesRs = mock(ResultSet.class);
        when(phyStatement.executeQuery(any())).thenReturn(showTablesRs);
        when(phyStatement.execute(any())).thenReturn(true);
        when(showTablesRs.next()).thenReturn(true);

        IConnection trxConn = mock(IConnection.class);
        when(trx.getConnection(any(), any(), any(), any())).thenReturn(trxConn);
        Statement trxStmt = mock(Statement.class);
        when(trxConn.createStatement()).thenReturn(trxStmt);
        ResultSet showSyncPointVarRs = mock(ResultSet.class);
        when(trxStmt.executeQuery(SHOW_SYNC_POINT_VAR)).thenReturn(showSyncPointVarRs);
        when(showSyncPointVarRs.next()).thenReturn(true);
        when(showSyncPointVarRs.getString(2)).thenReturn("ON");
        when(trxStmt.execute(any())).thenReturn(true);
        return new Result(extraServerVariables, inTrx, ds0, groupDataSources0, showTablesRs, showSyncPointVarRs,
            groups, trxCount);
    }

    private static class Result {
        public final Map<String, Object> extraServerVariables;
        public final AtomicBoolean inTrx;
        public final TGroupDataSource ds0;
        public final List<TGroupDataSource> groupDataSources0;
        public final ResultSet showTablesRs;
        public final ResultSet showSyncPointVarRs;
        public final Map<String, List<TGroupDataSource>> groups;
        public final AtomicInteger trxCount;

        public Result(Map<String, Object> extraServerVariables, AtomicBoolean inTrx, TGroupDataSource ds0,
                      List<TGroupDataSource> groupDataSources0, ResultSet showTablesRs,
                      ResultSet showSyncPointVarRs, Map<String, List<TGroupDataSource>> groups,
                      AtomicInteger trxCount) {
            this.extraServerVariables = extraServerVariables;
            this.inTrx = inTrx;
            this.ds0 = ds0;
            this.groupDataSources0 = groupDataSources0;
            this.showTablesRs = showTablesRs;
            this.showSyncPointVarRs = showSyncPointVarRs;
            this.groups = groups;
            this.trxCount = trxCount;
        }
    }
}
