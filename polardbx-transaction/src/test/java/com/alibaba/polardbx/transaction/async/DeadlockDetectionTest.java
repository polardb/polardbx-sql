package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.mock.MockStatus;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.utils.transaction.GroupConnPair;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.rpc.compatible.ArrayResultSet;
import com.alibaba.polardbx.transaction.utils.DiGraph;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_DEADLOCKS;
import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_LOCK_WAITS_80;
import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_TRX_80;

public class DeadlockDetectionTest {
    @Test
    public void test1() throws SQLException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.DEADLOCK_DETECTION_80_FETCH_TRX_ROWS, "5");
        InstanceVersion.setMYSQL80(true);
        MockStatus.setMock(true);
        DeadlockDetectionTask task = new DeadlockDetectionTask(new ArrayList<>());

        try (MockedStatic<DeadlockDetectionTask> deadlockDetectionTaskMockedStatic =
            Mockito.mockStatic(DeadlockDetectionTask.class)) {
            Connection connection = Mockito.mock(Connection.class);
            Statement stmt = Mockito.mock(Statement.class);
            TrxLookupSet lookupSet = new TrxLookupSet();
            addTrx(lookupSet, 10000L, "test_group_00000", 100L, 1000L, 1L, "waiting_sql");
            addTrx(lookupSet, 20000L, "test_group_00000", 200L, 2000L, 2L, "blocking_sql");

            deadlockDetectionTaskMockedStatic
                .when(() -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(Mockito.any()))
                .thenReturn(connection);
            Mockito.when(connection.createStatement()).thenReturn(stmt);
            Mockito.when(stmt.executeQuery(Mockito.anyString()))
                .thenAnswer(
                    invocation -> {
                        String sql = invocation.getArgument(0);
                        if (sql.contains(SQL_QUERY_TRX_80)) {
                            ArrayResultSet rs = getTrx80Rs();
                            rs.getRows().add(new Object[] {
                                1, 100, "LOCK WAIT", "waiting_physical_sql", "operation_state", 6, 7, 8, 9, 10
                            });
                            rs.getRows().add(new Object[] {
                                2, 200, "NULL", "blocking_physical_sql", "operation_state", 6, 7, 8, 9, 10
                            });
                            return rs;
                        } else if (sql.contains(SQL_QUERY_LOCK_WAITS_80)) {
                            ArrayResultSet rs = new ArrayResultSet();
                            rs.getColumnName().add("waiting_trx_id");
                            rs.getColumnName().add("blocking_trx_id");
                            rs.getRows().add(new Object[] {
                                1, 2
                            });
                            return rs;
                        } else {
                            return null;
                        }
                    }
                );
            DiGraph<TrxLookupSet.Transaction> graph = new DiGraph<>();
            task.fetchLockWaits(Mockito.mock(TGroupDataSource.class), ImmutableList.of("test_group_00000"), lookupSet,
                graph);
            TrxLookupSet.Transaction trx1 = lookupSet.getTransaction(10000L);
            TrxLookupSet.Transaction trx2 = lookupSet.getTransaction(20000L);
            Assert.assertEquals("waiting_physical_sql", trx1.getLocalTransaction("test_group_00000").getPhysicalSql());
            Assert.assertEquals("blocking_physical_sql", trx2.getLocalTransaction("test_group_00000").getPhysicalSql());
            System.out.println(graph);
        }
    }

    @Test
    public void test2() throws SQLException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.DEADLOCK_DETECTION_80_FETCH_TRX_ROWS, "5");
        InstanceVersion.setMYSQL80(false);
        MockStatus.setMock(true);
        DeadlockDetectionTask task = new DeadlockDetectionTask(new ArrayList<>());

        try (MockedStatic<DeadlockDetectionTask> deadlockDetectionTaskMockedStatic =
            Mockito.mockStatic(DeadlockDetectionTask.class)) {
            Connection connection = Mockito.mock(Connection.class);
            Statement stmt = Mockito.mock(Statement.class);
            TrxLookupSet lookupSet = new TrxLookupSet();
            addTrx(lookupSet, 10000L, "test_group_00000", 100L, 1000L, 1L, "waiting_sql");
            addTrx(lookupSet, 20000L, "test_group_00000", 200L, 2000L, 2L, "blocking_sql");

            deadlockDetectionTaskMockedStatic
                .when(() -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(Mockito.any()))
                .thenReturn(connection);
            Mockito.when(connection.createStatement()).thenReturn(stmt);
            Mockito.when(stmt.executeQuery(Mockito.anyString()))
                .thenAnswer(
                    invocation -> {
                        String sql = invocation.getArgument(0);
                        if (sql.contains(SQL_QUERY_DEADLOCKS)) {
                            return getDeadlocksRs();
                        } else {
                            return null;
                        }
                    }
                );
            DiGraph<TrxLookupSet.Transaction> graph = new DiGraph<>();
            task.fetchLockWaits(Mockito.mock(TGroupDataSource.class), ImmutableList.of("test_group_00000"), lookupSet,
                graph);
            TrxLookupSet.Transaction trx1 = lookupSet.getTransaction(10000L);
            TrxLookupSet.Transaction trx2 = lookupSet.getTransaction(20000L);
            Assert.assertEquals("waiting_physical_sql", trx1.getLocalTransaction("test_group_00000").getPhysicalSql());
            Assert.assertEquals("blocking_physical_sql", trx2.getLocalTransaction("test_group_00000").getPhysicalSql());
            System.out.println(graph);
        }
    }

    @Test
    public void test3() throws SQLException {
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD, "50000");
        InstanceVersion.setMYSQL80(true);
        MockStatus.setMock(true);
        DeadlockDetectionTask task = new DeadlockDetectionTask(new ArrayList<>());

        try (MockedStatic<DeadlockDetectionTask> deadlockDetectionTaskMockedStatic =
            Mockito.mockStatic(DeadlockDetectionTask.class)) {
            Connection connection = Mockito.mock(Connection.class);
            Statement stmt = Mockito.mock(Statement.class);
            deadlockDetectionTaskMockedStatic
                .when(() -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(Mockito.any()))
                .thenReturn(connection);
            Mockito.when(connection.createStatement()).thenReturn(stmt);
            Mockito.when(stmt.executeQuery(Mockito.anyString()))
                .thenReturn(getHotspotRs1());

            Assert.assertFalse(task.maybeTooManyDataLockWaits(Mockito.mock(TGroupDataSource.class)));
        }
    }

    @Test
    public void test4() throws SQLException {
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD, "50000");
        InstanceVersion.setMYSQL80(false);
        MockStatus.setMock(true);
        DeadlockDetectionTask task = new DeadlockDetectionTask(new ArrayList<>());

        try (MockedStatic<DeadlockDetectionTask> deadlockDetectionTaskMockedStatic =
            Mockito.mockStatic(DeadlockDetectionTask.class)) {
            Connection connection = Mockito.mock(Connection.class);
            Statement stmt = Mockito.mock(Statement.class);
            deadlockDetectionTaskMockedStatic
                .when(() -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(Mockito.any()))
                .thenReturn(connection);
            Mockito.when(connection.createStatement()).thenReturn(stmt);
            Mockito.when(stmt.executeQuery(Mockito.anyString()))
                .thenReturn(getHotspotRs2());

            Assert.assertTrue(task.maybeTooManyDataLockWaits(Mockito.mock(TGroupDataSource.class)));
        }
    }

    @NotNull
    private static ArrayResultSet getTrx80Rs() {
        ArrayResultSet rs = new ArrayResultSet();
        rs.getColumnName().add("trx_id");
        rs.getColumnName().add("conn_id");
        rs.getColumnName().add("state");
        rs.getColumnName().add("physical_sql");
        rs.getColumnName().add("operation_state");
        rs.getColumnName().add("tables_in_use");
        rs.getColumnName().add("tables_locked");
        rs.getColumnName().add("lock_structs");
        rs.getColumnName().add("heap_size");
        rs.getColumnName().add("row_locks");
        return rs;
    }

    @NotNull
    private static ArrayResultSet getDeadlocksRs() {
        ArrayResultSet rs = new ArrayResultSet();
        rs.getColumnName().add("waiting_conn_id");
        rs.getColumnName().add("blocking_conn_id");
        rs.getColumnName().add("waiting_state");
        rs.getColumnName().add("waiting_physical_sql");
        rs.getColumnName().add("waiting_operation_state");
        rs.getColumnName().add("waiting_tables_in_use");
        rs.getColumnName().add("waiting_tables_locked");
        rs.getColumnName().add("waiting_lock_structs");
        rs.getColumnName().add("waiting_heap_size");
        rs.getColumnName().add("waiting_row_locks");
        rs.getColumnName().add("waiting_lock_id");
        rs.getColumnName().add("waiting_lock_mode");
        rs.getColumnName().add("waiting_lock_type");
        rs.getColumnName().add("waiting_lock_physical_table");
        rs.getColumnName().add("waiting_lock_index");
        rs.getColumnName().add("waiting_lock_space");
        rs.getColumnName().add("waiting_lock_page");
        rs.getColumnName().add("waiting_lock_rec");
        rs.getColumnName().add("waiting_lock_data");
        rs.getColumnName().add("blocking_state");
        rs.getColumnName().add("blocking_physical_sql");
        rs.getColumnName().add("blocking_operation_state");
        rs.getColumnName().add("blocking_tables_in_use");
        rs.getColumnName().add("blocking_tables_locked");
        rs.getColumnName().add("blocking_lock_structs");
        rs.getColumnName().add("blocking_heap_size");
        rs.getColumnName().add("blocking_row_locks");
        rs.getColumnName().add("blocking_lock_id");
        rs.getColumnName().add("blocking_lock_mode");
        rs.getColumnName().add("blocking_lock_type");
        rs.getColumnName().add("blocking_lock_physical_table");
        rs.getColumnName().add("blocking_lock_index");
        rs.getColumnName().add("blocking_lock_space");
        rs.getColumnName().add("blocking_lock_page");
        rs.getColumnName().add("blocking_lock_rec");
        rs.getColumnName().add("blocking_lock_data");
        rs.getRows().add(new Object[] {
            100, 200,
            "LOCK WAIT", "waiting_physical_sql", "operation_state", 6, 7, 8, 9, 10, "lock_id", "RECORD LOCK",
            "X GAP", "physical_table", "PRIMARY", 100, 50, 25, 1234567,
            "NULL", "blocking_physical_sql", "operation_state", 6, 7, 8, 9, 10, "lock_id", "RECORD LOCK",
            "X GAP", "physical_table", "PRIMARY", 100, 50, 25, 1234567
        });
        return rs;
    }

    @NotNull
    private static ArrayResultSet getHotspotRs1() {
        ArrayResultSet rs = new ArrayResultSet();
        rs.getColumnName().add("cnt");
        rs.getColumnName().add("lock_id");
        rs.getRows().add(new Object[] {
            60000, "NULL",
            100, "1:1000"
        });
        return rs;
    }

    @NotNull
    private static ArrayResultSet getHotspotRs2() {
        ArrayResultSet rs = new ArrayResultSet();
        rs.getColumnName().add("cnt");
        rs.getColumnName().add("lock_id");
        rs.getRows().add(new Object[] {
            60000, "1:1000",
            100, "NULL"
        });
        return rs;
    }

    private static void addTrx(TrxLookupSet lookupSet, Long transId, String group, long connId, long frontendConnId,
                               Long startTime, String sql) {
        final GroupConnPair entry = new GroupConnPair(group, connId);
        lookupSet.addNewTransaction(entry, transId);
        lookupSet.updateTransaction(transId, frontendConnId, sql, startTime, false);
    }
}
