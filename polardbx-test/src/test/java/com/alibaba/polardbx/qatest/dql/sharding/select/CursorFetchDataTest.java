/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.GsiConstant.COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.GsiConstant.PK_COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.GsiConstant.PK_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;

public class CursorFetchDataTest extends BaseTestCase {
    private final boolean debug = true;

    final private Map<Integer, String> tableData = ImmutableMap.of(
        1, "aaa",
        2, "bbb",
        3, "ccc",
        4, "ddd");

    // All tables contain two columns: id and c
    final private String columnsDef = "(id int primary key, c varchar(10))";

    private Connection tddlConnection;
    private Connection mysqlConnection;

    @Before
    public void before() {
        tddlConnection = getPolardbxConnection();
        mysqlConnection = getMysqlConnection();
    }

    @Test
    public void testFetchData() throws SQLException {
        final List<String> tableNames = new ArrayList<>();
        final String mysqlTableName = "cursor_fetch_data_test_mysql";
        try {
            // 1. Init some tables and data.
            initTables(tableNames);

            // 2. Init mysql table.
            initMysqlTable(mysqlTableName, mysqlConnection);

            // 3. Test each table with different fetch size. Since not all fetch size
            // is valid, if any exception occurs, compare it with mysql.
            final int[] fetchSizes = {Integer.MIN_VALUE, -1, 0, 1, 2, 3, Integer.MAX_VALUE - 1};
            for (String tableName : tableNames) {
                for (int fetchSize : fetchSizes) {
                    fetchTableData(tableName, mysqlTableName, fetchSize, true);
                    fetchTableData(tableName, mysqlTableName, fetchSize, false);
                }
            }
        } finally {
            for (String tableName : tableNames) {
                dropTable(tableName, tddlConnection);
            }
            dropTable(mysqlTableName, mysqlConnection);
        }
    }

    private void fetchTableData(String tableName, String mysqlTableName, int fetchSize, boolean autocommit)
        throws SQLException {
        System.out.println("table name: " + tableName + ", fetch size: " + fetchSize + ", autocommit: " + autocommit);

        String polarXError;
        String mysqlError;

        try (final Connection conn = getPolardbxConnectionWithExtraParams("&useCursorFetch=true")) {
            JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_CURSOR_FETCH = TRUE");
            polarXError = fetchTableDataWithConn(tableName, fetchSize, conn, autocommit, true);
        }

        try (final Connection conn = getMysqlConnectionWithExtraParams("&useCursorFetch=true")) {
            mysqlError = fetchTableDataWithConn(mysqlTableName, fetchSize, conn, autocommit, false);
        }

        // Compare errors.
        final String streamingError =
            "No statements may be issued when any streaming result sets are open and in use on a given connection.";
        if (polarXError.contains(streamingError)) {
            // Streaming error contains the result set id,
            // and they may be different for different result set.
            // So we only compare a part of the error message.
            Assert.assertTrue("streaming error", mysqlError.contains(streamingError));
        } else {
            Assert.assertEquals("other errors", polarXError, mysqlError);
        }

    }

    private String fetchTableDataWithConn(String tableName, int fetchSize, Connection conn, boolean autocommit,
                                          boolean polarx)
        throws SQLException {
        conn.setAutoCommit(autocommit);
        final String sql = "select * from " + tableName + " order by id";
        // Only allow the following cases
        // 1. Prepare a statement 1.
        try (final PreparedStatement ps = conn
            .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            // 2. Set fetch size for statement 1.
            ps.setFetchSize(fetchSize);
            // 3. Execute statement 1.
            ResultSet rs = ps.executeQuery();
            fetchRowData(rs);
        } catch (Throwable e) {
            // If fetch size <= 0 and is not equal to Integer.MIN_VAL,
            // an illegal-value exception will be thrown.
            // If fetch size is exceeds JVM memory limit,
            // an OOM exception will be thrown.
            return e.getMessage();
        } finally {
            if (!autocommit) {
                conn.rollback();
            }
        }

        if (!polarx) {
            return "";
        }

        // Not allow this cases in polardb-x:
        // 1. Prepare a statement 1.
        try (final PreparedStatement ps = conn
            .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            // 2. Set fetch size for statement 1.
            ps.setFetchSize(3);
            // 3. Execute statement 1.
            ps.executeQuery();

            boolean fail = false;
            // 4. Before fetching data for statement 1, execute a statement 2.
            try (final Statement stmt = conn
                .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                // 5. Set fetch size for statement 2.
                stmt.setFetchSize(2);
                // 6. Execute statement 2.
                stmt.executeQuery(sql);
            } catch (SQLException e) {
                fail = true;
                Assert.assertTrue(e.getMessage().contains("Not allow to execute commands except for"));
            }
            Assert.assertTrue(fail);
        } finally {
            if (!autocommit) {
                conn.rollback();
            }
        }

        // No errors.
        return "";
    }

    private void fetchRowData(ResultSet rs) throws SQLException {
        int fetchRows = 0;
        while (rs.next()) {
            fetchRows++;
            final int id = rs.getInt("id");
            final String c = rs.getString("c");
            Assert.assertEquals("fetch data", c, tableData.get(id));
        }
        Assert.assertEquals("fetch size", fetchRows, tableData.size());
    }

    private void dropTable(String tableName, Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            JdbcUtil.executeUpdateSuccess(connection, "drop table if exists " + tableName);
        } catch (Throwable throwable) {
            // ignore
            System.out.println(throwable.getMessage());
        }
    }

    private void initTables(List<String> tableNames) {
        // 1. Single table.
        final String tb1 = "cursor_fetch_data_test_single_table";
        tableNames.add(tb1);
        dropTable(tb1, tddlConnection);
        String sql = "create table " + tb1 + columnsDef;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 2. Partitioned table.
        final String tb2 = "cursor_fetch_data_test_partitioned_table";
        tableNames.add(tb2);
        dropTable(tb2, tddlConnection);
        sql = "create table " + tb2 + columnsDef + " dbpartition by hash(id) tbpartition by hash(c) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 3. Broadcast table.
        final String tb3 = "cursor_fetch_data_test_broadcast_table";
        tableNames.add(tb3);
        dropTable(tb3, tddlConnection);
        sql = "create table " + tb3 + columnsDef + " broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Insert some data.
        for (String tableName : tableNames) {
            for (Map.Entry<Integer, String> data : tableData.entrySet()) {
                sql = String.format("insert into %s values (%s, '%s')", tableName, data.getKey(), data.getValue());
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            }
        }
    }

    private void initMysqlTable(String mysqlTableName, Connection mysqlConn) {
        dropTable(mysqlTableName, mysqlConn);
        String sql = "create table " + mysqlTableName + columnsDef;
        JdbcUtil.executeUpdateSuccess(mysqlConn, sql);
        // Insert some data.
        for (Map.Entry<Integer, String> data : tableData.entrySet()) {
            sql = String.format("insert into %s values (%s, '%s')", mysqlTableName, data.getKey(), data.getValue());
            JdbcUtil.executeUpdateSuccess(mysqlConn, sql);
        }
    }

    @Test
    public void testTrx() throws SQLException {
        final List<String> tableNames = new ArrayList<>();

        try {
            initTables(tableNames);
            for (String tableName : tableNames) {
                // Only allow the following cases:
                try (final Connection conn = getPolardbxConnectionWithExtraParams("&useCursorFetch=true");
                    final Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_CURSOR_FETCH = TRUE");
                    try {
                        stmt.execute("begin");
                        String sql = "select * from " + tableName + " order by id";
                        stmt.setFetchSize(1);
                        ResultSet rs = stmt.executeQuery(sql);
                        fetchRowData(rs);

                        stmt.executeQuery("commit");

                        stmt.execute("begin");
                        sql = String.format("insert into %s values (%s, '%s')", tableName, 100, "xxxx");
                        stmt.execute(sql);
                        stmt.setFetchSize(4);
                        rs = stmt.executeQuery(sql);
                        fetchRowData(rs);
                    } catch (Throwable t) {
                        stmt.executeQuery("rollback");
                    }
                }

                // Not allow the following cases:
                try (final Connection conn = getPolardbxConnectionWithExtraParams("&useCursorFetch=true");
                    final Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_CURSOR_FETCH = TRUE");
                    try {
                        stmt.execute("begin");
                        String sql = "select * from " + tableName + " order by id";
                        stmt.setFetchSize(1);
                        ResultSet rs = stmt.executeQuery(sql);
                        fetchRowData(rs);

                        boolean fail = false;
                        // Create another statement in the same connection, which is not allowed.
                        try (final Statement stmt2 =
                            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                            stmt2.setFetchSize(2);
                            try {
                                stmt2.executeQuery(sql);
                            } catch (SQLException e) {
                                fail = true;
                                Assert.assertTrue(e.getMessage().contains("Not allow to execute commands except for"));
                            }
                            Assert.assertTrue(fail);
                        }

                        fail = false;
                        try {
                            stmt.setFetchSize(3);
                            stmt.executeQuery(sql);
                        } catch (SQLException e) {
                            fail = true;
                            Assert.assertTrue(e.getMessage().contains("Not allow to execute commands except for"));
                        }
                        Assert.assertTrue(fail);
                    } catch (Throwable t) {
                        stmt.executeQuery("rollback");
                    }
                }
            }
        } finally {
            for (String tableName : tableNames) {
                dropTable(tableName, tddlConnection);
            }
        }
    }

    @Test
    public void testFetchAllTypeData() {
        final String tableName = "cursor_fetch_data_test_all_type";
        // The pk is also the partitioned key.
        final String pk = TableConstant.C_ID;
        final String CREATE_TEMPLATE = "CREATE TABLE {0} ({1} {2}) {3}";
        final String INSERT_TEMPLATE = "INSERT INTO {0}({1}) VALUES({2})";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + tableName);
        try {
            // 1. Create table with all type columns.
            final String columnDef = FULL_TYPE_TABLE_COLUMNS.stream().map(
                c -> pk.equalsIgnoreCase(c) ? PK_COLUMN_DEF_MAP.get(c) : COLUMN_DEF_MAP.get(c)).collect(
                Collectors.joining());
            final String pkDef = PK_DEF_MAP.get(pk);
            final String partitionDef = GsiConstant.hashPartitioning(pk);
            final String createFullTypeTableSql =
                MessageFormat.format(CREATE_TEMPLATE, tableName, columnDef, pkDef, partitionDef);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createFullTypeTableSql);
            if (debug) {
                System.out.println(createFullTypeTableSql);
            }

            // 2. Test each column/type.
            final ImmutableMap<String, List<String>> COLUMN_VALUES = GsiConstant.buildGsiFullTypeTestValues();
            final List<String> errors = new ArrayList<>();
            for (Map.Entry<String, List<String>> columnAndVals : COLUMN_VALUES.entrySet()) {
                try {
                    final String columnName = columnAndVals.getKey();
                    if (pk.equalsIgnoreCase(columnName)) {
                        continue;
                    }
                    final List<String> values = columnAndVals.getValue();

                    // 2.1 Prepare data.
                    if (debug) {
                        System.out.println("------------------");
                        System.out.println(columnName);
                    }
                    boolean skip = false;
                    for (String value : values) {
                        final String insertSql = MessageFormat.format(INSERT_TEMPLATE, tableName,
                            String.join(",", pk, columnName),
                            String.join(",", "null", value));
                        try (Statement ps = tddlConnection.createStatement()) {
                            ps.executeUpdate(insertSql);
                        } catch (SQLException e) {
                            // ignore exception
                            System.out.println(MessageFormat.format("column[{0}] value[{1}] error[{2}]",
                                columnName, String.valueOf(value), e.getMessage()));
                            if (e.getMessage()
                                .contains("ERR-CODE: [PXC-4518][ERR_VALIDATE] : Unknown target column")) {
                                // If the column does not exist, skip this column.
                                skip = true;
                                break;
                            }
                        }
                    }

                    if (skip) {
                        continue;
                    }

                    // 2.2 Select data in normal mode.
                    if (debug) {
                        System.out.println("Non-cursor-fetch result:");
                    }
                    final String selectSql = MessageFormat.format("SELECT {0}, {1} FROM {2} ORDER BY {0}",
                        pk, columnName, tableName);
                    final Map<Integer, String> expectedResult = new HashMap<>();
                    String expectedError = null;
                    try (final Connection conn = getPolardbxConnectionWithExtraParams("&useServerPrepStmts=true");
                        final PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                        final ResultSet rs1 = stmt.executeQuery();
                        int i = 0;
                        while (rs1.next()) {
                            final int index = rs1.getInt(pk);
                            final String c = rs1.getString(columnName);
                            expectedResult.put(index, c);
                            if (debug) {
                                System.out.println(index + " " + " " + i + " " + c);
                            }
                        }
                    } catch (Throwable t) {
                        expectedError = t.getMessage();
                    }

                    // 2.3 Select data in cursor-fetch mode, and compare the result with that of normal mode.
                    if (debug) {
                        System.out.println("Cursor-fetch result:");
                    }
                    try (final Connection conn = getPolardbxConnectionWithExtraParams("&useCursorFetch=true");
                        final Statement stmt = conn
                            .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                        JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_CURSOR_FETCH = TRUE");
                        stmt.setFetchSize(1);
                        final ResultSet rs2 = stmt.executeQuery(selectSql);
                        int i = 0;
                        while (rs2.next()) {
                            final int index = rs2.getInt(pk);
                            final String c = rs2.getString(columnName);
                            if (debug) {
                                System.out.println(index + " " + " " + i + " " + c);
                            }
                            Assert.assertEquals("wrong data for column " + columnName,
                                expectedResult.get(index),
                                c);
                            i++;
                        }
                        Assert.assertEquals("wrong data size", i, expectedResult.size());
                    } catch (AssertionError e) {
                        throw e;
                    } catch (Throwable t) {
                        // Other exception.
                        System.out.println("Error occurs, expected is : " + expectedError);
                        System.out.println("Error occurs, actual is : " + t.getMessage());
                        Assert.assertEquals("wrong exception ", expectedError, t.getMessage());
                    }
                } catch (Throwable e) {
                    errors.add(e.getMessage());
                } finally {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, "TRUNCATE " + tableName);
                }
            }

            if (!errors.isEmpty()) {
                errors.forEach(System.out::println);
                Assert.fail("Test all type columns failed, first error is " + errors.get(0));
            }
        } finally {
            JdbcUtil.executeUpdate(tddlConnection, "DROP TABLE IF EXISTS " + tableName, false, true);
        }
    }

    @Test
    public void testSwitch() {
        final String tableName = "cursor_fetch_data_test_switch";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + tableName);
        try {
            String sql = "create table " + tableName + " (id int primary key, a int) dbpartition by hash(id)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            boolean fail = false;
            try (final Connection conn = getPolardbxConnectionWithExtraParams("&useCursorFetch=true");
                final Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_CURSOR_FETCH = false");
                stmt.setFetchSize(2);
                sql = "select * from " + tableName;
                stmt.execute(sql);
            } catch (Exception e) {
                fail = true;
                System.out.println(e.getMessage());
            }
            Assert.assertTrue(fail);
        } finally {
            JdbcUtil.executeUpdate(tddlConnection, "DROP TABLE IF EXISTS " + tableName, false, true);
        }
    }
}
