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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @version 1.0
 */
@Ignore
public class FullGsiDdlTest extends DDLBaseNewDBTestCase {

    private static final String gsiPrimaryTableName = "full_gsi_ddl";
    private static final String gsiIndexTableName = "g_i_c";
    private static final String gsiAllowAlterGsiIndirectly = "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    private static final String HASH_PARTITIONING_TEMPLATE =
        "DBPARTITION BY HASH({0}) TBPARTITION BY HASH({0}) TBPARTITIONS 3";
    private static final String DD_PARTITIONING_TEMPLATE = "TBPARTITION BY DD({0}) TBPARTITIONS 3";

    /**
     * GSI alter circle template.
     */
    // Create GSI.
    private static final String CREATE_GSI_TEMPLATE = "CREATE GLOBAL INDEX {0} ON {1}({2}) COVERING({3}) {4}";
    private static final String ALTER_TABLE_CREATE_GSI_TEMPLATE =
        "ALTER TABLE {1} ADD GLOBAL INDEX {0} ({2}) COVERING({3}) {4}";
    private static final String CREATE_UGSI_TEMPLATE = "CREATE GLOBAL UNIQUE INDEX {0} ON {1}({2}) COVERING({3}) {4}";
    private static final String ALTER_TABLE_CREATE_UGSI_TEMPLATE =
        "ALTER TABLE {1} ADD GLOBAL UNIQUE INDEX {0} ({2}) COVERING({3}) {4}";

    // Drop GSI.
    private static final String DROP_GSI_TEMPLATE = "DROP INDEX {0} ON {1}";
    private static final String ALTER_TABLE_DROP_INDEX = "ALTER TABLE {1} DROP INDEX {0}";

    // Rename primary table.
    private static final String RENAME_TABLE_TEMPLATE = "RENAME TABLE {0} TO {1}";
    private static final String ALTER_TABLE_RENAME_TABLE_TEMPLATE = "ALTER TABLE {0} RENAME TO {1}";

    // Rename index table.
    private static final String RENAME_GSI_TEMPLATE = "ALTER TABLE {0} RENAME INDEX {1} TO {2}";

    // Drop column.
    private static final String DROP_COLUMN_TEMPLATE = "ALTER TABLE {0} DROP COLUMN {1}";

    // Add column.
    private static final String ALTER_TABLE_ADD_COLUMN_TEMPLATE = "ALTER TABLE {0} ADD COLUMN {1} {2}";

    private static FastsqlParser fp = null;

    static {
        fp = new FastsqlParser();
    }

    @Before
    public void before() throws SQLException {
        assertThat(JdbcUtil.supportXA(tddlConnection), is(true));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + gsiPrimaryTableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + gsiPrimaryTableName + "_renamed");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiPrimaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiPrimaryTableName + "_renamed");
    }

    private SqlCreateTable showCreateTable2(Connection conn, String tableName) throws SQLException {
        String sql = "show create table " + tableName;

        Statement stmt = null;
        ResultSet resultSet = null;
        String createTableString = null;
        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(sql);
            resultSet.next();
            createTableString = resultSet.getString("Create Table");
        } finally {
            // close(stmt);
            JdbcUtil.close(resultSet);
        }
        return (SqlCreateTable) fp.parse(createTableString).get(0);
    }

    private static class InsertRunner implements Runnable {

        private final AtomicBoolean stop;
        private final FullGsiDdlTest ctx;
        private final boolean insertMySql;
        private final boolean useTransaction;
        private final String tddlDb;
        private final String mysqlDB;

        public InsertRunner(AtomicBoolean stop, FullGsiDdlTest ctx, boolean insertMySql, boolean useTransaction,
                            String tddlDb, String mysqlDB) {
            this.stop = stop;
            this.ctx = ctx;
            this.insertMySql = insertMySql;
            this.useTransaction = useTransaction;
            this.mysqlDB = mysqlDB;
            this.tddlDb = tddlDb;
        }

        @Override
        public void run() {
            try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection();
                Connection mysqlconn = ConnectionManager.getInstance().newMysqlConnection()) {
                JdbcUtil.useDb(conn, tddlDb);
                JdbcUtil.useDb(mysqlconn, mysqlDB);
                // List<Pair< sql, error_message >>
                List<Pair<String, Exception>> failedList = new ArrayList<>();

                int count = 0;

                long startTime = System.currentTimeMillis();
                long lastLoadSchemaTime = 0;
                String primaryTableName = null;
                SqlCreateTable table = null;

                do {
                    if (null == primaryTableName || null == table
                        || System.currentTimeMillis() - lastLoadSchemaTime > 1000) {
                        // Load latest schema per 1s.
                        primaryTableName = gsiPrimaryTableName;
                        try {
                            table = ctx.showCreateTable2(conn, primaryTableName);
                        } catch (Exception ignore) {
                            table = null;
                        }
                        if (null == table) {
                            primaryTableName = gsiPrimaryTableName + "_renamed";
                            try {
                                table = ctx.showCreateTable2(conn, primaryTableName);
                            } catch (Exception ignore) {
                                table = null;
                            }
                        }
                    }

                    if (null == table) {
                        Thread.sleep(100);
                        continue;
                    }

                    // Get columns which need to set.
                    Set<String> columnSet = new HashSet<>();
                    if (table.getGlobalKeys() != null) {
                        table.getGlobalKeys()
                            .forEach(pair -> columnSet.add(pair.right.getColumns().get(0).getColumnNameStr()));
                    }
                    if (table.getGlobalUniqueKeys() != null) {
                        table.getGlobalUniqueKeys().forEach(
                            pair -> pair.right.getColumns().forEach(col -> columnSet.add(col.getColumnNameStr())));
                    }
                    Assert.assertFalse(columnSet.contains(TableConstant.C_ID));
                    List<String> validColumns = new ArrayList<>(columnSet);
                    validColumns.add(0, TableConstant.C_ID);

                    // Get columns which not sharded.
                    List<String> updateColumns = table.getColDefs().stream()
                        .map(pair -> pair.right.getName().getLastName())
                        .filter(col -> !validColumns.contains(col))
                        .collect(Collectors.toList());

                    if (useTransaction) {
                        try {
                            conn.setAutoCommit(false);
                            mysqlconn.setAutoCommit(false);

                            int tmpCount = 0;
                            tmpCount += gsiExecuteUpdate(conn, mysqlconn,
                                GsiConstant.genRandomInsert(primaryTableName, validColumns), failedList, insertMySql,
                                false);
                            Thread.sleep(500);
                            tmpCount += gsiExecuteUpdate(conn, mysqlconn,
                                GsiConstant.genRandomInsert(primaryTableName, validColumns), failedList, insertMySql,
                                false);
                            Thread.sleep(500);
                            tmpCount += gsiExecuteUpdate(conn, mysqlconn,
                                GsiConstant.genRandomInsert(primaryTableName, validColumns), failedList, insertMySql,
                                false);

                            conn.commit();
                            try {
                                mysqlconn.commit();
                            } catch (SQLException ignore) {
                                Assert.fail("MySql commit error.");
                            }
                            count += tmpCount;
                        } catch (SQLException e) {
                            conn.rollback();
                            mysqlconn.rollback();
                        }
                        conn.setAutoCommit(true);
                        mysqlconn.setAutoCommit(true);
                    } else {
                        try {
                            count += gsiExecuteUpdate(conn, mysqlconn,
                                GsiConstant.genRandomInsert(primaryTableName, validColumns), failedList, insertMySql,
                                false);
                        } catch (SQLSyntaxErrorException e) {
                            if (!e.getMessage().contains("doesn't exist")) {
                                throw e;
                            }
                        }
                    }
                } while (!stop.get() && System.currentTimeMillis() - startTime
                    < 30 * 1000); // TODO: Hack here to prevent DDL starvation.

                System.out.println(Thread.currentThread().getName() + " quit after " + count + " records inserted");
            } catch (SQLException e) {
                throw new RuntimeException("Insert failed!", e);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void createSimpleTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, ExecuteTableSelect.getFullTypeTableDef(gsiPrimaryTableName,
            MessageFormat.format(HASH_PARTITIONING_TEMPLATE, TableConstant.C_ID, TableConstant.C_ID)));

        final ResultSet resultSet = JdbcUtil.executeQuery(
            "SELECT COUNT(1) FROM " + gsiPrimaryTableName,
            tddlConnection);
        assertThat(resultSet.next(), is(true));
    }

    private String formGsiName(String indexSk) {
        return gsiIndexTableName + "_ddl_" + indexSk;
    }

    private String genPartition(String indexSk) {
        if (TableConstant.dateType.contains(indexSk)) {
            return MessageFormat.format(DD_PARTITIONING_TEMPLATE, indexSk, indexSk);
        } else {
            return MessageFormat.format(HASH_PARTITIONING_TEMPLATE, indexSk, indexSk);
        }
    }

    private String genKeyLength(String indexSk) {
        if (TableConstant.longDataType.contains(indexSk)) {
            return "(13)";
        } else {
            return "";
        }
    }

    private boolean randomAddGsiFullCovering(String primaryTableName, boolean unique) throws SQLException {
        SqlCreateTable nowTable = showCreateTable2(tddlConnection, primaryTableName);

        List<String> existingGSI = null == nowTable.getGlobalKeys() ?
            new ArrayList<>() :
            nowTable.getGlobalKeys().stream().map(pair -> pair.right.getColumns().get(0).getColumnNameStr())
                .collect(Collectors.toList());
        List<String> existingUGSI = null == nowTable.getGlobalUniqueKeys() ?
            new ArrayList<>() :
            nowTable.getGlobalUniqueKeys().stream().map(pair -> pair.right.getColumns().get(0).getColumnNameStr())
                .collect(Collectors.toList());

        String primarySk = TableConstant.C_ID;

        // Select one or multi index.
        int multiIndexNumber = ThreadLocalRandom.current().nextInt(3) + 1; // 1,2,3
        List<String> multiIndexList = new ArrayList<>();
        for (int i = 0; i < multiIndexNumber; ++i) {
            List<String> restIndexSks = nowTable.getColDefs().stream()
                .map(pair -> pair.left.getLastName())
                // TODO: hack here remove date type.
                .filter(col -> !TableConstant.dateType.contains(col))
                // Json not support.
                .filter(col -> !col.equalsIgnoreCase(TableConstant.C_JSON))
                .filter(col -> !col.equalsIgnoreCase(primarySk))
                .filter(col -> !existingGSI.contains(col) && !existingUGSI.contains(col))
                .filter(col -> !multiIndexList.contains(col))
                .collect(Collectors.toList());
            if (0 == restIndexSks.size()) {
                return false;
            }
            multiIndexList.add(restIndexSks.get(ThreadLocalRandom.current().nextInt(restIndexSks.size())));
        }

        String indexColumns = multiIndexList.stream()
            .map(col -> col + genKeyLength(col))
            .collect(Collectors.joining(", "));
        String covering = nowTable.getColDefs().stream()
            .map(pair -> pair.left.getLastName())
            // do not support covering column with default current_timestamp
            .filter(column -> !column.equalsIgnoreCase(TableConstant.C_TIMESTAMP))
            .filter(column -> !column.equalsIgnoreCase(primarySk))
            .filter(column -> !multiIndexList.contains(column))
            .collect(Collectors.joining(", "));
        if (null == covering || covering.isEmpty()) {
            return false;
        }

        String template = unique ?
            (ThreadLocalRandom.current().nextBoolean() ? CREATE_UGSI_TEMPLATE : ALTER_TABLE_CREATE_UGSI_TEMPLATE)
            : (ThreadLocalRandom.current().nextBoolean() ? CREATE_GSI_TEMPLATE : ALTER_TABLE_CREATE_GSI_TEMPLATE);
        String sql = MessageFormat.format(template,
            formGsiName(multiIndexList.get(0)),
            primaryTableName,
            indexColumns,
            covering,
            genPartition(multiIndexList.get(0)));

        // Run.
        System.out.println("create GSI:\n" + sql);
        boolean hasIgnore = JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, GSI_ALLOW_ADD_HINT + sql,
            ImmutableSet.of("Rule generator dataType is not supported!",
                "Invalid type for a sharding key",
                "Unsupported index table structure"));

        if (!hasIgnore) {
            // Success and check result.
            nowTable = showCreateTable2(tddlConnection, primaryTableName);
            AtomicReference<SqlIndexDefinition> indexDefinitionRef = new AtomicReference<>(null);
            if (unique) {
                nowTable.getGlobalUniqueKeys().forEach(pair -> {
                    if (pair.left.getLastName().equals(formGsiName(multiIndexList.get(0)))) {
                        indexDefinitionRef.set(pair.right);
                    }
                });
            } else {
                nowTable.getGlobalKeys().forEach(pair -> {
                    if (pair.left.getLastName().equals(formGsiName(multiIndexList.get(0)))) {
                        indexDefinitionRef.set(pair.right);
                    }
                });
            }
            SqlIndexDefinition indexDefinition = indexDefinitionRef.get();
            Assert.assertNotNull(indexDefinition);

            // Check columns and covering.
            Assert.assertEquals(indexColumns,
                indexDefinition.getColumns().stream()
                    .map(col -> col.getColumnNameStr() + (null == col.getLength() ? "" :
                        "(" + col.getLength().toValue() + ")"))
                    .collect(Collectors.joining(", ")));
            Assert.assertEquals(TableConstant.C_ID + ", " + covering,
                indexDefinition.getCovering().stream()
                    .map(SqlIndexColumnName::getColumnNameStr)
                    .collect(Collectors.joining(", ")));

            return true;
        }
        return false;
    }

    private boolean randomDropGsi(String primaryTableName) throws SQLException {
        SqlCreateTable nowTable = showCreateTable2(tddlConnection, primaryTableName);

        List<String> existingIndexName = new ArrayList<>();
        if (nowTable.getGlobalKeys() != null) {
            existingIndexName.addAll(
                nowTable.getGlobalKeys().stream().map(pair -> pair.left.getLastName()).collect(Collectors.toList()));
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            existingIndexName.addAll(nowTable.getGlobalUniqueKeys().stream().map(pair -> pair.left.getLastName())
                .collect(Collectors.toList()));
        }
        if (0 == existingIndexName.size()) {
            return false;
        }
        String gsiName = existingIndexName.get(ThreadLocalRandom.current().nextInt(existingIndexName.size()));

        String sql =
            MessageFormat.format(ThreadLocalRandom.current().nextBoolean() ? DROP_GSI_TEMPLATE : ALTER_TABLE_DROP_INDEX,
                gsiName, primaryTableName);

        // Run.
        System.out.println("drop GSI:\n" + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiAllowAlterGsiIndirectly + sql);

        // Check.
        nowTable = showCreateTable2(tddlConnection, primaryTableName);
        if (nowTable.getGlobalKeys() != null) {
            nowTable.getGlobalKeys().forEach(pair -> Assert.assertNotEquals(gsiName, pair.left.getLastName()));
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            nowTable.getGlobalUniqueKeys().forEach(pair -> Assert.assertNotEquals(gsiName, pair.left.getLastName()));
        }

        return true;
    }

    private boolean randomDropColumn(String primaryTableName) throws SQLException {
        SqlCreateTable nowTable = showCreateTable2(tddlConnection, primaryTableName);

        String primarySk = TableConstant.C_ID;

        List<String> existingIndex = new ArrayList<>();
        if (nowTable.getGlobalKeys() != null) {
            // Add sharding key.
            existingIndex.addAll(
                nowTable.getGlobalKeys().stream().map(pair -> pair.right.getColumns().get(0).getColumnNameStr())
                    .collect(Collectors.toList()));
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            // Need add all unique index.
            nowTable.getGlobalUniqueKeys()
                .forEach(pair -> pair.right.getColumns().forEach(col -> existingIndex.add(col.getColumnNameStr())));
        }

        List<String> cols = nowTable.getColDefs().stream()
            .map(pair -> pair.left.getLastName())
            .filter(column -> !column.equalsIgnoreCase(primarySk))
            .filter(column -> !existingIndex.contains(column))
            .collect(Collectors.toList());
        // Need at least 1 column after drop.
        if (cols.size() <= 1) {
            return false;
        }
        String dropColumn = cols.get(ThreadLocalRandom.current().nextInt(cols.size()));

        String sql = MessageFormat.format(DROP_COLUMN_TEMPLATE, primaryTableName, dropColumn);
        // Run.
        System.out.println("drop column:\n" + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiAllowAlterGsiIndirectly + sql);

        // Check not in column, gsi column/covering, ugsi column/covering.
        nowTable = showCreateTable2(tddlConnection, primaryTableName);
        nowTable.getColDefs().stream()
            .map(pair -> pair.left.getLastName())
            .forEach(col -> Assert.assertNotEquals(dropColumn, col));
        if (nowTable.getGlobalKeys() != null) {
            nowTable.getGlobalKeys().stream()
                .map(pair -> pair.right.getColumns())
                .forEach(list -> list.forEach(col -> Assert.assertNotEquals(dropColumn, col.getColumnNameStr())));
            nowTable.getGlobalKeys().stream()
                .map(pair -> pair.right.getCovering())
                .forEach(list -> list.forEach(col -> Assert.assertNotEquals(dropColumn, col.getColumnNameStr())));
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            nowTable.getGlobalUniqueKeys().stream()
                .map(pair -> pair.right.getColumns())
                .forEach(list -> list.forEach(col -> Assert.assertNotEquals(dropColumn, col.getColumnNameStr())));
            nowTable.getGlobalUniqueKeys().stream()
                .map(pair -> pair.right.getCovering())
                .forEach(list -> list.forEach(col -> Assert.assertNotEquals(dropColumn, col.getColumnNameStr())));
        }

        return true;
    }

    private boolean randomAddColumn(String primaryTableName) throws SQLException {
        List<Pair<String, String>> columnDefs = new ArrayList<>();
        for (int i = 0; i < TableConstant.FULL_TYPE_TABLE_COLUMNS.size(); ++i) {
            columnDefs.add(new Pair<>(TableConstant.FULL_TYPE_TABLE_COLUMNS.get(i),
                TableConstant.FULL_TYPE_TABLE_COLUMNS_TYPE.get(i)));
        }

        SqlCreateTable nowTable = showCreateTable2(tddlConnection, primaryTableName);

        List<String> existingColumns = nowTable.getColDefs().stream()
            .map(pair -> pair.left.getLastName())
            .collect(Collectors.toList());
        List<Pair<String, String>> missingColumns = columnDefs.stream()
            .filter(pair -> !existingColumns.contains(pair.getKey()))
            .collect(Collectors.toList());

        if (0 == missingColumns.size()) {
            return false;
        }

        Pair<String, String> addCol = missingColumns.get(ThreadLocalRandom.current().nextInt(missingColumns.size()));
        String sql = MessageFormat.format(ALTER_TABLE_ADD_COLUMN_TEMPLATE,
            primaryTableName, addCol.getKey(), addCol.getValue());

        // Run.
        System.out.println("add column:\n" + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiAllowAlterGsiIndirectly + sql);

        // Check.
        nowTable = showCreateTable2(tddlConnection, primaryTableName);
        Assert.assertEquals(1,
            nowTable.getColDefs().stream().filter(pair -> pair.left.getLastName().equals(addCol.getKey())).count());

        return true;
    }

    private String renameTable(String originalName) {
        String newName = originalName.endsWith("_renamed") ?
            originalName.substring(0, originalName.length() - "_renamed".length()) :
            originalName + "_renamed";

        String template = ThreadLocalRandom.current().nextBoolean() ?
            RENAME_TABLE_TEMPLATE :
            ALTER_TABLE_RENAME_TABLE_TEMPLATE;
        String sql = MessageFormat.format(template,
            originalName, newName);
        // Run.
        System.out.println("rename table:\n" + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiAllowAlterGsiIndirectly + sql);

        // Check.
        JdbcUtil.executeQueryFaied(tddlConnection, "show create table `" + originalName + "`", "doesn't exist");
        JdbcUtil.executeQuerySuccess(tddlConnection, "show create table `" + newName + "`");

        return newName;
    }

    private boolean renameGsi(String primaryTableName) throws SQLException {
        SqlCreateTable nowTable = showCreateTable2(tddlConnection, primaryTableName);

        List<String> existingIndexName = new ArrayList<>();
        if (nowTable.getGlobalKeys() != null) {
            existingIndexName.addAll(
                nowTable.getGlobalKeys().stream().map(pair -> pair.left.getLastName()).collect(Collectors.toList()));
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            existingIndexName.addAll(nowTable.getGlobalUniqueKeys().stream().map(pair -> pair.left.getLastName())
                .collect(Collectors.toList()));
        }
        if (0 == existingIndexName.size()) {
            return false;
        }

        String gsiName = existingIndexName.get(ThreadLocalRandom.current().nextInt(existingIndexName.size()));
        String newName = gsiName.endsWith("_renamed") ?
            gsiName.substring(0, gsiName.length() - "_renamed".length()) :
            gsiName + "_renamed";

        String sql = MessageFormat.format(RENAME_GSI_TEMPLATE, primaryTableName, gsiName, newName);
        // Run.
        System.out.println("rename GSI:\n" + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiAllowAlterGsiIndirectly + sql);

        // Check.
        nowTable = showCreateTable2(tddlConnection, primaryTableName);
        if (nowTable.getGlobalKeys() != null) {
            nowTable.getGlobalKeys().stream()
                .map(pair -> pair.left.getLastName())
                .forEach(name -> Assert.assertNotEquals(gsiName, name));
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            nowTable.getGlobalUniqueKeys().stream()
                .map(pair -> pair.left.getLastName())
                .forEach(name -> Assert.assertNotEquals(gsiName, name));
        }
        int nameCount = 0;
        if (nowTable.getGlobalKeys() != null) {
            nameCount += nowTable.getGlobalKeys().stream()
                .map(pair -> pair.left.getLastName()).filter(name -> name.equals(newName)).count();
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            nameCount += nowTable.getGlobalUniqueKeys().stream()
                .map(pair -> pair.left.getLastName()).filter(name -> name.equals(newName)).count();
        }
        Assert.assertEquals(1, nameCount);

        return true;
    }

    private void checkIntegrity(String primaryTable, boolean compareMySql) {
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

        String showTable = showCreateTable(tddlConnection, primaryTable);
        System.out.println(showTable);

        SqlCreateTable nowTable = (SqlCreateTable) fp.parse(showTable).get(0);
        List<String> primaryColumns =
            nowTable.getColDefs().stream().map(pair -> pair.left.getLastName()).collect(Collectors.toList());
        Map<String, List<String>> indexColumns = new HashMap<>();
        if (nowTable.getGlobalKeys() != null) {
            nowTable.getGlobalKeys().forEach(pair -> {
                String name = pair.left.getLastName();
                Set<String> cols = new HashSet<>();
                pair.right.getColumns().forEach(col -> cols.add(col.getColumnNameStr()));
                pair.right.getCovering().forEach(col -> cols.add(col.getColumnNameStr()));
                indexColumns.put(name, primaryColumns.stream()
                    .filter(cols::contains)
                    .collect(Collectors.toList()));
            });
        }
        if (nowTable.getGlobalUniqueKeys() != null) {
            nowTable.getGlobalUniqueKeys().forEach(pair -> {
                String name = pair.left.getLastName();
                Set<String> cols = new HashSet<>();
                pair.right.getColumns().forEach(col -> cols.add(col.getColumnNameStr()));
                pair.right.getCovering().forEach(col -> cols.add(col.getColumnNameStr()));
                indexColumns.put(name, primaryColumns.stream()
                    .filter(cols::contains)
                    .collect(Collectors.toList()));
            });
        }

        // Now read and compare.
        indexColumns.forEach((indexName, columns) -> {
            String drdsColumns = columns.stream()
                .filter(col -> !col.equalsIgnoreCase(TableConstant.C_TIMESTAMP))
                .collect(Collectors.joining(", "));
            String mysqlColumns = columns.stream()
                .filter(col -> !col.equalsIgnoreCase(TableConstant.C_TIMESTAMP))
                .filter(col -> !col.equalsIgnoreCase(TableConstant.C_ID))
                .collect(Collectors.joining(", "));
            System.out.println("Check integrity: " + indexName + " col: (" + drdsColumns + ")");

            gsiIntegrityCheck(primaryTable, indexName, drdsColumns, mysqlColumns, compareMySql);
        });

        System.out.println("################################################################################");
    }

    private static final int RANDOM_TIME_SECONDS = 20;

    @Test
    public void test_createSimple_then_random_0() throws Exception {
        test_createSimple_then_random(true, false);
    }

    @Test
    public void test_createSimple_then_random_1() throws Exception {
        test_createSimple_then_random(false, false);
    }

    @Test
    public void test_createSimple_then_random_2() throws Exception {
        test_createSimple_then_random(true, true);
    }

    @Test
    public void test_createSimple_then_random_3() throws Exception {
        test_createSimple_then_random(false, true);
    }

    @Test
    public void test_long_transaction() throws Exception {
        createSimpleTable();

        // Add concurrent inserts.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, ExecuteTableSelect.getFullTypeTableDef(gsiPrimaryTableName, ""));

        // Fill data in concurrent.
        int concurrentNumber = 8;
        AtomicBoolean stop = new AtomicBoolean(false);
        ExecutorService dmlPool = Executors.newFixedThreadPool(concurrentNumber);
        List<Future> inserts = new ArrayList<>();
        for (int i = 0; i < concurrentNumber; ++i) {
            inserts.add(dmlPool.submit(new InsertRunner(stop, this, true, true, tddlDatabase1, mysqlDatabase1)));
        }

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            // ignore exception
        }

        stop.set(true);
        dmlPool.shutdown();

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        // Check integrity.
        checkIntegrity(gsiPrimaryTableName, true);
    }

    public void test_createSimple_then_random(boolean compareWithMySql, boolean testLongTransaction) throws Exception {
        createSimpleTable();

        // Add concurrent inserts.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, ExecuteTableSelect.getFullTypeTableDef(gsiPrimaryTableName, ""));

        // Fill data in concurrent.
        int concurrentNumber = testLongTransaction ? 8 : 2;
        AtomicBoolean stop = new AtomicBoolean(false);
        ExecutorService dmlPool = Executors.newFixedThreadPool(concurrentNumber);
        List<Future> inserts = new ArrayList<>();
        for (int i = 0; i < concurrentNumber; ++i) {
            inserts.add(dmlPool.submit(new InsertRunner(
                stop, this, compareWithMySql, testLongTransaction, tddlDatabase1, mysqlDatabase1)));
        }

        System.out.println("Insert some data.");
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            // ignore exception
        }
        System.out.println("Insert ok. Now start random ddl.");

        final ResultSet resultSet = JdbcUtil.executeQuery(
            "SELECT COUNT(1) FROM " + gsiPrimaryTableName,
            tddlConnection);
        assertThat(resultSet.next(), is(true));
        //assertThat(resultSet.getLong(1), greaterThan(0L));

        String primaryTableName = gsiPrimaryTableName;

        long startTime = System.currentTimeMillis();

        again:
        do {
            int op = ThreadLocalRandom.current().nextInt(7);
            boolean bret;
            switch (op) {
            case 0:
                do {
                    bret = randomAddGsiFullCovering(primaryTableName, false);
                    if (!bret) {
                        SqlCreateTable nowTable = showCreateTable2(tddlConnection, primaryTableName);
                        if (nowTable.getGlobalKeys() != null && nowTable.getGlobalKeys().size() >= 3) {
                            continue again;
                        }
                    }
                } while (!bret);
                break;

            case 1:
                // TODO: hack here remove unique.
                continue again;
//                    do {
//                        bret = randomAddGsiFullCovering(primaryTableName, true);
//                        if (!bret) {
//                            SqlCreateTable nowTable = showCreateTable2(polarDbXConnection, primaryTableName);
//                            if (nowTable.getGlobalUniqueKeys() != null && nowTable.getGlobalUniqueKeys().size() >= 3) {
//                                bret = true;
//                            }
//                        }
//                    } while (!bret);
//                    break;

            case 2:
                if (compareWithMySql) {
                    // May mismatch because rename can cause some failure.
                    continue again;
                }
                primaryTableName = renameTable(primaryTableName);
                break;

            case 3:
                if (!renameGsi(primaryTableName)) {
                    continue again;
                }
                break;

            case 4:
                if (compareWithMySql) {
                    // Some problem when add column.
                    continue again;
                }
                if (!randomDropColumn(primaryTableName)) {
                    continue again;
                }
                break;

            case 5: {
                SqlCreateTable nowTable = showCreateTable2(tddlConnection, primaryTableName);
                int gsiCount = 0;
                if (nowTable.getGlobalKeys() != null) {
                    gsiCount += nowTable.getGlobalKeys().size();
                }
                if (nowTable.getGlobalUniqueKeys() != null) {
                    gsiCount += nowTable.getGlobalUniqueKeys().size();
                }
                if (gsiCount >= 2) {
                    // Leave at least 1 GSI for integrity check.
                    randomDropGsi(primaryTableName);
                } else {
                    continue again;
                }
            }
            break;

            case 6:
                if (compareWithMySql) {
                    // May mismatch because adding column on drds & mysql are not at same time.
                    continue again;
                }
                if (!randomAddColumn(primaryTableName)) {
                    continue again;
                }
                break;
            }

            stop.set(true);
            dmlPool.shutdown();

            for (Future future : inserts) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            // Check integrity.
            checkIntegrity(primaryTableName, compareWithMySql);

            // Check done, start thread and again.
            stop = new AtomicBoolean(false);
            dmlPool = Executors.newFixedThreadPool(concurrentNumber);
            inserts = new ArrayList<>();
            for (int i = 0; i < concurrentNumber; ++i) {
                inserts.add(dmlPool.submit(
                    new InsertRunner(stop, this, compareWithMySql, testLongTransaction, tddlDatabase1,
                        mysqlDatabase1)));
            }

            // Insert some data.
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                // ignore exception
            }

        } while (System.currentTimeMillis() - startTime < RANDOM_TIME_SECONDS * 1000);

        // Final stop.
        stop.set(true);
        dmlPool.shutdown();

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        // Check integrity.
        checkIntegrity(primaryTableName, compareWithMySql);

        // Done, then drop table.
        // JdbcUtil.executeUpdateSuccess(polarDbXConnection, "DROP TABLE IF EXISTS " + primaryTableName);
        // JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
    }

    private static final int GSI_COUNT = 3;
    private static final int UGSI_COUNT = 3;
    private static final int RENAME_TABLE_COUNT = 3;
    private static final int RENAME_GSI_COUNT = 3;
    private static final int DROP_COLUMN_COUNT = 3;
    private static final int DROP_INDEX_COUNT = 3;
    private static final int ADD_COLUMN_COUNT = 3;

    @Test
    public void test_createSimple_then_random_seq() throws Exception {
        createSimpleTable();

        String primaryTableName = gsiPrimaryTableName;

        // Create GSI.
        int gsiCount = 0;
        do {
            boolean bret = randomAddGsiFullCovering(primaryTableName, false);
            if (bret) {
                ++gsiCount;
            }
        } while (gsiCount < GSI_COUNT);

        // Create UGSI.
        gsiCount = 0;
        do {
            boolean bret = randomAddGsiFullCovering(primaryTableName, true);
            if (bret) {
                ++gsiCount;
            }
        } while (gsiCount < UGSI_COUNT);

        // Rename table.
        for (int i = 0; i < RENAME_TABLE_COUNT; ++i) {
            primaryTableName = renameTable(primaryTableName);
        }

        // Rename GSI.
        for (int i = 0; i < RENAME_GSI_COUNT; ++i) {
            renameGsi(primaryTableName);
        }

        // Drop column.
        for (int i = 0; i < DROP_COLUMN_COUNT; ++i) {
            randomDropColumn(primaryTableName);
        }

        // Drop index.
        for (int i = 0; i < DROP_INDEX_COUNT; ++i) {
            randomDropGsi(primaryTableName);
        }

        // Add column.
        for (int i = 0; i < ADD_COLUMN_COUNT; ++i) {
            randomAddColumn(primaryTableName);
        }

        // Done, then drop table.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
    }

    @Test
    public void test_createSimple_createGSI_dropGSI_dropTable() throws Exception {
        createSimpleTable();

        final int check_times = 3;

        for (int i = 0; i < check_times; ++i) {
            // Create GSI.
            String primarySk = TableConstant.C_ID;
            String indexSk =
                TableConstant.C_BIGINT_64_UN; // FULL_TYPE_TABLE_COLUMNS.get(ThreadLocalRandom.current().nextInt(FULL_TYPE_TABLE_COLUMNS.size()));
            String covering = TableConstant.FULL_TYPE_TABLE_COLUMNS.stream()
                // do not support covering column with default current_timestamp
                .filter(column -> !column.equalsIgnoreCase(TableConstant.C_TIMESTAMP))
                .filter(column -> !column.equalsIgnoreCase(primarySk))
                .filter(column -> !column.equalsIgnoreCase(indexSk))
                .collect(Collectors.joining(", "));
            String partition = MessageFormat.format(HASH_PARTITIONING_TEMPLATE, indexSk, indexSk);

            String sql = GSI_ALLOW_ADD_HINT + MessageFormat
                .format(CREATE_GSI_TEMPLATE, gsiIndexTableName, gsiPrimaryTableName, indexSk, covering, partition);
            System.out.println("create GSI: " + sql);

            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // Check.
            SqlCreateTable createTable = showCreateTable2(tddlConnection, gsiPrimaryTableName);
            Assert.assertTrue(createTable.createGsi());
            Assert.assertEquals(1, createTable.getGlobalKeys().size());
            Assert.assertEquals(gsiIndexTableName, createTable.getGlobalKeys().get(0).left.getLastName());
            Assert.assertEquals(1, createTable.getGlobalKeys().get(0).right.getColumns().size());
            Assert.assertEquals(indexSk,
                createTable.getGlobalKeys().get(0).right.getColumns().get(0).getColumnName().getLastName());
            // Covering will auto add pk of primary table.
            String coveringReal = TableConstant.FULL_TYPE_TABLE_COLUMNS.stream()
                // do not support covering column with default current_timestamp
                .filter(column -> !column.equalsIgnoreCase(TableConstant.C_TIMESTAMP))
                .filter(column -> !column.equalsIgnoreCase(indexSk))
                .collect(Collectors.joining(", "));
            Assert.assertEquals(coveringReal, createTable.getGlobalKeys().get(0).right.getCovering().stream()
                .map(name -> name.getColumnName().getLastName()).collect(Collectors.joining(", ")));

            // Now drop it.
            sql = MessageFormat.format(DROP_GSI_TEMPLATE, gsiIndexTableName, gsiPrimaryTableName);
            System.out.println("drop GSI: " + sql);

            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Done, then drop table.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiPrimaryTableName);
    }

}
