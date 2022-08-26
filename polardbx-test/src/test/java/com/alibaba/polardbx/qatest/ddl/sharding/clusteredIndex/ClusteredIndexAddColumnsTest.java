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

package com.alibaba.polardbx.qatest.ddl.sharding.clusteredIndex;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author lijiu
 */
public class ClusteredIndexAddColumnsTest extends DDLBaseNewDBTestCase {

    private boolean supportXA = false;

    @Before
    public void cleanEnv() throws SQLException {

        supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    @After
    public void clean() {

    }

    private void checkTablesAndColumns(Connection tddlConnection, String tableName,
                                       Set<String> indexNames, Map<String, Set<String>> indexColumns,
                                       Map<String, Set<String>> coveringColumns) throws SQLException {

        // check index / covering columns exists
        try (final ResultSet resultSet = JdbcUtil
            .executeQuery("SHOW CREATE TABLE " + quoteSpecialName(tableName), tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            final String createTable = resultSet.getString(2);
            Assert.assertTrue(TStringUtil.contains(createTable, "CLUSTERED INDEX")
                || TStringUtil.contains(createTable, "UNIQUE CLUSTERED"));

            for (Entry<String, Set<String>> entry : indexColumns.entrySet()) {
                for (String indexColumn : entry.getValue()) {
                    Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain index column "
                        + indexColumn, TStringUtil.countMatches(createTable, indexColumn) > 2);
                }
            }
        }

        for (String indexName : indexNames) {
            try (final ResultSet resultSet = JdbcUtil
                .executeQuery("SHOW CREATE TABLE " + quoteSpecialName(indexName), tddlConnection)) {
                Assert.assertTrue(resultSet.next());
                final String createTable = resultSet.getString(2);
                Assert.assertFalse("Incorrect SHOW CREATE TABLE result " + createTable,
                    TStringUtil.contains(createTable, "CLUSTERED INDEX")
                        || TStringUtil.contains(createTable, "UNIQUE CLUSTERED"));
                for (String indexColumn : indexColumns.get(indexName)) {
                    Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain index column "
                        + indexColumn, TStringUtil.contains(createTable, indexColumn));
                }

                if (!coveringColumns.isEmpty()) {
                    for (String coveringColumn : indexColumns.get(indexName)) {
                        Assert.assertTrue("Incorrect SHOW CREATE TABLE result, does not contain covering column "
                            + coveringColumn, TStringUtil.contains(createTable, coveringColumn));
                    }
                }
            }
        }

        try (final ResultSet resultSet = JdbcUtil
            .executeQuery("SHOW INDEX FROM " + quoteSpecialName(tableName), tddlConnection)) {
            final Map<String, Set<String>> resultIndexColumnMap = new HashMap<>();
            final Map<String, Set<String>> resultCoveringColumnMap = new HashMap<>();

            Assert.assertTrue(resultSet.next());
            do {

                final String indexName = resultSet.getString(3);
                final String comment = resultSet.getString(12);

                if (!TStringUtil.equalsIgnoreCase("INDEX", comment)
                    && !TStringUtil.equalsIgnoreCase("COVERING", comment)) {
                    continue;
                }

                final String indexType = resultSet.getString(11);
                final String columnName = resultSet.getString(5);

                Assert.assertEquals(resultSet.getString(1), tableName);
                Assert.assertTrue("Unexpected index name " + indexName, indexNames.contains(indexName));
                Assert.assertEquals("GLOBAL", indexType);

                final Set<String> resultIndexColumns = resultIndexColumnMap.computeIfAbsent(indexName,
                    s -> new HashSet<>());
                final Set<String> resultCoveringColumns = resultCoveringColumnMap.computeIfAbsent(indexName,
                    s -> new HashSet<>());

                if (TStringUtil.equalsIgnoreCase("INDEX", comment)) {
                    resultIndexColumns.add(columnName);
                } else if (TStringUtil.equalsIgnoreCase("COVERING", comment)) {
                    resultCoveringColumns.add(columnName);
                }

                Assert.assertTrue("Unexpected index columns " + TStringUtil.join(resultIndexColumns, ","),
                    indexColumns.get(indexName).containsAll(resultIndexColumns));
                if (!coveringColumns.isEmpty()) {
                    Assert.assertTrue("Unexpected index columns " + TStringUtil.join(resultCoveringColumns, ","),
                        coveringColumns.get(indexName).containsAll(resultCoveringColumns));
                }
            } while (resultSet.next());

            for (Entry<String, Set<String>> entry : indexColumns.entrySet()) {
                final String indexName = entry.getKey();
                final Set<String> indexColumnSet = entry.getValue();
                Assert.assertTrue("Unexpected index columns " + TStringUtil.join(indexColumnSet, ","),
                    indexColumnSet.equals(resultIndexColumnMap.get(indexName)));

            }
            if (!coveringColumns.isEmpty()) {
                for (Entry<String, Set<String>> entry : coveringColumns.entrySet()) {
                    final String indexName = entry.getKey();
                    final Set<String> coveringColumnSet = entry.getValue();
                    Assert.assertEquals(coveringColumnSet, resultCoveringColumnMap.get(indexName));
                }
            }

        }

    }

    /**
     * Assert that data in all tables are the same, even if columns in index
     * tables may be less than columns in the base table.
     */
    private void assertTablesSame(List<String> tableNames) {
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;
        String tableToCompare = tableNames.get(0);
        List<List<Object>> resultsToCompare = null;
        List<String> columnsToCompare = null;

        for (String tableName : tableNames) {
            String sql = "select * from " + tableName;
            try {
                tddlPs = JdbcUtil.preparedStatementSet(sql, Lists.newArrayList(), tddlConnection);
                tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
                List<List<Object>> results = JdbcUtil.getAllResult(tddlRs);
                List<String> columns = JdbcUtil.getColumnNameListToLowerCase(tddlRs);
                if (resultsToCompare == null) {
                    resultsToCompare = results;
                    columnsToCompare = columns;
                    continue;
                }

                List<Integer> columnIndexes = columns.stream()
                    .map(columnsToCompare::indexOf)
                    .collect(Collectors.toList());
                List<List<Object>> pickedResults = resultsToCompare.stream()
                    .map(list -> columnIndexes.stream().map(list::get).collect(Collectors.toList()))
                    .collect(Collectors.toList());

                assertWithMessage("Table contents are not the same, tables are " + tableToCompare + " and " + tableName)
                    .that(results)
                    .containsExactlyElementsIn(pickedResults);

                tddlPs = null;
                tddlRs = null;
            } finally {
                if (tddlPs != null) {
                    JdbcUtil.close(tddlPs);
                }
                if (tddlRs != null) {
                    JdbcUtil.close(tddlRs);
                }
            }
        }
    }

    @Test
    public void test1AddColumn() {

        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`seller_id`) dbpartition by hash(`seller_id`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`buyer_id`) dbpartition by hash(`buyer_id`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);
            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "order_id", "buyer_id", "order_snapshot"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "order_id", "seller_id", "order_snapshot")));

            final String AddColumn =
                MessageFormat.format("alter table {0} add column c1 varchar(30)", gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "order_id", "buyer_id", "order_snapshot", "c1"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "order_id", "seller_id", "order_snapshot", "c1")));

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    @Ignore("Add multiple columns is denied when with clustered index")
    public void test2AddColumns() {

        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "gsi_test_table";
        final String gsiTestKeyName = "g_i_test_seller";
        final String gsiTestUkName = "g_i_test_buyer";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`seller_id`) dbpartition by hash(`seller_id`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`buyer_id`) dbpartition by hash(`buyer_id`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);
            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "order_id", "buyer_id", "order_snapshot"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "order_id", "seller_id", "order_snapshot")));

            final String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c1 varchar(30) default null,"
                        + " add column c2 bigint(10)", gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("seller_id"),
                    gsiTestUkName,
                    ImmutableSet.of("buyer_id")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("id", "order_id", "buyer_id", "order_snapshot", "c1", "c2"),
                    gsiTestUkName,
                    ImmutableSet.of("id", "order_id", "seller_id", "order_snapshot", "c1", "c2")));

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void test3AddDefaultColumn() {
        Assume.assumeTrue(supportXA);

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            final String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 bigint(10) default 20,", gsiTestTableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void test4AddDefaultCurrentTimestampColumn() {
        Assume.assumeTrue(supportXA);

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            final String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 timestamp default current_timestamp,", gsiTestTableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void test5AddDefaultCurrentTimestamp3Column() {
        Assume.assumeTrue(supportXA);

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            final String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 timestamp(3) default current_timestamp(3);",
                        gsiTestTableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    private void gsiIntegrityCheck(String index) {
        final String CHECK_HINT =
            "/*+TDDL: cmd_extra(GSI_CHECK_PARALLELISM=4, GSI_CHECK_BATCH_SIZE=1024, GSI_CHECK_SPEED_LIMITATION=-1)*/";
        final ResultSet rs = JdbcUtil
            .executeQuery(CHECK_HINT + "check global index " + index, tddlConnection);
        List<String> result = JdbcUtil.getStringResult(rs, false)
            .stream()
            .map(row -> row.get(row.size() - 1))
            .collect(Collectors.toList());
        System.out.println("Checker: " + result.get(result.size() - 1));
        Assert.assertTrue(result.get(result.size() - 1).contains("OK"));
    }

    @Test
    public void test6AddDefaultTimeColumnForDatetime() {
        Assume.assumeTrue(supportXA);

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 datetime default current_timestamp,",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c6 datetime default current_timestamp(),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c7 datetime default now(),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c8 datetime default localtime,",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c9 datetime default localtime(),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c10 datetime default localtimestamp,",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c11 datetime default localtimestamp(),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            gsiIntegrityCheck(gsiTestKeyName);
            gsiIntegrityCheck(gsiTestUkName);

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void test6AddDefaultTimeColumnForDatetime3() {
        Assume.assumeTrue(supportXA);

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 datetime(3) default current_timestamp(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c6 datetime(3) default now(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c7 datetime(3) default localtime(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c8 datetime(3) default localtimestamp(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5", "c6", "c7", "c8"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5", "c6", "c7", "c8")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            gsiIntegrityCheck(gsiTestKeyName);
            gsiIntegrityCheck(gsiTestUkName);

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void test7AddDefaultTimeColumnForTimestamp3() {
        Assume.assumeTrue(supportXA);

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 timestamp(3) default current_timestamp(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c6 timestamp(3) default now(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c7 timestamp(3) default localtime(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c8 timestamp(3) default localtimestamp(3),",
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5", "c6", "c7", "c8"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5", "c6", "c7", "c8")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            gsiIntegrityCheck(gsiTestKeyName);
            gsiIntegrityCheck(gsiTestUkName);

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void test8AddDefaultTimeColumnForFirstTimestamp3() {

        if (!supportXA) {
            return;
        }

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 timestamp(3),", // This with implicit default current.
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c6 timestamp(3) null,", // Can null.
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5", "c6"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5", "c6")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            gsiIntegrityCheck(gsiTestKeyName);
            gsiIntegrityCheck(gsiTestUkName);

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

    @Test
    public void test9AddDefaultTimeColumnWithAdditionalOnUpdateColumn() {
        Assume.assumeTrue(supportXA);

        final String gsiTestTableName = "clustered_primary_table";
        final String gsiTestKeyName = "clustered_index";
        final String gsiTestUkName = "clustered_unique_index";
        final String gsiTestTable = HINT_CREATE_GSI
            + "CREATE TABLE `"
            + gsiTestTableName
            + "` (\n"
            + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(11) DEFAULT NULL,\n"
            + "\t`c2` bigint(11) DEFAULT NULL,\n"
            + "\t`c3` bigint(11) DEFAULT NULL,\n"
            + "\t`c4` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`c1`);\n";
        final String gsiTestKeyTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE CLUSTERED INDEX {1} ON {0} (`c2`) dbpartition by hash(`c2`);",
            gsiTestTableName, gsiTestKeyName);
        final String gsiTestUkTable = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE CLUSTERED INDEX {1} ON {0} (`c3`) dbpartition by hash(`c3`);",
            gsiTestTableName, gsiTestUkName);

        try {
            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestTable);

            StringBuilder insertValues =
                new StringBuilder(MessageFormat.format("INSERT INTO {0} (`c1`, `c2`, `c3`) VALUES ",
                    gsiTestTableName));

            for (int i = 0; i < 100; i++) {
                insertValues.append(MessageFormat.format("({0}, {1}, {2})", i, i, i));
                if (i != 99) {
                    insertValues.append(",");
                }
            }
            insertValues.append(";");

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertValues.toString());

            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestKeyTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiTestUkTable);

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            String AddColumn =
                MessageFormat
                    .format("alter table {0} add column c5 timestamp(3),", // This with implicit default current.
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            AddColumn =
                MessageFormat
                    .format("alter table {0} add column c6 timestamp(3) default now(3) on update current_timestamp(3),",
                        // The backfill should also update the previous one.
                        gsiTestTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, AddColumn);
            System.out.println("done " + AddColumn);

            checkTablesAndColumns(tddlConnection,
                gsiTestTableName,
                ImmutableSet.of(gsiTestKeyName, gsiTestUkName),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("c2"),
                    gsiTestUkName,
                    ImmutableSet.of("c3")),
                ImmutableMap.of(gsiTestKeyName,
                    ImmutableSet.of("pk", "c1", "c3", "c4", "c5", "c6"),
                    gsiTestUkName,
                    ImmutableSet.of("pk", "c1", "c2", "c4", "c5", "c6")));

            assertTablesSame(ImmutableList.of(gsiTestTableName, gsiTestKeyName, gsiTestUkName));

            gsiIntegrityCheck(gsiTestKeyName);
            gsiIntegrityCheck(gsiTestUkName);

            dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestKeyName, gsiTestUkName));
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }

    }

}
