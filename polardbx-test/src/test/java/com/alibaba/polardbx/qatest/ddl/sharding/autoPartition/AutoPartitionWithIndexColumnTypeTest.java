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

package com.alibaba.polardbx.qatest.ddl.sharding.autoPartition;

import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.GsiConstant.COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.PK_COLUMNS;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * @version 1.0
 */

public class AutoPartitionWithIndexColumnTypeTest extends AutoPartitionTestBase {
    public static final String TABLE_NAME = "auto_partition_idx_tb";
    public static final String INDEX_NAME = "ap_index";

    private static final String CREATE_TEMPLATE = "CREATE PARTITION TABLE {0} ({1} {2})";
    private static final String INSERT_TEMPLATE = "INSERT INTO {0}({1}) VALUES({2})";
    private static final ImmutableMap<String, List<String>> COLUMN_VALUES = GsiConstant.buildGsiFullTypeTestValues();

    private final String indexColumnRaw;
    private final String indexColumn;
    private final String columnDef;

    public AutoPartitionWithIndexColumnTypeTest(String indexColumn) {
        this.indexColumnRaw = indexColumn;
        this.indexColumn = indexColumn.contains("_text") ? "`" + indexColumn + "`(64)" : "`" + indexColumn + "`";
        this.columnDef = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(c -> !c.equalsIgnoreCase("id")) // Manually add it in case.
            .map(c -> COLUMN_DEF_MAP.get(c))
            .collect(Collectors.joining());
    }

    @Parameterized.Parameters(name = "{index}:indexColumn={0}")
    public static List<String[]> prepareDate() {
        // Use all pk possible column as index column.
        return PK_COLUMNS.stream()
            .filter(c -> !c.contains("_bit_"))
            .filter(c -> !c.contains("_decimal"))
            .filter(c -> !c.contains("_float"))
            .filter(c -> !c.contains("_double"))
            .filter(c -> !c.contains("_time"))
            .filter(c -> !c.contains("_year"))
            .filter(c -> !c.contains("_blob"))
            .filter(c -> !c.contains("_text"))
            .filter(c -> !c.contains("_enum"))
            .filter(c -> !c.contains("_set"))
            .filter(c -> !c.contains("_binary"))
            .filter(c -> !c.contains("_varbinary"))
            .map(c -> new String[] {c})
            .collect(Collectors.toList());
    }

    @Before
    public void before() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @After
    public void after() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    private void insertAndAssertSame(final String indexPrefix) throws Exception {
        // Try insert.
        List<String> failedList = new ArrayList<>();
        for (String val : COLUMN_VALUES.get(indexColumnRaw)) {

            final String insertSql = MessageFormat.format(INSERT_TEMPLATE, TABLE_NAME, indexColumnRaw, val);

            // TODO: Known problem and junqi will fix later.
            if (indexColumnRaw.contains("datetime") && val.contains("0000-00-00 01:01:01")) {
                continue;
            }

            Statement ps = null;
            try {
                ps = tddlConnection.createStatement();
                ps.executeUpdate(insertSql);
            } catch (SQLSyntaxErrorException msee) {
                throw msee;
            } catch (SQLException e) {
                // ignore exception
                failedList.add(MessageFormat
                    .format("col[{0}] value[{1}] error[{2}]", indexColumn, String.valueOf(val), e.getMessage()));
            } finally {
                JdbcUtil.close(ps);
            }
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(System.out::println);

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + TABLE_NAME,
            tddlConnection);
        assertThat(resultSet.next(), is(true));
        final long rowCount = resultSet.getLong(1);
        assertThat(rowCount, greaterThan(0L));

        final String selectGSI =
            MessageFormat
                .format("select {2} from `{0}` force index({1})", TABLE_NAME, indexPrefix + INDEX_NAME, indexColumnRaw);
        final String selectPrimary = MessageFormat
            .format("select {2} from `{0}` ignore index({1})", TABLE_NAME, indexPrefix + INDEX_NAME, indexColumnRaw);

        // Assert correct plan.
        Assert.assertFalse(getExplainResult(tddlConnection, selectPrimary).contains("IndexScan"));
        Assert.assertTrue(getExplainResult(tddlConnection, selectGSI).contains("IndexScan"));

        // Assert that data equal.
        selectContentSameAssert(selectPrimary, selectGSI, null, tddlConnection, tddlConnection);
    }

    @Test
    public void explicitPkIndexToGlobalTest() throws Exception {

        if (indexColumnRaw.equals("id")) {
            return; // Ignore pk 'id' and add index id.
        }

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`), index `{0}` ({1})", INDEX_NAME,
            indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that no local just gsi.
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat
                .format("GLOBAL INDEX `{0}`({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void implicitPkIndexToGlobalTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) DEFAULT NULL, index `{0}` ({1})", INDEX_NAME, indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that no local and global
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("GLOBAL INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void explicitPkUniqueIndexToUniqueGlobalTest() throws Exception {

        if (indexColumnRaw.equals("id")) {
            return; // Ignore pk 'id' and add index id.
        }

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`), unique index `{0}` ({1})", INDEX_NAME,
            indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that no local just global.
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat
                .format("UNIQUE GLOBAL KEY `{0}` ({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void implicitPkUniqueIndexToUniqueGlobalTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) DEFAULT NULL, unique index `{0}` ({1})", INDEX_NAME, indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that no local just global.
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat
                .format("UNIQUE GLOBAL KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void explicitPkGlobalAutoPartitionTest() throws Exception {

        if (indexColumnRaw.equals("id")) {
            return; // Ignore pk 'id' and add index id.
        }

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`), global index `{0}` ({1})", INDEX_NAME,
            indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat
                .format("GLOBAL INDEX `{0}`({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void implicitPkGlobalAutoPartitionTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) DEFAULT NULL, global index `{0}` ({1})", INDEX_NAME, indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("GLOBAL INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void explicitPkClusteredAutoPartitionTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`), clustered index `{0}` ({1})", INDEX_NAME,
            indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("CLUSTERED INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void implicitPkClusteredAutoPartitionTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) DEFAULT NULL, clustered index `{0}` ({1})", INDEX_NAME, indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("CLUSTERED INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void explicitPkUniqueGlobalAutoPartitionTest() throws Exception {

        if (indexColumnRaw.equals("id")) {
            return; // Ignore pk 'id' and add index id.
        }

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`), unique global index `{0}` ({1})",
            INDEX_NAME,
            indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat
                    .format("UNIQUE GLOBAL KEY `{0}` ({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void implicitPkUniqueGlobalAutoPartitionTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) DEFAULT NULL, unique global index `{0}` ({1})", INDEX_NAME, indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE GLOBAL KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void explicitPkUniqueClusteredAutoPartitionTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`), unique clustered index `{0}` ({1})",
            INDEX_NAME,
            indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE CLUSTERED KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    @Test
    public void implicitPkUniqueClusteredAutoPartitionTest() throws Exception {

        final String additionalDef = MessageFormat.format(
            "`id` bigint(20) DEFAULT NULL, unique clustered index `{0}` ({1})", INDEX_NAME, indexColumn);
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE CLUSTERED KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");
    }

    // Alter table add index.

    @Test
    public void explicitPkAddGlobalIndexAutoPartitionTest() throws Exception {

        if (indexColumnRaw.equals("id")) {
            return; // Ignore pk 'id' and add index id.
        }

        final String additionalDef =
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`)";
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Global.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add global index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat
                .format("GLOBAL INDEX `{0}`({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique global.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add unique global index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat
                    .format("UNIQUE GLOBAL KEY `{0}` ({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Try with create index.

        // Global.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create global index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat
                .format("GLOBAL INDEX `{0}`({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique global.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create unique global index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat
                    .format("UNIQUE GLOBAL KEY `{0}` ({1}) COVERING (`id`) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));
    }

    @Test
    public void implictPkAddGlobalIndexAutoPartitionTest() throws Exception {

        final String additionalDef =
            "`id` bigint(20) DEFAULT NULL";
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Global.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add global index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("GLOBAL INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique global.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add unique global index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE GLOBAL KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Try with create index.

        // Clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create global index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("GLOBAL INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique global.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create unique global index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only global)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE GLOBAL KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));
    }

    @Test
    public void explicitPkAddClusteredIndexAutoPartitionTest() throws Exception {

        final String additionalDef =
            "`id` bigint(20) NOT NULL AUTO_INCREMENT, primary key (`id`)";
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add clustered index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("CLUSTERED INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add unique clustered index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE CLUSTERED KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Try with create index.

        // Clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create clustered index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("CLUSTERED INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create unique clustered index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE CLUSTERED KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));
    }

    @Test
    public void implictPkAddClusteredIndexAutoPartitionTest() throws Exception {

        final String additionalDef =
            "`id` bigint(20) DEFAULT NULL";
        final String createTable =
            MessageFormat.format(CREATE_TEMPLATE, TABLE_NAME, columnDef, additionalDef);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add clustered index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("CLUSTERED INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` add unique clustered index `{1}` ({2})", TABLE_NAME, INDEX_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE CLUSTERED KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Try with create index.

        // Clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create clustered index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("CLUSTERED INDEX `{0}`({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));

        // Unique clustered.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("create unique clustered index `{0}` on `{1}` ({2})", INDEX_NAME, TABLE_NAME, indexColumn));

        // Assert that 1 index(only clustered)
        Assert.assertFalse(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(MessageFormat.format("UNIQUE KEY `{0}` ({1})", INDEX_NAME, indexColumn)));
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME)
            .contains(
                MessageFormat.format("UNIQUE CLUSTERED KEY `{0}` ({1}) DBPARTITION BY ", INDEX_NAME, indexColumn)));

        insertAndAssertSame("");

        // Clear.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("alter table `{0}` drop index `{1}`", TABLE_NAME, INDEX_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from `{0}` where 1=1", TABLE_NAME));
    }

}
