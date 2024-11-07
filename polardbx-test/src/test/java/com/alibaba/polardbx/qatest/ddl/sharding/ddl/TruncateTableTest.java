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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.gms.metadb.limit.Limits;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.weakref.jmx.internal.guava.collect.ImmutableList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class TruncateTableTest extends DDLBaseNewDBTestCase {

    private String testTableName = "truncate_test";
    private final String gsiPrimaryTableName = "truncate_gsi_test";
    private final String gsiIndexTableName = "g_i_truncate_test";
    private final String gsiTruncateHint = "/*+TDDL:cmd_extra(TRUNCATE_TABLE_WITH_GSI=true)*/";
    private final String gsiDisableStorageCheckHint = "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false)*/";

    private boolean supportXA = false;

    public TruncateTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    @Before
    public void before() throws SQLException {
        supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateBroadCastTable() {
        String tableName = schemaPrefix + testTableName + "_1";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + "(id int primary key auto_increment, name varchar(20))broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = "truncate table " + tableName;
//        Assert.assertEquals(getNodeNum(tddlConnection), getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        sql = "insert into " + tableName + " (id, name) values (null, \"tom\"), (null, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        List<Integer> nums = getAllDataNumFromTable(tddlConnection, tableName, "id");
        nums = nums.stream().map(n -> n % 100).collect(Collectors.toList());
        Assert.assertTrue(ImmutableList.of(1, 2).equals(nums));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateSingleTable() {
        String tableName = schemaPrefix + testTableName + "_2";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + " (id int primary key auto_increment by group, name varchar(20))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        sql = "insert into " + tableName + " (id, name) values (null, \"tom\"), (null, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        List<Integer> nums = getAllDataNumFromTable(tddlConnection, tableName, "id");
        nums = nums.stream().map(n -> n % 100).collect(Collectors.toList());
        Assert.assertTrue(ImmutableList.of(1, 2).equals(nums));

        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardTbTable() {
        String tableName = schemaPrefix + testTableName + "_3";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName
            + " (id int primary key auto_increment, name varchar(20)) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = "truncate table " + tableName;
//        Assert.assertEquals(2, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        sql = "insert into " + tableName + " (id, name) values (null, \"tom\"), (null, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        List<Integer> nums = getAllDataNumFromTable(tddlConnection, tableName, "id");
        nums = nums.stream().map(n -> n % 100).collect(Collectors.toList());
        Assert.assertTrue(ImmutableList.of(1, 2).equals(nums));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTable() {
        String tableName = schemaPrefix + testTableName + "_4";
        dropTableIfExists(tableName);

        // 清除表
        String sql = "drop table if exists " + tableName + ";";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertNotExistsTable(tableName, tddlConnection);

        sql = "create table " + tableName + " (id int, name varchar(20)) dbpartition by hash (id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTbTable() {
        String tableName = schemaPrefix + testTableName + "_5";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = "truncate table " + tableName;
//        Assert.assertEquals(8, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTableWithGsi() throws SQLException {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }

        String tableName = schemaPrefix + gsiPrimaryTableName + "_6";
        String indexTableName1 = schemaPrefix + gsiIndexTableName + "_6_1";
        String indexTableName2 = schemaPrefix + gsiIndexTableName + "_6_2";
        String indexTableName3 = schemaPrefix + gsiIndexTableName + "_6_3";
        String indexTableName4 = schemaPrefix + gsiIndexTableName + "_6_4";
        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName1);
        dropTableIfExists(indexTableName2);
        dropTableIfExists(indexTableName3);
        dropTableIfExists(indexTableName4);

        String sql = gsiDisableStorageCheckHint + "create table " + tableName
            + " (id int primary key auto_increment, "
            + "name varchar(20), "
            + "global index " + indexTableName1 + " (name) dbpartition by hash(name),"
            + "global unique index " + indexTableName2 + " (name) dbpartition by hash(name),"
            + "clustered index " + indexTableName3 + " (name) dbpartition by hash(name),"
            + "clustered unique index " + indexTableName4 + " (name) dbpartition by hash(name)"
            + ") dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        } else {
            sql = gsiDisableStorageCheckHint + "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName1));
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName2));
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName3));
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName4));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, indexTableName1));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, indexTableName2));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, indexTableName3));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, indexTableName4));

        if (supportXA) {
            sql = "insert into " + tableName
                + " (id, name) values (null, \"a\"), (null, \"b\") , (null, \"c\"), (null, \"d\"), (null, \"e\"), (null, \"f\"), (null, \"g\"), (null, \"h\")";
        } else {
            sql = gsiDisableStorageCheckHint + "insert into " + tableName
                + " (id, name) values (null, \"a\"), (null, \"b\") , (null, \"c\"), (null, \"d\"), (null, \"e\"), (null, \"f\"), (null, \"g\"), (null, \"h\")";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));

        List<Integer> nums = getAllDataNumFromTable(tddlConnection, tableName, "id");
        nums = nums.stream().map(n -> n % 100).collect(Collectors.toList());
        Assert.assertTrue(ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8).equals(nums));

        checkGsi(tddlConnection, indexTableName1);
        checkGsi(tddlConnection, indexTableName2);
        checkGsi(tddlConnection, indexTableName3);
        checkGsi(tddlConnection, indexTableName4);

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName1);
        dropTableIfExists(indexTableName2);
        dropTableIfExists(indexTableName3);
        dropTableIfExists(indexTableName4);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTableWithGsiShadowTestTableHint() {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }

        String testPrefixHint = "/* //1/ */";
        String testTablePrefix = "__test_";

        String tableName = schemaPrefix + gsiPrimaryTableName + "_7";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_7";

        String tableNameWithPrefix = schemaPrefix + testTablePrefix + gsiPrimaryTableName + "_7";
        String indexTableNameWithPrefix = schemaPrefix + testTablePrefix + gsiIndexTableName + "_7";

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        dropTableIfExists(tableNameWithPrefix);
        dropTableIfExists(indexTableNameWithPrefix);

        String sql = gsiDisableStorageCheckHint + "create table " + tableName
            + " (id int primary key, name varchar(20), global index "
            + indexTableName
            + " (name) dbpartition by hash(name)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = gsiDisableStorageCheckHint + "create shadow table " + tableNameWithPrefix
            + " (id int primary key, name varchar(20), global index "
            + indexTableNameWithPrefix
            + " (name) dbpartition by hash(name)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = testPrefixHint + gsiTruncateHint + "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        dropTableIfExists(tableNameWithPrefix);
        dropTableIfExists(indexTableNameWithPrefix);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTableWithGsiLongTableName() throws SQLException {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }

        String tableName = schemaPrefix + gsiPrimaryTableName + "_8";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_8";

        tableName = StringUtils.rightPad(tableName, Limits.MAX_LENGTH_OF_LOGICAL_TABLE_NAME, 'P');
        indexTableName = StringUtils.rightPad(indexTableName, Limits.MAX_LENGTH_OF_INDEX_NAME, 'P');

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        String sql = gsiDisableStorageCheckHint + "create table " + tableName
            + " (id int primary key, name varchar(20), global index "
            + indexTableName
            + " (name) dbpartition by hash(name)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        } else {
            sql = gsiDisableStorageCheckHint + "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, indexTableName));

        if (supportXA) {
            sql = "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        } else {
            sql = gsiDisableStorageCheckHint + "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, indexTableName);

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTbTableWithGsi() throws SQLException {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }

        String tableName = schemaPrefix + gsiPrimaryTableName + "_9";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_9";
        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        String sql = gsiDisableStorageCheckHint + "create table "
            + tableName
            + " (id int primary key, name varchar(20), global index "
            + indexTableName
            + " (name) dbpartition by hash(name) tbpartition by hash(name) tbpartitions 2) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        } else {
            sql = gsiDisableStorageCheckHint + "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(8, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, indexTableName));

        if (supportXA) {
            sql = "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        } else {
            sql = gsiDisableStorageCheckHint + "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, indexTableName);

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    /**
     * @since 5.4.12
     */
    @Test
    public void testTruncateShardDbTbTableWithGsiWhileInsert() throws Exception {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + gsiPrimaryTableName + "_10";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_10";
        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
        String sql = gsiDisableStorageCheckHint + "create table "
            + tableName
            + " (id int primary key, name int, global index "
            + indexTableName
            + " (name) dbpartition by hash(name) tbpartition by hash(name) tbpartitions 2) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        List<Thread> threads = new ArrayList<>();
        List<Exception> exceptions = new ArrayList<>();
        List<AssertionError> errors = new ArrayList<>();
        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Thread insert_thread = new Thread(new Runnable() {
            public void run() {
                Connection connection = null;
                try {
                    connection = getPolardbxDirectConnection();
                    int count = 0;
                    while (!shouldStop.get()) {
                        String sql = String.format("insert into table %s values (%d, %d)", tableName, count, count);
                        if (!supportXA) {
                            sql = gsiDisableStorageCheckHint + sql;
                        }
                        JdbcUtil.executeUpdateSuccess(connection, sql);
                        count += 1;
                    }
                } catch (Exception e) {
                    synchronized (this) {
                        exceptions.add(e);
                    }
                } catch (AssertionError ae) {
                    synchronized (this) {
                        errors.add(ae);
                    }
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        insert_thread.start();
        threads.add(insert_thread);
        Thread alter_thread = new Thread(new Runnable() {
            public void run() {
                Connection connection = null;
                try {
                    connection = getPolardbxDirectConnection();
                    String sql = "truncate table " + tableName;
                    JdbcUtil.executeUpdateSuccess(connection, sql);
                } catch (Exception e) {
                    synchronized (this) {
                        exceptions.add(e);
                    }
                } catch (AssertionError ae) {
                    synchronized (this) {
                        errors.add(ae);
                    }
                } finally {
                    shouldStop.set(true);
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        alter_thread.start();
        threads.add(alter_thread);
        for (Thread thread : threads) {
            thread.join();
        }
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
        Assert.assertEquals(getDataNumFromTable(tddlConnection, indexTableName),
            getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, indexTableName);
        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    @Test
    public void testTruncateShardDbTbTableWithGsiBinaryDefaultValue() throws Exception {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + gsiPrimaryTableName + "_11";
        String gsiName = schemaPrefix + gsiIndexTableName + "_11";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTable = String.format("create table %s ("
            + "`pk` int primary key auto_increment, "
            + "`bin_col` varbinary(20) default x'0A08080E10011894AB0E', "
            + "`pad` varchar(20) default 'ggg' "
            + ")", tableName);
        String partitionDef = " dbpartition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String createGsi =
            String.format("create global index %s on %s(`pk`) dbpartition by hash(`pk`)", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        // Use upsert to test default value on CN
        String upsert = String.format("insert into %s(`pk`) values (null) on duplicate key update pad=null", tableName);
        // Use insert to test default value on DN
        String insert = String.format("insert into %s(`pk`) values (null)", tableName);
        String select = String.format("select `bin_col` from %s", tableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        String truncate = String.format("truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, truncate);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testTableNoPk() throws SQLException {
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + testTableName + "_12";
        String gsiName = schemaPrefix + gsiIndexTableName + "_12";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String sql = "create table " + tableName
            + " (id int,"
            + "name varchar(20),"
            + "global index " + gsiName + "(name) dbpartition by hash(name)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, gsiName);

        sql = "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, gsiName);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, gsiName);
    }

    @Test
    public void testTableWithGeneratedColumn() {
        if (crossSchema) {
            return;
        }

        String tableName = schemaPrefix + testTableName + "_12";
        String gsiName = schemaPrefix + gsiIndexTableName + "_12";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        dropTableIfExistsInMySql(tableName);

        // create table
        String createTable = "create table " + tableName
            + " (a int,"
            + " b int,"
            + " c int as (a-b) logical,"
            + " d int as (a+b) virtual,"
            + "global index " + gsiName + "(c) dbpartition by hash(c)) dbpartition by hash(a)";
        String createTableMySQL = "create table " + tableName
            + " (a int,"
            + " b int,"
            + " c int as (a-b),"
            + " d int as (a+b))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableMySQL);

        String insert = String.format("insert into %s(a,b) values (1,2),(3,4)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String truncate = String.format("truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, truncate);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testTableWithRecycleBin() {
        if (crossSchema) {
            return;
        }

        String tableName = schemaPrefix + testTableName + "_13";
        String createTable = "create table " + tableName
            + " (a int,"
            + " b int"
            + ") dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        assertSequenceStartValue(tableName);

        String truncate = String.format("/*+TDDL:cmd_extra(ENABLE_RECYCLEBIN=true)*/truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);

        insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        assertSequenceStartValue(tableName);

        truncate = String.format("/*+TDDL:cmd_extra(ENABLE_RECYCLEBIN=true)*/truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);

        insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        assertSequenceStartValue(tableName);

        truncate = String.format("/*+TDDL:cmd_extra(ENABLE_RECYCLEBIN=true)*/truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);

        insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        assertSequenceStartValue(tableName);
    }

    @Test
    public void testTableWithRecycleBin2() {
        if (crossSchema) {
            return;
        }

        String tableName = schemaPrefix + testTableName + "_14";
        String createTable = "create table " + tableName
            + " (a int primary key,"
            + " b int"
            + ") dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String truncate = String.format("/*+TDDL:cmd_extra(ENABLE_RECYCLEBIN=true)*/truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);

        insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        truncate = String.format("/*+TDDL:cmd_extra(ENABLE_RECYCLEBIN=true)*/truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);

        insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        truncate = String.format("/*+TDDL:cmd_extra(ENABLE_RECYCLEBIN=true)*/truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncate);

        insert = String.format("insert into %s(a,b) values (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
    }

    void assertSequenceStartValue(String tableName) {
        String select = String.format("select _drds_implicit_id_ from %s", tableName);
        try (ResultSet rs = JdbcUtil.executeQuery(select, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getLong(1), 100001L);
        } catch (SQLException e) {
            Assert.fail();
        }
    }
}
