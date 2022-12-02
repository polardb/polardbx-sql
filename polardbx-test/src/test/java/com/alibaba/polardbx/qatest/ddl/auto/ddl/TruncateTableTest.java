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

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.gms.metadb.limit.Limits;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class TruncateTableTest extends DDLBaseNewDBTestCase {

    private String testTableName = "truncate_test";
    private final String gsiPrimaryTableName = "truncate_gsi_test";
    private final String gsiIndexTableName = "g_i_truncate_test";
    private final String gsiTruncateHint = "/*+TDDL:cmd_extra(TRUNCATE_TABLE_WITH_GSI=true)*/";

    public TruncateTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    @Before
    public void before() throws SQLException {
    }

    @Test
    public void testTruncateBroadCastTable() {
        String tableName = schemaPrefix + testTableName + "_1";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + "(id int, name varchar(20))broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = "truncate table " + tableName;
//        Assert.assertEquals(getNodeNum(tddlConnection), getExplainNum(sql));
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
    public void testTruncateSingleTable() {
        String tableName = schemaPrefix + testTableName + "_2";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + " (id int, name varchar(20))";
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

    @Test
    public void testTruncateShardDbTable() {
        String tableName = schemaPrefix + testTableName + "_4";
        dropTableIfExists(tableName);

        // 清除表
        String sql = "drop table if exists " + tableName + ";";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertNotExistsTable(tableName, tddlConnection);

        sql = "create table " + tableName + " (id int, name varchar(20)) partition by hash (id)";
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
            + " (id int, name varchar(20)) partition by hash (id)";
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

        String sql = "create table " + tableName
            + " (id int primary key, "
            + "name varchar(20), "
            + "global index " + indexTableName1 + " (name) partition by hash(name),"
            + "global unique index " + indexTableName2 + " (name) partition by hash(name),"
            + "clustered index " + indexTableName3 + " (name) partition by hash(name),"
            + "clustered unique index " + indexTableName4 + " (name) partition by hash(name)"
            + ") partition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName2));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName3));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName4));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName2));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName3));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName4));

        sql = "insert into " + tableName + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName2));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName3));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName4));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName1);
        dropTableIfExists(indexTableName2);
        dropTableIfExists(indexTableName3);
        dropTableIfExists(indexTableName4);
    }

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

        String sql = "create table " + tableName
            + " (id int primary key, name varchar(20), global index "
            + indexTableName
            + " (name) partition by hash(name)) partition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "create shadow table " + tableNameWithPrefix
            + " (id int primary key, name varchar(20), global index "
            + indexTableNameWithPrefix
            + " (name) partition by hash(name)) partition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = testPrefixHint + gsiTruncateHint + "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        dropTableIfExists(tableNameWithPrefix);
        dropTableIfExists(indexTableNameWithPrefix);
    }

    @Test
    public void testTruncateShardDbTableWithGsiLongTableName() throws SQLException {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }

        String tableName = schemaPrefix + gsiPrimaryTableName + "_8";
        String indexTableName1 = schemaPrefix + gsiIndexTableName;
        String indexTableName2 = schemaPrefix + gsiIndexTableName;

        tableName = StringUtils.rightPad(tableName, Limits.MAX_LENGTH_OF_LOGICAL_TABLE_NAME, 'P');
        indexTableName1 = StringUtils.rightPad(indexTableName1, Limits.MAX_LENGTH_OF_INDEX_NAME - 7, 'P') + "1";
        indexTableName2 = StringUtils.rightPad(indexTableName2, Limits.MAX_LENGTH_OF_INDEX_NAME - 7, 'P') + "2";

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName1);
        dropTableIfExists(indexTableName2);

        String sql = "create table " + tableName
            + " (id int primary key, name varchar(20), "
            + "global index " + indexTableName1 + " (name) partition by hash(name),"
            + "global index " + indexTableName2 + " (name) partition by hash(name)) partition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName2));

        sql = gsiTruncateHint + "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName2));

            sql = "insert into " + tableName
                + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName2));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName1);
        dropTableIfExists(indexTableName2);
    }

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

        String sql = "create table "
            + tableName
            + " (id int primary key, name varchar(20), global index "
            + indexTableName
            + " (name) partition by hash(name)) partition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(8, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName));

        sql = "insert into " + tableName + " (id, name) values (1, \"a\"), (2, \"b\") , (3, \"c\"), (4, \"d\"), (5, \"e\"), (6, \"f\"), (7, \"g\"), (8, \"h\")";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

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
        String sql =  "create table "
            + tableName
            + " (id int primary key, name int, global index "
            + indexTableName
            + " (name) partition by hash(name)) partition by hash(id)";
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
        Assert.assertEquals(
            getDataNumFromTable(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName)),
            getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, indexTableName));
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
        String partitionDef = " partition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String createGsi =
            String.format("create global index %s on %s(`pk`) partition by hash(`pk`)", gsiName,
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
    public void testAutoPartitionTable() throws SQLException {
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + testTableName + "_12";
        String gsiName = schemaPrefix + gsiIndexTableName + "_12";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String sql = "create table " + tableName
            + " (id int primary key,"
            + "name varchar(20),"
            + "index " + gsiName + "(name))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        sql = "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testTruncateTableWithGsiTopology() throws SQLException {
        // gsi not supported for cross db ddl
        if (crossSchema) {
            return;
        }
        List<String> storageList = LocalityTestUtils.getDatanodes(tddlConnection);

        Map<String, String> tableGroups = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> locality = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        int tableNum = storageList.size();
        // Create multiple tables, some are same
        for (int i = 0; i < tableNum * 2; i++) {
            String tableName = schemaPrefix + gsiPrimaryTableName + "_9_" + i;
            String indexTableName = schemaPrefix + gsiIndexTableName + "_9_" + i;
            dropTableIfExists(tableName);
            dropTableIfExists(indexTableName);

            String sql = String.format("create table %s (a int primary key, b int) partition by hash(a) locality='dn=%s'", tableName, storageList.get(i % tableNum));
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = String.format("alter table %s add global index %s(b) partition by hash(b)", tableName, indexTableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            String realGsiName = getRealGsiName(tddlConnection, tableName, indexTableName);

            tableGroups.put(tableName, getTableGroup(tddlConnection, tableName));
            tableGroups.put(indexTableName, getTableGroup(tddlConnection, realGsiName));

            locality.put(tableName, getLocality(tddlConnection, tableName));
            locality.put(indexTableName, getLocality(tddlConnection, realGsiName));
        }

        for (int i = 0; i < tableNum * 2; i++) {
            String tableName = schemaPrefix + gsiPrimaryTableName + "_9_" + i;
            String sql = String.format("truncate table %s", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        for (int i = 0; i < tableNum * 2; i++) {
            String tableName = schemaPrefix + gsiPrimaryTableName + "_9_" + i;
            String indexTableName = schemaPrefix + gsiIndexTableName + "_9_" + i;
            String realGsiName = getRealGsiName(tddlConnection, tableName, indexTableName);

            Assert.assertEquals(tableGroups.get(tableName), getTableGroup(tddlConnection, tableName));
            Assert.assertEquals(tableGroups.get(indexTableName), getTableGroup(tddlConnection, realGsiName));

            Assert.assertEquals(locality.get(tableName), getLocality(tddlConnection, tableName));
            Assert.assertEquals(locality.get(indexTableName), getLocality(tddlConnection, realGsiName));
        }
    }

    String getTableGroup(Connection tddlConnection, String tableName) {
        String sql = "show full create table " + tableName;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            if (rs.next()) {
                String fullCreateTable = rs.getString(2);
                for (String line : fullCreateTable.split("\n")) {
                    if (line.contains("tablegroup = ")) {
                        return line.substring(line.indexOf("`") + 1, line.lastIndexOf("`")).trim();
                    }
                }

            }
        } catch (SQLException e) {
        }
        return null;
    }

    String getLocality(Connection tddlConnection, String tableName) {
        String sql = "show full create table " + tableName;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            if (rs.next()) {
                String fullCreateTable = rs.getString(2);
                for (String line : fullCreateTable.split("\n")) {
                    if (line.contains("LOCALITY")) {
                        return line.substring(line.indexOf("'") + 1, line.lastIndexOf("'")).trim();
                    }
                }

            }
        } catch (SQLException e) {
        }
        return "";
    }

    @Test
    public void testTruncateTableFulltextIndex() {
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + testTableName + "_13";
        String gsiName1 = schemaPrefix + gsiIndexTableName + "_13_1";
        String gsiName2 = schemaPrefix + gsiIndexTableName + "_13_2";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName1);
        dropTableIfExists(gsiName2);
        String createSql = String.format(
            "create table %s (a int primary key, b varchar(255), c int, fulltext index %s(b), index %s(c))", tableName, gsiName1, gsiName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        String insertSql = String.format("insert into %s values (1,2,'1'),(2,3,'2'),(3,4,'3')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        Assert.assertEquals(3, getDataNumFromTable(tddlConnection, tableName));

        String truncateSql = String.format("truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncateSql);

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        Assert.assertEquals(3, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testTruncateTableSplitKey() {
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + testTableName + "_14";
        String gsiName = schemaPrefix + gsiIndexTableName + "_14";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13", tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        String splitSql = String.format("alter table %s split into partitions 10 by hot value(1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);
        splitSql = String.format("alter table %s split into partitions 10 by hot value(1)", getRealGsiName(tddlConnection, tableName, gsiName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        String insertSql = String.format("insert into %s values (1,2),(2,3),(3,4)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        Assert.assertEquals(3, getDataNumFromTable(tddlConnection, tableName));

        String truncateSql = String.format("truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncateSql);

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        Assert.assertEquals(3, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testTruncateTableAutoPartitionSplitKey() throws Exception {
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + testTableName + "_15";
        String gsiName = schemaPrefix + gsiIndexTableName + "_15";
        String tgName1 =  testTableName + "_tg_15_1";
        String tgName2 =  testTableName + "_tg_15_2";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);

        String createTgSql = String.format("create tablegroup %s", tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);
        createTgSql = String.format("create tablegroup %s", tgName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);

        String createSql = String.format(
            "create table %s (a int primary key, b int, global index %s(b,a)) tablegroup=%s", tableName, gsiName, tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        createSql = String.format(
            "alter table %s set tablegroup=%s", getRealGsiName(tddlConnection, tableName, gsiName), tgName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        Thread.sleep(2000);
        String splitSql = String.format("alter table %s split into partitions 1 by hot value(1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);
        splitSql = String.format("alter table %s split into partitions 10 by hot value(1)", getRealGsiName(tddlConnection, tableName, gsiName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        String insertSql = String.format("insert into %s values (1,2),(2,3),(3,4)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        Assert.assertEquals(3, getDataNumFromTable(tddlConnection, tableName));

        String truncateSql = String.format("truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncateSql);

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        Assert.assertEquals(3, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testAutoPartitionTableNoPk() throws SQLException {
        if (crossSchema) {
            return;
        }
        String tableName = schemaPrefix + testTableName + "_16";
        String gsiName = schemaPrefix + gsiIndexTableName + "_16";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String sql = "create table " + tableName
            + " (id int,"
            + "name varchar(20),"
            + "index " + gsiName + "(name))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        sql = "truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }
}
