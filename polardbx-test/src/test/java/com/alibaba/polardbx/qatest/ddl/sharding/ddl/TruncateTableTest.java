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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardTbTable() {
        String tableName = schemaPrefix + testTableName + "_3";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + " (id int, name varchar(20)) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = "truncate table " + tableName;
//        Assert.assertEquals(2, getExplainNum(sql));
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
    public void testTruncateShardDbTable() {
        String tableName = schemaPrefix + testTableName + "_3";
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
        String tableName = schemaPrefix + testTableName + "_4";
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
    public void testTruncateShardDbTableWithGsi() {
        // gsi not supported for cross db ddl
        if (StringUtils.isNotBlank(tddlDatabase2)) {
            return;
        }

        String tableName = schemaPrefix + gsiPrimaryTableName + "_2";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_2";
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
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTableWithGsiShadowTestTableHint() {
        // gsi not supported for cross db ddl
        if (StringUtils.isNotBlank(tddlDatabase2)) {
            return;
        }

        String testPrefixHint = "/* //1/ */";
        String testTablePrefix = "__test_";

        String tableName = schemaPrefix + gsiPrimaryTableName + "_2";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_2";

        String tableNameWithPrefix = schemaPrefix + testTablePrefix + gsiPrimaryTableName + "_2";
        String indexTableNameWithPrefix = schemaPrefix + testTablePrefix + gsiIndexTableName + "_2";

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
    public void testTruncateShardDbTableWithGsiLongTableName() {
        // gsi not supported for cross db ddl
        if (StringUtils.isNotBlank(tddlDatabase2)) {
            return;
        }

        String tableName = schemaPrefix + gsiPrimaryTableName + "_3";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_3";

        tableName = StringUtils.rightPad(tableName, Limits.MAX_LENGTH_OF_LOGICAL_TABLE_NAME, 'p');
        indexTableName = StringUtils.rightPad(indexTableName, Limits.MAX_LENGTH_OF_INDEX_NAME, 'p');

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
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTbTableWithGsi() {
        // gsi not supported for cross db ddl
        if (StringUtils.isNotBlank(tddlDatabase2)) {
            return;
        }

        String tableName = schemaPrefix + gsiPrimaryTableName + "_3";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_3";
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
        Assert.assertEquals(8, getDataNumFromTable(tddlConnection, indexTableName));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    /**
     * @since 5.4.12
     */
    @Test
    public void testTruncateShardDbTbTableWithGsiWhileInsert() throws Exception {
        // gsi not supported for cross db ddl
        if (StringUtils.isNotBlank(tddlDatabase2)) {
            return;
        }
        String tableName = schemaPrefix + gsiPrimaryTableName + "_3";
        String indexTableName = schemaPrefix + gsiIndexTableName + "_3";
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

}
