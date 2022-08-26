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

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.enableAsyncDDL;

/**
 * 删除表测试
 *
 * @author simiao
 * @since 14-11-27
 */

@FixMethodOrder(MethodSorters.JVM)
public class DropTableTest extends AsyncDDLBaseNewDBTestCase {

    public DropTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    @Before
    public void init() {

    }

    @After
    public void clean() {

    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testDropTableOneGroupMultiTable() {

        String tableName = schemaPrefix + "simiao_test";
        dropTableAnd(tableName);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testDropTableOnlyPartitionWithoutKey() {
        String tableName = schemaPrefix + "simiao2_test";
        dropTableAnd(tableName);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testDropTableOnlyPartitionWithKey() {
        String tableName = schemaPrefix + "simiao3_test";
        dropTableAnd(tableName);
    }

    /**
     * @since 5.1.17
     */
    // @Ignore
    @Test
    public void testDropTableNoPartition() {

        String tableName = schemaPrefix + "simiao4_test";

        dropTableAnd(tableName);
    }

    private void dropTableAnd(String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select * from " + tableName + " where id > 0";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, "doesn't exist");
    }

    /**
     * @since 5.1.25
     */
    @Test
    public void testDropPartitionedTable() {
        String tableName = schemaPrefix + "minggong_drop_test";
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table " + tableName + "(id int) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {
            sql = "drop table " + tableName + "_0";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "drop parititioned single table");
        } catch (Throwable e) {
            sql = "select * from " + tableName;
            JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            return;
        } finally {
            sql = "drop table " + tableName;
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        Assert.assertTrue("should throw exception", false);
    }

    /**
     * remove rule if there are "Identifier name '%s' is too long" exceptions
     * when drop table table partitioned
     */
    @Test
    public void testRemoveRuleWhenNameTooLong1() {
        String table =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        String tableName = schemaPrefix + table;

        String dropSql = String.format("drop table %s", tableName);
        String dropIfExistsSql = String.format("drop table if exists %s", tableName);

        String createSql = String.format(
            "create table %s(id int, value int) dbpartition by hash(id) tbpartition by hash(value) tbpartitions 2",
            tableName);

        Connection showTableConn = StringUtils.isBlank(tddlDatabase2) ? tddlConnection : getTddlConnection2();

        // Drop first
        JdbcUtil.executeUpdateFailed(tddlConnection, dropIfExistsSql, "is too long", "data too long");

        // create table failed but rule pushed
        JdbcUtil.executeUpdateFailed(tddlConnection, createSql, "is too long", "data too long");

        List<String> tables = showTables(showTableConn);
        if (enableAsyncDDL) {
            Assert.assertFalse(tables.contains(table));
        } else {
            Assert.assertTrue(tables.contains(table));
        }

        tables = showTables(showTableConn);
        Assert.assertFalse(tables.contains(table));
    }

    /**
     * remove rule if there are "Identifier name '%s' is too long" exceptions
     * table not partitioned
     */
    @Test
    public void testRemoveRuleWhenNameTooLong2() {
        String table =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        String tableName = schemaPrefix + table;

        String dropSql = String.format("drop table %s", tableName);
        String dropIfExistsSql = String.format("drop table if exists %s", tableName);

        String createSql = String.format("create table %s(id int, value int)", tableName);

        Connection showTableConn = StringUtils.isBlank(tddlDatabase2) ? tddlConnection : getTddlConnection2();

        // Drop first
        JdbcUtil.executeUpdateFailed(tddlConnection, dropIfExistsSql, "is too long", "data too long");

        // create table failed but rule pushed
        JdbcUtil.executeUpdateFailed(tddlConnection, createSql, "is too long", "data too long");

        List<String> tables = showTables(showTableConn);
        if (enableAsyncDDL) {
            Assert.assertFalse(tables.contains(table));
        } else {
            Assert.assertTrue(tables.contains(table));
        }

        if (!enableAsyncDDL) {
            // drop table succeed and rule removed
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);
        }

        tables = showTables(showTableConn);
        Assert.assertFalse(tables.contains(table));
    }

    @Test
    @Ignore
    public void testTableMetaVisiablity() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists testTableMetaVisibility8");
        //DropTableHideTableMetaTask
        String hint = "/*+TDDL:cmd_extra(FP_PAUSE_AFTER_DDL_TASK_EXECUTION='TableSyncTask')*/";
        JdbcUtil.executeUpdateSuccess(
            tddlConnection, "create table if not exists testTableMetaVisibility8(c1 int primary key)");
        JdbcUtil.executeUpdate(tddlConnection, hint + "drop table testTableMetaVisibility8");
        JdbcUtil.executeQueryFaied(tddlConnection, "select * from testTableMetaVisibility8", "doesn't exist");
        JdbcUtil.executeUpdate(tddlConnection, "continue ddl all");
    }
}
