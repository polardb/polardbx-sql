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

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

/**
 * @author 梦实 2017年9月11日 下午2:44:58
 * @since 5.0.0
 */
public class OptimizeTableTest extends DDLBaseNewDBTestCase {

    private String testTableName = "truncate_test";

    private void checkOptimizeTable(String table) {
        checkOptimizeTable(Arrays.asList(table));
    }

    private void checkOptimizeTable(String table, Connection conn) {
        checkOptimizeTable(Arrays.asList(table), conn);
    }

    private void checkOptimizeTable(List<String> tables) {
        checkOptimizeTable(tables, tddlConnection);
    }

    private void checkOptimizeTable(List<String> tables, Connection conn) {
        String sql = "OPTIMIZE TABLE " + StringUtils.join(tables, ", ");
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql)) {
            /**
             * +--------------------+----------+----------+------------------------------------------+
             * | Table              | Op       | Msg_type | Msg_text                                 |
             * +--------------------+----------+----------+------------------------------------------+
             * | haha_single.hehe   | optimize | Error    | Table 'haha_single.hehe' doesn't exist   |
             * | haha_single.hehe   | optimize | status   | Operation failed                         |
             * +-------------------------------------------------------------------------------------|
             */
            for (List<Object> row : JdbcUtil.getAllResult(rs)) {
                String msgType = (String) row.get(2);
                String msgTxt = (String) row.get(3);
                if (StringUtils.equalsIgnoreCase(msgType, "Error")) {
                    Assert.fail("optimize table failed with: " + row);
                } else if (StringUtils.equalsIgnoreCase(msgType, "status")) {
                    Assert.assertEquals("OK", msgTxt);
                }
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testOptimizeTableBroadCastTable() {
        String tableName = testTableName + "_1";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + "(id int, name varchar(20))broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

//        sql = "optimize table " + tableName;
//        String explainResult = getExplainResult(tddlConnection, sql);
//        Assert.assertTrue(explainResult.contains("1_GROUP"));
//        Assert.assertTrue(explainResult.contains("0_GROUP"));
//        Assert.assertTrue(explainResult.contains(tableName));
        checkOptimizeTable(tableName);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        dropTableIfExists(tableName);
    }

    @Test
    public void testOptimizeTableSingleTable() {
        String tableName = testTableName + "_2";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + " (id int, name varchar(20))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

//        sql = "optimize table " + tableName;
//        String explainResult = getExplainResult(tddlConnection, sql);
//        Assert.assertTrue(explainResult.contains(tableName));
        checkOptimizeTable(tableName);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);

    }

    @Test
    public void testOptimizeTableShardTbTable() {
        String tableName = testTableName + "3";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + " (id int, name varchar(20)) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

//        sql = "optimize table " + tableName;
//        String explainResult = getExplainResult(tddlConnection, sql);
//        Assert.assertTrue(explainResult.contains(tableName + "_[0,1]"));
        checkOptimizeTable(tableName);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testOptimizeTableShardDbTable() {
        String tableName = testTableName + "_3";
        dropTableIfExists(tableName);

        String sql = "drop table if exists `" + tableName + "`;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertNotExistsTable(tableName, tddlConnection);

        sql = "create table " + tableName + " (id int, name varchar(20)) dbpartition by hash (id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "optimize table " + tableName;
        checkOptimizeTable(tableName);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testOptimizeTableShardDbTbTable() {
        String tableName = testTableName + "4";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

//        sql = "optimize table " + tableName;
//        String explainResult = getExplainResult(tddlConnection, sql);
//        Assert.assertTrue(explainResult.contains("[000000,000001]"));
//        Assert.assertTrue(explainResult.contains(tableName + "_[0-3]\""));
        checkOptimizeTable(tableName);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testOneTable() {
        String tableName = testTableName + "_5";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String gsiSql = String.format(
            "create global index %s on %s(id) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 6",
            tableName + "_gsi", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql = "optimize table " + tableName;
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            Assert.assertTrue("unexpected result row count: " + count,
                count == 8 || count == 16 || count == 4 || count == 2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        checkOptimizeTable(tableName);

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testTwoTable() {
        final String tableName1 = testTableName + "_6";
        final String tableName2 = testTableName + "_7";

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);

        String sql = "create table " + tableName1
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table " + tableName2
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = MessageFormat.format("optimize table {0}, {1}", tableName1, tableName2);

        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            Assert.assertTrue("unexpected result row count: " + count, count == 16 || count == 32 || count == 4);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        checkOptimizeTable(Arrays.asList(tableName1, tableName2));

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName1));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName2));

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
    }

    @Test
    public void testOneTableWithHint_1() {
        String tableName = testTableName + "_8";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "/*+TDDL:node(0)*/ optimize table " + tableName + "_00";
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            Assert.assertTrue("unexpected result row count: " + count, count == 1 || count == 2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        checkOptimizeTable(tableName);

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testOneTableWithHint_2() {
        final String tableName1 = testTableName + "_9";
        final String tableName2 = testTableName + "_10";

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);

        String sql = "create table " + tableName1
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table " + tableName2
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = MessageFormat.format("/*+TDDL:node(0)*/ optimize table {0}, {1}", tableName1, tableName2);

        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            Assert.assertTrue("unexpected result row count: " + count, count == 2 || count == 4);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        checkOptimizeTable(Arrays.asList(tableName1, tableName2));

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName1));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName2));

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
    }

    @Test
    public void testOneTableWithHint_3() {
        String tableName = testTableName + "_11";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "/*+TDDL:scan('" + tableName + "')*/ optimize table " + tableName;
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            Assert.assertTrue("unexpected result row count: " + count, count == 8 || count == 16 || count == 2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        checkOptimizeTable(tableName);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testOneTableWithHint_4() {
        final String tableName1 = testTableName + "_12";
        final String tableName2 = testTableName + "_13";

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);

        String sql = "create table " + tableName1
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table " + tableName2
            + " (id int, name varchar(20)) dbpartition by hash (id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = MessageFormat.format("/*+TDDL:scan(\"" + tableName1 + "," + tableName2 + "\")*/ optimize table {0}, {1}",
            tableName1,
            tableName2);

        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            Assert.assertTrue("unexpected result row count: " + count, count == 16 || count == 32 || count == 4);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        checkOptimizeTable(Arrays.asList(tableName1, tableName2));

        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName1));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName2));

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
    }

    @Test
    public void testPartitionTable() throws SQLException {
        final String dbName = "opt_part_db";
        final String tableName = "opt_part_tb";
        final List<String> partitions = Arrays.asList(
            "",
            "broadcast",
            "partition by range(id) (partition p0 values less than(MAXVALUE))",
            "partition by hash(id)",
            "partition by key(id)"
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create database if not exists %s mode='auto'", dbName));

        String prevDb = null;
        Connection conn = null;
        try {
            conn = getPolardbxDirectConnection();
            prevDb = JdbcUtil.executeQueryAndGetFirstStringResult("select database()", conn);
            JdbcUtil.executeUpdateSuccess(conn, "use " + dbName);

            for (String part : partitions) {
                String sql = String.format("create table if not exists %s (id int) %s", tableName, part);
                JdbcUtil.executeUpdateSuccess(conn, sql);
                checkOptimizeTable(tableName, conn);
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + tableName);
                logger.info("optimize table: " + part);
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database " + dbName);
        } finally {
            if (prevDb != null) {
                JdbcUtil.executeUpdateSuccess(conn, "use " + prevDb);
            }
            conn.close();
        }
    }
}
