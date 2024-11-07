package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ForceIndexTest extends ColumnarReadBaseTestCase {
    @Test
    public void testSingleTable() throws SQLException, InterruptedException {
        final String tableName = "force_index_test_single";
        final String indexName = "force_index_test_single_cci";
        final String createTable = "create table if not exists " + tableName + " (id int primary key, a int) single";
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        String sql = "insert into " + tableName + " values (0, 0), (1, 1), (2, 2)";
        JdbcUtil.executeUpdateSuccessInTrx(tddlConnection, sql);
        ColumnarUtils.createColumnarIndex(tddlConnection, indexName, tableName, "a", "a", 3);
        sql = "insert into " + tableName + " values (10, 10), (11, 11), (12, 12)";
        JdbcUtil.executeUpdateSuccessInTrx(tddlConnection, sql);

        // simple select
        sql = "select count(a) from %s force index (%s)";
        ResultSet rs;
        boolean success = false;
        int retry = 0;
        do {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, tableName, indexName));
            Assert.assertTrue(rs.next());
            if (rs.getLong(1) == 6) {
                success = true;
                break;
            }
            Thread.sleep(1000);
        } while (retry++ < 10);
        Assert.assertTrue(success);

        // get tso 0
        sql = "select tso_timestamp()";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long tso0 = rs.getLong(1);

        // insert more data
        sql = "insert into " + tableName + " values (100, 100), (111, 111), (112, 112)";
        JdbcUtil.executeUpdateSuccessInTrx(tddlConnection, sql);

        sql = "select count(a) from %s force index (%s)";
        success = false;
        retry = 0;
        do {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, tableName, indexName));
            Assert.assertTrue(rs.next());
            if (rs.getLong(1) == 9) {
                success = true;
                break;
            }
            Thread.sleep(1000);
        } while (retry++ < 10);
        Assert.assertTrue(success);

        // get tso 1
        sql = "select tso_timestamp()";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        long tso1 = rs.getLong(1);

        // insert select
        final String targetTable = "force_index_test_single_target";
        final String targetPartitionedTable = "force_index_test_single_target_partitioned";
        JdbcUtil.dropTable(tddlConnection, targetTable);
        JdbcUtil.dropTable(tddlConnection, targetPartitionedTable);
        sql = "create table if not exists " + targetTable + " (id int primary key, a int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "create table if not exists " + targetPartitionedTable
            + " (id int primary key, a int) partition by key(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into %s select * from %s force index(%s)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, targetTable, tableName, indexName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, targetPartitionedTable, tableName, indexName));
        sql = "select count(0) from " + targetTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(9, rs.getLong(1));
        sql = "select count(0) from " + targetPartitionedTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(9, rs.getLong(1));

        // flashback
        sql = "select count(a) from %s as of tso %s force index(%s)";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, tableName, tso0, indexName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(6, rs.getLong(1));
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, tableName, tso1, indexName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(9, rs.getLong(1));

        // replace select flashback
        sql = "delete from " + targetTable;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "delete from " + targetPartitionedTable;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "replace into %s select * from %s as of tso %s force index(%s)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, targetTable, tableName, tso0, indexName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(sql, targetPartitionedTable, tableName, tso0, indexName));
        sql = "select count(0) from " + targetTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(6, rs.getLong(1));
        sql = "select count(0) from " + targetPartitionedTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(6, rs.getLong(1));
    }
}
