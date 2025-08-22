package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FlashbackAreaTest extends CrudBasedLockTestCase {
    private final String isolation;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @After
    public void after() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback");
    }

    @Parameterized.Parameters(name = "isolation:{0}")
    public static List<Object[]> prepare() {
        return new ArrayList<Object[]>() {
            {
                add(new Object[] {"READ-COMMITTED"});
                add(new Object[] {"REPEATABLE-READ"});
            }
        };
    }

    public FlashbackAreaTest(String isolation) {
        this.isolation = isolation;
    }

    @Test
    public void testSimpleCase() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set TRANSACTION_ISOLATION = '" + isolation + "'");
        String tableName = "FlashbackAreaTest_tb";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global opt_flashback_area = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global innodb_txn_retention = 259200");
        String createTableSql = "create table if not exists " + tableName + " (\n"
            + "  id int primary key,\n"
            + "  a int,\n"
            + "  local index idx(a)\n"
            + ") partition by key(id)";
        long beforeCreateTableTso = getTso();
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + tableName + " values(1,1)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "commit");
        long beforeInsert100TableTso = getTso();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + tableName + " values(100,100)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "commit");
        long beforeUpdate100TableTso = getTso();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableName + " set a = 200 where id = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "commit");
        long afterTso = getTso();

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select * from " + tableName + " as of tso " + beforeCreateTableTso);
        Assert.assertFalse(rs.next());

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select count(0) from " + tableName + " as of tso " + beforeInsert100TableTso);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select * from " + tableName + " as of tso " + beforeUpdate100TableTso + " order by id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt("id"));
        Assert.assertEquals(1, rs.getInt("a"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(100, rs.getInt("id"));
        Assert.assertEquals(100, rs.getInt("a"));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select * from " + tableName + " as of tso " + afterTso + " order by id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt("id"));
        Assert.assertEquals(1, rs.getInt("a"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(100, rs.getInt("id"));
        Assert.assertEquals(200, rs.getInt("a"));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select * from " + tableName + " order by id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt("id"));
        Assert.assertEquals(1, rs.getInt("a"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(100, rs.getInt("id"));
        Assert.assertEquals(200, rs.getInt("a"));

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
    }

    @Test
    public void testSpm() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "FlashbackAreaTest_testSpm";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
        String createTableSql = "create table if not exists " + tableName + " (\n"
            + "  id int primary key,\n"
            + "  a int,\n"
            + "  local index idx(a)\n"
            + ") partition by key(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        String sql = "insert into " + tableName + " values (0,0), (1,1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        long tso = getTso();
        sql =
            "select * from " + tableName + " as t1 as of tso " + tso + " join " + tableName + " as t2 as of tso " + tso
                + " on t1.id=t2.id";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void testSwitch() throws SQLException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }

        int retry = 5;
        while (retry-- > 0) {
            if (testSwitchOnce()) {
                return;
            }
        }
        Assert.fail("Try 5 times and still fails.");
    }

    private boolean testSwitchOnce() throws SQLException, InterruptedException {
        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(connection, "set global opt_flashback_area = true");
            JdbcUtil.executeUpdateSuccess(connection, "set global innodb_txn_retention = 259200");
            JdbcUtil.executeUpdateSuccess(connection, "set global innodb_undo_retention = 0");
            JdbcUtil.executeUpdateSuccess(connection, "reload datasources");
            String tableName = "FlashbackAreaTest_testSwitch";
            JdbcUtil.executeUpdateSuccess(connection, "drop table if exists " + tableName);
            String createTableSql = "create table if not exists " + tableName + " (\n"
                + "  id int primary key,\n"
                + "  a int,\n"
                + "  local index idx(a)\n"
                + ") partition by key(id)";
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);
            String sql = "insert into " + tableName + " values (0,0), (1,1)";
            JdbcUtil.executeUpdateSuccess(connection, sql);
            long tso = getTso();
            sql = "insert into " + tableName + " values (100,100), (101,101)";
            JdbcUtil.executeUpdateSuccess(connection, sql);

            ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                "select * from " + tableName + " as of tso " + tso + " order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt("id"));
            Assert.assertEquals(0, rs.getInt("a"));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt("id"));
            Assert.assertEquals(1, rs.getInt("a"));
            Assert.assertFalse(false);

            Thread.sleep(5000);

            JdbcUtil.executeUpdate(connection, "SET ENABLE_FLASHBACK_AREA = FALSE");

            try {
                JdbcUtil.executeQuerySuccess(connection,
                    "select * from " + tableName + " as of tso " + tso + " order by id");
            } catch (Throwable t) {
                System.out.println(t.getMessage());
                Assert.assertTrue(t.getMessage().contains("Snapshot too old"));

                JdbcUtil.executeUpdate(connection, "SET ENABLE_FLASHBACK_AREA = TRUE");
                rs = JdbcUtil.executeQuerySuccess(connection,
                    "select * from " + tableName + " as of tso " + tso + " order by id");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(0, rs.getInt("id"));
                Assert.assertEquals(0, rs.getInt("a"));
                Assert.assertTrue(rs.next());
                Assert.assertEquals(1, rs.getInt("id"));
                Assert.assertEquals(1, rs.getInt("a"));
                Assert.assertFalse(false);

                return true;
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global innodb_undo_retention = 1800");
        }
        return false;
    }

    private long getTso() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select tso_timestamp()");
        Assert.assertTrue(rs.next());
        return rs.getLong(1);
    }
}
