package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TransactionViewTest extends ReadBaseTestCase {
    private static final String SCHEMA_NAME = "TransactionViewTest_db";
    private static final String CREATE_DB = "CREATE DATABASE if not exists " + SCHEMA_NAME + " mode=auto";
    private static final String DROP_DB = "DROP DATABASE if exists " + SCHEMA_NAME;
    private static final String TABLE_NAME = "TransactionViewTest_tb";
    private static final String CREATE_TABLE = "create table if not exists " + TABLE_NAME +
        "(id int primary key) partition by key(id)";
    private static final String INSERT_DATA = "insert into " + TABLE_NAME + " values (0), (1), (2)";
    private static final String SELECT_INNODB_TRX =
        "select count(0) from information_schema.innodb_trx where trx_id = '%s'";
    private static final String SELECT_INNODB_LOCKS =
        "select count(0) from information_schema.innodb_locks where lock_table like '%%%s%%'";
    private static final String SELECT_INNODB_LOCK_WAITS =
        "select count(0) from information_schema.innodb_lock_waits where blocking_trx_id = '%s'";

    @Before
    public void before() {
        JdbcUtil.executeUpdate(tddlConnection, CREATE_DB);
        JdbcUtil.executeUpdate(tddlConnection, "use " + SCHEMA_NAME);
        JdbcUtil.executeUpdate(tddlConnection, CREATE_TABLE);
        JdbcUtil.executeUpdate(tddlConnection, INSERT_DATA);
    }

    @After
    public void after() {
        JdbcUtil.executeUpdate(tddlConnection, DROP_DB);
    }

    @Test
    public void testSimple() throws SQLException, InterruptedException {
        final String sql = "select * from " + TABLE_NAME + " where id = 0 for update";

        JdbcUtil.executeUpdate(tddlConnection, "begin");
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);

        String trxId;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select current_trans_id()");
        Assert.assertTrue(rs.next());
        trxId = rs.getString(1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(SELECT_INNODB_TRX, trxId));
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        new Thread(() -> {
            try (Connection conn = ConnectionManager.newPolarDBXConnection0()) {
                JdbcUtil.executeUpdate(conn, "use " + SCHEMA_NAME);
                System.out.println("trx 2 starts.");
                JdbcUtil.executeQuerySuccess(conn, sql);
                JdbcUtil.executeUpdate(conn, "rollback");
                System.out.println("trx 2 ends.");
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }).start();

        int retry = 0;
        boolean success = false;
        do {
            Thread.sleep(1000);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(SELECT_INNODB_LOCKS, TABLE_NAME));
            if (rs.next() && rs.getLong(1) > 0) {
                rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(SELECT_INNODB_LOCK_WAITS, trxId));
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getLong(1) > 0);
                success = true;
                break;
            }
        } while (retry++ < 10);
        Assert.assertTrue(success);

        JdbcUtil.executeUpdate(tddlConnection, "rollback");
    }
}
