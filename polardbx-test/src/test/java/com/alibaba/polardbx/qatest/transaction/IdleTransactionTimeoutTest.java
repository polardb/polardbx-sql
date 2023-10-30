package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;

public class IdleTransactionTimeoutTest extends CrudBasedLockTestCase {
    private final static String tbName = "IdleTransactionTimeoutTestTB";

    @Test
    public void testIdleTimeout() throws InterruptedException {
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists " + tbName);
        JdbcUtil.executeSuccess(tddlConnection,
            "create table " + tbName + "(id int primary key)dbpartition by hash(id)");
        JdbcUtil.executeSuccess(tddlConnection, "insert into " + tbName + " values (0), (1)");

        {
            // Test idle_transaction_timeout.
            Connection conn = getPolardbxConnection();
            JdbcUtil.executeSuccess(conn, "set idle_transaction_timeout = 1");
            JdbcUtil.executeSuccess(conn, "begin");
            JdbcUtil.executeSuccess(conn, "insert into " + tbName + " values (100)");

            Thread.sleep(1000 + 5000);

            // Idle 1s, connection should be closed.
            JdbcUtil.executeFaied(conn, "select * from " + tbName, "Communications link failure");
        }

        {
            // Test idle_transaction_timeout.
            Connection conn = getPolardbxConnection();
            JdbcUtil.executeSuccess(conn, "set idle_readonly_transaction_timeout = 1");
            JdbcUtil.executeSuccess(conn, "begin");
            JdbcUtil.executeSuccess(conn, "select * from " + tbName);

            Thread.sleep(1000 + 5000);

            // Idle 1s, connection should be closed.
            JdbcUtil.executeFaied(conn, "select * from " + tbName, "Communications link failure");
        }

        {
            // Test idle_write_transaction_timeout.
            Connection conn = getPolardbxConnection();
            JdbcUtil.executeSuccess(conn, "set idle_write_transaction_timeout = 1");
            JdbcUtil.executeSuccess(conn, "begin");
            JdbcUtil.executeSuccess(conn, "select * from " + tbName);

            Thread.sleep(1000 + 5000);

            // Readonly transaction should not time out.
            JdbcUtil.executeSuccess(conn, "select * from " + tbName);

            // Make it a write transaction.
            JdbcUtil.executeSuccess(conn, "insert into " + tbName + " values (100)");

            Thread.sleep(1000 + 5000);

            // Idle 1s, connection should be closed.
            JdbcUtil.executeFaied(conn, "select * from " + tbName, "Communications link failure");
        }

        {
            // Test idle_readonly_transaction_timeout.
            Connection conn = getPolardbxConnection();
            JdbcUtil.executeSuccess(conn, "set idle_readonly_transaction_timeout = 1");
            JdbcUtil.executeSuccess(conn, "begin");
            JdbcUtil.executeSuccess(conn, "insert into " + tbName + " values (100)");

            Thread.sleep(1000 + 5000);

            // Write transaction should not time out.
            JdbcUtil.executeSuccess(conn, "select * from " + tbName);
            JdbcUtil.executeSuccess(conn, "rollback");

            JdbcUtil.executeSuccess(conn, "begin");
            JdbcUtil.executeSuccess(conn, "select * from " + tbName);

            Thread.sleep(1000 + 5000);

            // Idle 1s, connection should be closed.
            JdbcUtil.executeFaied(conn, "select * from " + tbName, "Communications link failure");
        }

        {
            // Priority.
            Connection conn = getPolardbxConnection();
            JdbcUtil.executeSuccess(conn, "set idle_transaction_timeout = 1");
            JdbcUtil.executeSuccess(conn, "set idle_readonly_transaction_timeout = 7");
            JdbcUtil.executeSuccess(conn, "begin");

            JdbcUtil.executeSuccess(conn, "select * from " + tbName);

            Thread.sleep(1000 + 5000);

            // Should ignore idle_transaction_timeout, and only idle_readonly_transaction_timeout works.
            JdbcUtil.executeSuccess(conn, "select * from " + tbName);

            Thread.sleep(7000 + 5000);

            JdbcUtil.executeFaied(conn, "select * from " + tbName, "Communications link failure");
        }

        {
            // Priority.
            Connection conn = getPolardbxConnection();
            JdbcUtil.executeSuccess(conn, "set idle_transaction_timeout = 1");
            JdbcUtil.executeSuccess(conn, "set idle_write_transaction_timeout = 7");
            JdbcUtil.executeSuccess(conn, "begin");

            JdbcUtil.executeSuccess(conn, "insert into " + tbName + " values (100)");

            Thread.sleep(1000 + 5000);

            // Should ignore idle_transaction_timeout, and only idle_readonly_transaction_timeout works.
            JdbcUtil.executeSuccess(conn, "select * from " + tbName);

            Thread.sleep(7000 + 5000);

            JdbcUtil.executeFaied(conn, "select * from " + tbName, "Communications link failure");
        }

    }
}
