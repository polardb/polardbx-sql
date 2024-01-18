package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
@NotThreadSafe
public class XErrorTest extends ReadBaseTestCase {

    @Test
    public void TimeoutTest() {
        if (!useXproto(tddlConnection)) {
            return;
        }
        final String table = ExecuteTableSelect.selectBaseOneTable()[0][0];
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
            String sql = "/*+TDDL: cmd_extra(SOCKET_TIMEOUT=100)*/select sleep(1) from " + table;
            JdbcUtil.executeQueryFaied(tddlConnection, sql, "XResult stream fetch result timeout");
            sql = "select * from " + table + " limit 1;";
            JdbcUtil.executeQueryFaied(tddlConnection, sql, "Previous query timeout");
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback");
        }
    }

    @Ignore
    public void MaxSessionTest() {
        if (!useXproto(tddlConnection)) {
            return;
        }

        try (final ResultSet rs = JdbcUtil.executeQuery("show variables like 'new_rpc'", tddlConnection)) {
            while (rs.next()) {
                if (rs.getString(2).equalsIgnoreCase("on")) {
                    return;
                }
            }
        } catch (Throwable ignore) {
        }

        final long max_conns =
            JdbcUtil.resultLong(JdbcUtil.executeQuery("select @@polarx_max_connections", tddlConnection));
        try {
            // make it smaller
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global polarx_max_connections=100");

            // make more connections
            List<Connection> conns = new ArrayList<>(200);
            try {
                for (int i = 0; i < 200; ++i) {
                    final Connection conn = getPolardbxDirectConnection();
                    conns.add(conn);
                    conn.setAutoCommit(false);
                    try (final Statement s = conn.createStatement()) {
                        s.execute("select * from " + polardbxOneDB + ".select_base_one_multi_db_multi_tb where pk=1");
                    }
                }
                Assert.fail("should fail whit max conns exceed");
            } catch (Throwable e) {
                Assert.assertTrue("Should throw out of max session count",
                    e.getMessage().contains("Out of max session count"));
            }
            for (Connection c : conns) {
                try {
                    c.close();
                } catch (Throwable ignore) {
                }
            }
        } finally {
            // restore
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global polarx_max_connections=" + max_conns);
        }
    }
}
