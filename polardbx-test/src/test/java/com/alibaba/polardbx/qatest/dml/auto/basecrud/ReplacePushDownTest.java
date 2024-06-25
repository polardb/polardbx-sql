package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReplacePushDownTest extends BaseTestCase {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private static final String canPushDownTable0 = "ReplacePushDownTest_canPushDownTable0";
    private static final String canPushDownTable1 = "ReplacePushDownTest_canPushDownTable1";
    private static final String canNotPushDownTable0 = "ReplacePushDownTest_canNotPushDownTable0";
    private static final String canNotPushDownTable1 = "ReplacePushDownTest_canNotPushDownTable1";
    private static final String createTableCanPushDownTable0
        = "create table if not exists " + canPushDownTable0
        + " (id int auto_increment primary key, a int, b int, c int, d int, data int, "
        + " clustered index cgsi_canPushDownTable0 (c, d) partition by key(c, d), "
        + " local unique index uk(a, b, c, d) "
        + " ) partition by key(a, b)";

    private static final String createTableCanPushDownTable1
        = "create table if not exists " + canPushDownTable1
        + " (a int, b int, c int, d int, data int, "
        + " clustered index cgsi_canPushDownTable1 (c, d) partition by key(c, d), "
        + " primary key(a, b, c, d) "
        + " ) partition by key(a, b)";

    /**
     * Considering existing record: (10, 10, 10, 10),
     * tx1: replace (10, 10, 10, 20)
     * tx2: replace (10, 20, 20, 20)
     * tx1 may replace primary table and delete + insert gsi
     * tx2 may insert into primary table and gsi
     * (10, 10) and (10, 20) happens to be in the same partition
     * the above case may cause data inconsistency in primary table and gsi.
     */
    private static final String createTableCanNotPushDownTable0
        = "create table if not exists " + canNotPushDownTable0
        + " (a int, b int, c int, d int, data int, "
        + " clustered index cgsi_canNotPushDownTable0 (c, d) partition by key(c, d), "
        + " primary key(a, c), "
        + " local unique index uk0(a, b, c, d), "
        + " local unique index uk1(a, d)"
        + " ) partition by key(a, b)";

    private static final String createTableCanNotPushDownTable1
        = "create table if not exists " + canNotPushDownTable1
        + " (a int, b int, c int, d int, data int, "
        + " global unique index ugsi_canNotPushDownTable0 (c, d) partition by key(c, d), "
        + " primary key(a, b, c, d) "
        + " ) partition by key(a, b)";

    private static final String insertSql = "insert into %s (a, b, c, d, data) values %s";

    private static final String dropSql = "drop table if exists %s";

    private void clean(Connection tddlConnection) {
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropSql, canPushDownTable0));
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropSql, canPushDownTable1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropSql, canNotPushDownTable0));
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropSql, canNotPushDownTable1));
    }

    @Test
    public void testReplacePushDown() throws SQLException {
        try (Connection tddlConnection = getPolardbxConnection()) {
            clean(tddlConnection);
            JdbcUtil.executeSuccess(tddlConnection, createTableCanNotPushDownTable0);
            JdbcUtil.executeSuccess(tddlConnection, createTableCanNotPushDownTable1);
            JdbcUtil.executeSuccess(tddlConnection, createTableCanPushDownTable0);
            JdbcUtil.executeSuccess(tddlConnection, createTableCanPushDownTable1);

            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    String.format(insertSql, canNotPushDownTable0, "(10, 10, 10, 10, 10)"));
                JdbcUtil.executeSuccess(tddlConnection,
                    String.format(insertSql, canNotPushDownTable1, "(10, 10, 10, 10, 10)"));
                JdbcUtil.executeSuccess(tddlConnection,
                    String.format(insertSql, canPushDownTable0, "(10, 10, 10, 10, 10)"));
                JdbcUtil.executeSuccess(tddlConnection,
                    String.format(insertSql, canPushDownTable1, "(10, 10, 10, 10, 10)"));

                String replaceSql = "trace replace into %s (a, b, c, d, data) values (10, 10, 10, 10, 100)";
                JdbcUtil.executeSuccess(tddlConnection, "set transaction_isolation = 'REPEATABLE-READ'");
                shouldPushDown(tddlConnection, String.format(replaceSql, canPushDownTable0));
                shouldPushDown(tddlConnection, String.format(replaceSql, canPushDownTable1));
                shouldPushDown(tddlConnection, String.format(replaceSql, canNotPushDownTable0));
                shouldNotPushDown(tddlConnection, String.format(replaceSql, canNotPushDownTable1));

                JdbcUtil.executeSuccess(tddlConnection, "set transaction_isolation = 'READ-COMMITTED'");
                shouldPushDown(tddlConnection, String.format(replaceSql, canPushDownTable0));
                shouldPushDown(tddlConnection, String.format(replaceSql, canPushDownTable1));
                shouldNotPushDown(tddlConnection, String.format(replaceSql, canNotPushDownTable0));
                shouldNotPushDown(tddlConnection, String.format(replaceSql, canNotPushDownTable1));
            } finally {
                clean(tddlConnection);
            }
        }
    }

    private void shouldPushDown(Connection tddlConnection, String replaceSql) throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection, "begin");
        JdbcUtil.executeSuccess(tddlConnection, replaceSql);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show trace");
        while (rs.next()) {
            String statement = rs.getString("STATEMENT");
            // Should only contain SELECT or REPLACE
            if (!statement.contains("SELECT") && !statement.contains("REPLACE")) {
                System.out.println("unexpected statement: " + statement);
                Assert.fail("Should only contain SELECT or REPLACE");
            }
        }
        JdbcUtil.executeSuccess(tddlConnection, "rollback");
    }

    private void shouldNotPushDown(Connection tddlConnection, String replaceSql) throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection, "begin");
        JdbcUtil.executeSuccess(tddlConnection, replaceSql);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show trace");
        while (rs.next()) {
            String statement = rs.getString("STATEMENT");
            // Should only contain SELECT or REPLACE
            if (statement.contains("REPLACE")) {
                System.out.println("unexpected statement: " + statement);
                Assert.fail("Should not contain REPLACE");
            }
        }
        JdbcUtil.executeSuccess(tddlConnection, "rollback");
    }
}
