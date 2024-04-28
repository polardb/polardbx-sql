package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ShowCompatibilityLevelTest extends ReadBaseTestCase {
    private static final String tbName = "ShowDowngradeTest_tb";
    private static final String createTable =
        "create table if not exists " + tbName + "(id int primary key, a int)dbpartition by hash(id)";
    private static final String dropTable = "drop table if exists " + tbName;
    private static final String insertData = "insert into " + tbName + " values (0, 0), (1, 0), (2, 0), (3, 0)";
    private int current = 0;
    private static final String turnOffRecover = "set global ENABLE_TRANSACTION_RECOVER_TASK = false";
    private static final String turnOnRecover = "set global ENABLE_TRANSACTION_RECOVER_TASK = true";
    private static final String selectCompatibilityLevel = "select compatibility_level";
    private static final String alterCompatibilityLevel = "alter system set compatibility_level = ";
    private static final String showCompatibilityLevel = "show compatibility_level ";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeSuccess(conn, dropTable);
            JdbcUtil.executeSuccess(conn, createTable);
            JdbcUtil.executeSuccess(conn, insertData);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeIgnoreErrors(conn, turnOnRecover);
            JdbcUtil.executeIgnoreErrors(conn, dropTable);
            JdbcUtil.executeIgnoreErrors(conn, "set global trx_log_method = 1");
        }
    }

    @Test
    public void testDowngradeTo5_4_18_17034692() throws SQLException, InterruptedException {
        // Downgrading to 5.4.18-17034692 is ok, and trx_log_method should still be 1.
        JdbcUtil.executeSuccess(tddlConnection, "set global trx_log_method = 1");
        JdbcUtil.waitUntilVariableChanged(tddlConnection, "trx_log_method", "1", 5);
        String version = "5.4.18-17034692";
        Long level;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, selectCompatibilityLevel + "(" + version + ")");
        Assert.assertTrue(rs.next());
        level = rs.getLong(1);
        JdbcUtil.executeSuccess(tddlConnection, alterCompatibilityLevel + level);
        String status, cause, solution;
        // wait at most 10s
        int retry = 0, maxRetry = 100;
        do {
            Thread.sleep(100);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCompatibilityLevel + level);
            Assert.assertTrue("No result for alter", rs.next());
            status = rs.getString("STATUS");
            cause = rs.getString("CAUSE");
            solution = rs.getString("SOLUTION");
            retry++;
            if (retry > maxRetry) {
                break;
            }
        } while (status.equalsIgnoreCase("RUNNING"));
        if (!status.equalsIgnoreCase("OK")) {
            Assert.fail("status: " + status + ", cause: " + cause);
        }
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show variables like 'trx_log_method'");
        Assert.assertTrue("No variable trx_log_method found", rs.next());
        Assert.assertEquals("trx_log_method is not 1", 1, rs.getInt(2));
    }

    @Test
    public void testDowngradeTo5_4_18_17004745() throws SQLException, InterruptedException {
        JdbcUtil.executeSuccess(tddlConnection, "set global trx_log_method = 1");
        JdbcUtil.waitUntilVariableChanged(tddlConnection, "trx_log_method", "1", 5);
        String version = "5.4.18-17004745";
        Long level;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, selectCompatibilityLevel + "(" + version + ")");
        Assert.assertTrue(rs.next());
        level = rs.getLong(1);
        JdbcUtil.executeSuccess(tddlConnection, alterCompatibilityLevel + level);
        String status, cause, solution;
        // wait at most 10s
        int retry = 0, maxRetry = 100;
        do {
            Thread.sleep(100);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCompatibilityLevel + level);
            Assert.assertTrue("No result for alter", rs.next());
            status = rs.getString("STATUS");
            cause = rs.getString("CAUSE");
            solution = rs.getString("SOLUTION");
            retry++;
            if (retry > maxRetry) {
                break;
            }
        } while (status.equalsIgnoreCase("RUNNING"));
        if (!status.equalsIgnoreCase("OK")) {
            Assert.fail("status: " + status + ", cause: " + cause);
        }
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show variables like 'trx_log_method'");
        Assert.assertTrue("No variable trx_log_method found", rs.next());
        Assert.assertEquals("trx_log_method is not 0", 0, rs.getInt(2));
    }

    @Test
    public void testDowngradeTo5_4_18_17004744() throws SQLException, InterruptedException {
        // Downgrading to 5.4.18-17004744 will change trx_log_method to 0, and fail if we make a long prepared trx.
        JdbcUtil.executeSuccess(tddlConnection, "set global trx_log_method = 1");
        JdbcUtil.executeSuccess(tddlConnection, turnOffRecover);
        JdbcUtil.waitUntilVariableChanged(tddlConnection, "trx_log_method", "1", 5);

        // Make a prepared trx.
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_AFTER_PRIMARY_COMMIT') */";
        String sql = "update " + tbName + " set a = " + (++current);
        JdbcUtil.executeUpdate(tddlConnection, "begin");
        try {
            JdbcUtil.executeUpdate(tddlConnection, hint + sql);
            JdbcUtil.executeIgnoreErrors(tddlConnection, "commit");
        } finally {
            JdbcUtil.executeUpdate(tddlConnection, "rollback");
        }

        String version = "5.4.18-17004744";
        Long level;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, selectCompatibilityLevel + "(" + version + ")");
        Assert.assertTrue(rs.next());
        level = rs.getLong(1);
        JdbcUtil.executeSuccess(tddlConnection, alterCompatibilityLevel + level);
        String status, cause, solution;
        // wait at most 35s
        int retry = 0, maxRetry = 40;
        do {
            Thread.sleep(1000);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCompatibilityLevel + level);
            Assert.assertTrue("No result for alter", rs.next());
            status = rs.getString("STATUS");
            cause = rs.getString("CAUSE");
            solution = rs.getString("SOLUTION");
            retry++;
            if (retry > maxRetry) {
                break;
            }
        } while (status.equalsIgnoreCase("RUNNING"));
        if (!status.equalsIgnoreCase("ERROR")) {
            Assert.fail("status: " + status + ", cause: " + cause);
        }
        Assert.assertTrue(cause.contains("Draining prepared trx timeout."));
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show variables like 'trx_log_method'");
        Assert.assertTrue("No variable trx_log_method found", rs.next());
        Assert.assertEquals("trx_log_method is not 0", 0, rs.getInt(2));
        JdbcUtil.executeSuccess(tddlConnection, turnOnRecover);

        // wait at most 35s
        JdbcUtil.executeSuccess(tddlConnection, alterCompatibilityLevel + level);
        retry = 0;
        do {
            Thread.sleep(1000);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCompatibilityLevel + level);
            Assert.assertTrue("No result for alter", rs.next());
            status = rs.getString("STATUS");
            cause = rs.getString("CAUSE");
            solution = rs.getString("SOLUTION");
            retry++;
            if (retry > maxRetry) {
                break;
            }
        } while (status.equalsIgnoreCase("RUNNING"));
        if (!status.equalsIgnoreCase("OK")) {
            Assert.fail("status: " + status + ", cause: " + cause);
        }
    }

    @Test
    public void testDowngradeTo5_4_18_17047709() throws SQLException, InterruptedException {
        // Downgrading to 5.4.18-17047709 will disable ENABLE_X_PROTO_OPT_FOR_AUTO_SP.
        JdbcUtil.executeSuccess(tddlConnection, "set global ENABLE_X_PROTO_OPT_FOR_AUTO_SP = true");
        JdbcUtil.waitUntilVariableChanged(tddlConnection, "ENABLE_X_PROTO_OPT_FOR_AUTO_SP", "true", 5);
        String version = "5.4.18-17047709";
        Long level;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, selectCompatibilityLevel + "(" + version + ")");
        Assert.assertTrue(rs.next());
        level = rs.getLong(1);
        JdbcUtil.executeSuccess(tddlConnection, alterCompatibilityLevel + level);
        String status, cause, solution;
        // wait at most 10s
        int retry = 0, maxRetry = 100;
        do {
            Thread.sleep(100);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCompatibilityLevel + level);
            Assert.assertTrue("No result for alter", rs.next());
            status = rs.getString("STATUS");
            cause = rs.getString("CAUSE");
            solution = rs.getString("SOLUTION");
            retry++;
            if (retry > maxRetry) {
                break;
            }
        } while (status.equalsIgnoreCase("RUNNING"));
        if (!status.equalsIgnoreCase("OK")) {
            Assert.fail("status: " + status + ", cause: " + cause);
        }
        rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, "show variables like 'ENABLE_X_PROTO_OPT_FOR_AUTO_SP'");
        Assert.assertTrue("No variable ENABLE_X_PROTO_OPT_FOR_AUTO_SP found", rs.next());
        Assert.assertEquals("ENABLE_X_PROTO_OPT_FOR_AUTO_SP is not disabled",
            "false", rs.getString(2));
    }

    @Test
    public void testDowngradeTo5_4_18_17004745_rpm() throws SQLException, InterruptedException {
        JdbcUtil.executeSuccess(tddlConnection, "set global trx_log_method = 1");
        JdbcUtil.waitUntilVariableChanged(tddlConnection, "trx_log_method", "1", 5);
        String version = "polarx-kernel_5.4.18-17004745_xcluster-20231213";
        Long level;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, selectCompatibilityLevel + "(" + version + ")");
        Assert.assertTrue(rs.next());
        level = rs.getLong(1);
        JdbcUtil.executeSuccess(tddlConnection, alterCompatibilityLevel + level);
        String status, cause, solution;
        // wait at most 10s
        int retry = 0, maxRetry = 100;
        do {
            Thread.sleep(100);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCompatibilityLevel + level);
            Assert.assertTrue("No result for alter", rs.next());
            status = rs.getString("STATUS");
            cause = rs.getString("CAUSE");
            solution = rs.getString("SOLUTION");
            retry++;
            if (retry > maxRetry) {
                break;
            }
        } while (status.equalsIgnoreCase("RUNNING"));
        if (!status.equalsIgnoreCase("OK")) {
            Assert.fail("status: " + status + ", cause: " + cause);
        }
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show variables like 'trx_log_method'");
        Assert.assertTrue("No variable trx_log_method found", rs.next());
        Assert.assertEquals("trx_log_method is not 0", 0, rs.getInt(2));
    }

    @Test
    public void testPrivilege() throws SQLException {
        // create user test_privilege_compatibility_level
        String dropUser = "drop user 'test_compatibility_level'@'%'";
        JdbcUtil.executeIgnoreErrors(tddlConnection, dropUser);
        String sql = "create user 'test_compatibility_level'@'%' identified by 'kasjhdka'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        try (Connection conn = ConnectionManager.getInstance()
            .newPolarDBXConnection("test_compatibility_level", "kasjhdka")) {
            String version = "polarx-kernel_5.4.18-17004745_xcluster-20231213";
            Long level;
            ResultSet rs = JdbcUtil.executeQuerySuccess(conn, selectCompatibilityLevel + "(" + version + ")");
            Assert.assertTrue(rs.next());
            level = rs.getLong(1);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(alterCompatibilityLevel + level);
                rs = stmt.getResultSet();
                Assert.assertTrue(rs.next());
                System.out.println(rs.getString(1) + " " + rs.getString(2));
                Assert.assertEquals("only super user can execute",
                    "ERROR",
                    rs.getString(1));
                Assert.assertEquals("only super user can execute",
                    "Only super user can change COMPATIBILITY_LEVEL",
                    rs.getString(2));
            }
        } finally {
            JdbcUtil.executeIgnoreErrors(tddlConnection, dropUser);
        }

    }
}
