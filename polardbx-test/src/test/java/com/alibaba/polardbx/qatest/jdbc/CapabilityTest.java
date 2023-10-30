package com.alibaba.polardbx.qatest.jdbc;

import com.alibaba.polardbx.qatest.ConnectionWrap;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.dml.auto.basecrud.MultiSqlTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.mysql.jdbc.ConnectionImpl;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CapabilityTest extends ReadBaseTestCase {
    // PolarDB-X 默认启用 DEPRECATE_EOF
    public static final boolean DEFAULT_DEPRECATE_EOF = true;

    @Test
    public void testDeprecateEof() throws SQLException {
        ConnectionImpl connImpl = (ConnectionImpl) ((ConnectionWrap) tddlConnection).getConnection();

        Assert.assertEquals(DEFAULT_DEPRECATE_EOF,
            connImpl.getIO().isEOFDeprecated());
    }

    /**
     * 启用EOF 并测试DAL语句回包是否正常
     */
    @Test
    public void testGlobalDeprecateEofFalse() throws InterruptedException, SQLException {
        if (!DEFAULT_DEPRECATE_EOF) {
            return;
        }
        try {
            JdbcUtil.setGlobal(tddlConnection, "set global DEPRECATE_EOF=false;");
            Thread.sleep(1500);

            try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
                ConnectionImpl connImpl = (ConnectionImpl) conn;
                Assert.assertNotEquals(DEFAULT_DEPRECATE_EOF,
                    connImpl.getIO().isEOFDeprecated());

                testMultiDalStatements(conn);
            }

        } finally {
            JdbcUtil.setGlobal(tddlConnection, "set global DEPRECATE_EOF=true;");
        }
    }

    /**
     * @see MultiSqlTest#testMultiDalStatements()
     */
    public void testMultiDalStatements(Connection conn) throws SQLException {
        String sql = "select @@VERSION_COMMENT;"
            + "select ?;"
            + "select database();"
            + "show databases;"
            + "show warnings;"
            + "show errors;"
            + "select ?;";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "foo");
            ps.setInt(2, 123);
            assertTrue(ps.execute());
            // select @@version_comment
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            // select "foo"
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertEquals("foo", rs.getString(1));
                assertFalse(rs.next());
            }
            // select database()
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            // show databases
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                while (rs.next()) {
                }
            }
            // show warnings
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                while (rs.next()) {
                }
            }
            // show errors
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                while (rs.next()) {
                }
            }
            // select 1
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertEquals(123, rs.getInt(1));
                assertFalse(rs.next());
            }
            // END
            assertFalse(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
        }
    }
}
