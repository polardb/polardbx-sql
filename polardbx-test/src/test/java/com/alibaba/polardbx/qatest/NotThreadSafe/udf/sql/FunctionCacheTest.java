package com.alibaba.polardbx.qatest.NotThreadSafe.udf.sql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
public class FunctionCacheTest extends BaseTestCase {
    protected Connection tddlConnection;
    protected Connection checkConnection;

    @Before
    public void init() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        this.checkConnection = getPolardbxConnection();
        dropAllfunctions();
    }

    private void dropAllfunctions() throws SQLException {
        try (Statement checkStmt = checkConnection.createStatement();
            Statement stmt = tddlConnection.createStatement();
            ResultSet rs = checkStmt.executeQuery(
                "select * from information_schema.routines where routine_type = 'function'")) {
            while (rs.next()) {
                String functionName = rs.getString("ROUTINE_NAME");
                stmt.executeUpdate("drop function if exists " + functionName);
            }
        }
    }

    @Test
    public void testCreateAndLoadFunction() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            Statement checkStmt = checkConnection.createStatement()) {
            // check empty when init
            functionCacheCompareTo0(checkStmt, true);
            functionUsedSizeCompareTo0(checkStmt, true);

            statement.executeUpdate("create function test_cache() returns int begin return 1; end");
            // create function should not affect cache size
            functionCacheCompareTo0(checkStmt, true);
            functionUsedSizeCompareTo0(checkStmt, true);
            // function cache contain function's meta
            cacheContainsfunction(checkStmt, "mysql.test_cache");

            statement.execute("select test_cache()");
            // when we call the function, it's definition will load into cache
            functionCacheCompareTo0(checkStmt, false);
            functionUsedSizeCompareTo0(checkStmt, false);

            // 1000 is greater than function's definition, so no function will be evicted
            statement.executeUpdate("resize function cache 1000");
            functionCacheCompareTo0(checkStmt, false);
            functionUsedSizeCompareTo0(checkStmt, false);

            // 50 is less than function's definition, so all function will be evicted
            statement.executeUpdate("resize function cache 50");
            functionCacheCompareTo0(checkStmt, true);
            functionUsedSizeCompareTo0(checkStmt, true);

            // reload function definition into cache
            statement.executeUpdate("resize function cache 1000");
            statement.execute("select test_cache()");

            // test clear function cache
            statement.executeUpdate("clear function cache");
            functionCacheCompareTo0(checkStmt, true);
            functionUsedSizeCompareTo0(checkStmt, true);

            statement.execute("select test_cache()");

            // test reload functions work will
            statement.executeUpdate("reload functions");
            functionCacheCompareTo0(checkStmt, true);
            functionUsedSizeCompareTo0(checkStmt, true);
            cacheContainsfunction(checkStmt, "mysql.test_cache");
        }
    }

    private void functionCacheCompareTo0(Statement statement, boolean equal) throws SQLException {
        boolean allEqual = true;
        try (ResultSet rs = statement.executeQuery("select * from information_schema.function_cache");) {
            while (rs.next()) {
                if (rs.getLong("SIZE") > 0) {
                    allEqual = false;
                    break;
                }
            }
        }

        if (equal != allEqual) {
            if (equal) {
                Assert.fail("function cache size should be zero");
            } else {
                Assert.fail("function cache size should greater than zero");
            }
        }
    }

    private void functionUsedSizeCompareTo0(Statement statement, boolean equal) throws SQLException {
        boolean allEqual = true;
        try (ResultSet rs = statement.executeQuery("select * from information_schema.function_cache_capacity");) {
            while (rs.next()) {
                if (rs.getLong("USED_SIZE") > 0) {
                    allEqual = false;
                    break;
                }
            }
        }
        if (equal != allEqual) {
            if (equal) {
                Assert.fail("function cache size should be zero");
            } else {
                Assert.fail("function cache size should greater than zero");
            }
        }
    }

    private void cacheContainsfunction(Statement statement, String function) throws SQLException {
        try (ResultSet rs = statement.executeQuery("select * from information_schema.function_cache");) {
            boolean find = false;
            while (rs.next()) {
                if (function.equalsIgnoreCase(rs.getString("function"))) {
                    find = true;
                    break;
                }
            }
            Assert.assertTrue(find, "function " + function + " should be find in the cache, but not");
        }
    }

    @After
    public void dropFunction() throws SQLException {
        try (
            Statement stmt = tddlConnection.createStatement();) {
            stmt.executeUpdate("drop function if exists test_cache");
        }
    }
}
