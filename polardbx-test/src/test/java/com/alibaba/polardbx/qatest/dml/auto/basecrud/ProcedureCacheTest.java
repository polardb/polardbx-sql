package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
@FileStoreIgnore
public class ProcedureCacheTest extends BaseTestCase {
    protected Connection tddlConnection;
    protected Connection checkConnection;

    @Before
    public void init() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        this.checkConnection = getPolardbxConnection();
        dropAllProcedures();
    }

    private void dropAllProcedures() throws SQLException {
        try (Statement checkStmt = checkConnection.createStatement();
            Statement stmt = tddlConnection.createStatement();
            ResultSet rs = checkStmt.executeQuery("select * from information_schema.routines where routine_type = 'procedure'")) {
            while(rs.next()) {
                String schema = rs.getString("ROUTINE_SCHEMA");
                String procedureName = rs.getString("ROUTINE_NAME");
                stmt.executeUpdate("drop procedure if exists " + schema + "." + procedureName);
            }
        }
    }

    @Test
    public void testCreateAndLoadProcedure() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
        Statement checkStmt = checkConnection.createStatement()) {
            // check empty when init
            procedureCacheCompareTo0(checkStmt, true);
            procedureUsedSizeCompareTo0(checkStmt, true);

            statement.executeUpdate("create procedure test_cache() begin select 1; end");
            // create procedure should not affect cache size
            procedureCacheCompareTo0(checkStmt, true);
            procedureUsedSizeCompareTo0(checkStmt, true);
            // procedure cache contain procedure's meta
            cacheContainsProcedure(checkStmt, "test_cache");

            statement.executeUpdate("call test_cache");
            // when we call the procedure, it's definition will load into cache
            procedureCacheCompareTo0(checkStmt, false);
            procedureUsedSizeCompareTo0(checkStmt, false);

            // 1000 is greater than procedure's definition, so no procedure will be evicted
            statement.executeUpdate("resize procedure cache 1000");
            procedureCacheCompareTo0(checkStmt, false);
            procedureUsedSizeCompareTo0(checkStmt, false);

            // 50 is less than procedure's definition, so all procedure will be evicted
            statement.executeUpdate("resize procedure cache 50");
            procedureCacheCompareTo0(checkStmt, true);
            procedureUsedSizeCompareTo0(checkStmt, true);

            // reload procedure definition into cache
            statement.executeUpdate("resize procedure cache 1000");
            statement.executeUpdate("call test_cache");

            // test clear procedure cache
            statement.executeUpdate("clear procedure cache");
            procedureCacheCompareTo0(checkStmt, true);
            procedureUsedSizeCompareTo0(checkStmt, true);

            statement.executeUpdate("call test_cache");

            // test reload procedures work will
            statement.executeUpdate("reload procedures");
            procedureCacheCompareTo0(checkStmt, true);
            procedureUsedSizeCompareTo0(checkStmt, true);
            cacheContainsProcedure(checkStmt, "test_cache");
        }
    }

    private void procedureCacheCompareTo0(Statement statement, boolean equal) throws SQLException {
        boolean allEqual = true;
        try (ResultSet rs = statement.executeQuery("select * from information_schema.procedure_cache");) {
            while(rs.next()) {
                if (rs.getLong("SIZE") > 0) {
                    allEqual = false;
                    break;
                }
            }
        }
        if (equal != allEqual) {
            if (equal) {
                Assert.fail("procedure cache size should be zero");
            } else {
                Assert.fail("procedure cache size should greater than zero");
            }
        }
    }

    private void procedureUsedSizeCompareTo0(Statement statement, boolean equal) throws SQLException {
        boolean allEqual = true;
        try (ResultSet rs = statement.executeQuery("select * from information_schema.procedure_cache_capacity");) {
            while(rs.next()) {
                if (rs.getLong("USED_SIZE") > 0) {
                    allEqual = false;
                    break;
                }
            }
        }
        if (equal != allEqual) {
            if (equal) {
                Assert.fail("procedure cache size should be zero");
            } else {
                Assert.fail("procedure cache size should greater than zero");
            }
        }
    }

    private void cacheContainsProcedure(Statement statement, String procedure) throws SQLException {
        try (ResultSet rs = statement.executeQuery("select * from information_schema.procedure_cache");) {
            boolean find  = false;
            while(rs.next()) {
                if (procedure.equalsIgnoreCase(rs.getString("PROCEDURE"))) {
                    find = true;
                    break;
                }
            }
            Assert.assertTrue(find, "procedure " + procedure + " should be find in the cache, but not");
        }
    }

    @After
    public void dropProcedure() throws SQLException {
        try (
            Statement stmt = tddlConnection.createStatement();) {
            stmt.executeUpdate("drop procedure if exists test_cache");
        }
    }
}
