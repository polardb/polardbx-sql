package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IgnoreNoTransactionJdbcTest {
    @Test
    public void test() throws SQLException {
        String testDb = "IgnoreNoTransactionJdbcTest_db";
        String testTb = "IgnoreNoTransactionJdbcTest_tb";
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("create database if not exists " + testDb + " mode=auto ");
                stmt.execute("use " + testDb);
                stmt.execute("create table if not exists " + testTb + "(id int, a int) partition by key(id)");
            }
        }
        // Make sure it is the newly created JDBC connection.
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("use " + testDb);
            }
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("set transaction policy 4");
                stmt.execute("select * from " + testTb + " where 1 = 2");
            }
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("replace into " + testTb + " values (101, 101)");
                ResultSet rs = stmt.executeQuery("show trans");
                Assert.assertTrue("Should start a explicit transaction", rs.next());
                System.out.println(rs.getString(2));
            }
            conn.commit();

            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("replace into " + testTb + " values (101, 101)");
            }

            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("replace into " + testTb + " values (101, 101)");
                ResultSet rs = stmt.executeQuery("show trans");
                Assert.assertTrue("Should start a explicit transaction", rs.next());
                System.out.println(rs.getString(2));
            }
            conn.commit();
        }
    }
}
