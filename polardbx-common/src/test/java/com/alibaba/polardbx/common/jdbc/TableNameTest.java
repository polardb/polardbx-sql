package com.alibaba.polardbx.common.jdbc;

import com.alibaba.polardbx.common.utils.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class TableNameTest {

    @Test
    public void testTableName() throws SQLException {
        try {
            TableName tableName = new TableName("t1");
        } catch (SQLException e) {
            throw e;
        }
    }

    @Test
    public void testTableNameWithBlank() throws SQLException {
        try {
            TableName tableName = new TableName("t1 t2");
        } catch (SQLException e) {
            throw e;
        }
    }

    @Test
    public void testTableNameErr() throws SQLException {
        try {
            TableName tableName = new TableName("t'1");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("tableName format error"));
        }
    }

    @Test
    public void testTableNameErr2() throws SQLException {
        try {
            TableName tableName = new TableName("t\"1");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("tableName format error"));
        }
    }

    @Test
    public void testTableNameErr3() throws SQLException {
        try {
            TableName tableName = new TableName("t\\1");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("tableName format error"));
        }
    }

    @Test
    public void testTableNameErr4() throws SQLException {
        try {
            TableName tableName = new TableName("t\n1");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("tableName format error"));
        }
    }

    @Test
    public void testTableNameErr5() throws SQLException {
        try {
            TableName tableName = new TableName("");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("tableName should not be empty"));
        }
    }
}
