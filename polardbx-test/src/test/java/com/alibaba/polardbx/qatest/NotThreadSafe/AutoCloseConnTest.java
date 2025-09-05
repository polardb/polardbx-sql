package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AutoCloseConnTest extends DDLBaseNewDBTestCase {
    private static final String DB_NAME = "test_auto_close_conn_db";
    private static final String CREATE_DB_SQL = "create database " + DB_NAME + " mode=auto";
    private static final String DROP_DB_SQL = "drop database if exists " + DB_NAME;
    private static final String TABLE_NAME = "test_auto_close_conn_tb";
    private static final String CREATE_TABLE_SQL = "create table if not exists " + TABLE_NAME + " ("
        + "id int auto_increment primary key, "
        + "name varchar(255), "
        + "global unique index ugsi(name) partition by key(name) "
        + ") partition by key(id)";
    private static final String DROP_TABLE_SQL = "drop table if exists " + TABLE_NAME;
    private static final String INSERT_SQL = "insert into " + TABLE_NAME + " (name) values ";
    private static final String SELECT_SQL = "select count(0) from " + TABLE_NAME;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(conn, DROP_DB_SQL);
            JdbcUtil.executeUpdateSuccess(conn, CREATE_DB_SQL);
            JdbcUtil.executeUpdateSuccess(conn, "use " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(conn, DROP_TABLE_SQL);
            JdbcUtil.executeUpdateSuccess(conn, CREATE_TABLE_SQL);
            JdbcUtil.executeUpdateSuccess(conn, INSERT_SQL + "('a'), ('b'), ('c')");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(conn, DROP_DB_SQL);
        }
    }

    @Test
    public void testAutoCloseConn() throws SQLException {
        try (Connection conn = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(conn, "set global ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = true");
            JdbcUtil.executeUpdateSuccess(conn, "set ENABLE_AUTO_SAVEPOINT = true");
            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateFailed(conn, INSERT_SQL + "('d'), ('b'), ('e')", "Duplicate entry");
            JdbcUtil.executeUpdateSuccess(conn, "commit");

            ResultSet rs = JdbcUtil.executeQuerySuccess(conn, SELECT_SQL);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getLong(1));

            JdbcUtil.executeUpdateSuccess(conn, "set ENABLE_AUTO_SAVEPOINT = false");
            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateFailed(conn, INSERT_SQL + "('f'), ('b'), ('g')", "Duplicate entry");
            JdbcUtil.executeUpdateSuccess(conn, "rollback");

            rs = JdbcUtil.executeQuerySuccess(conn, SELECT_SQL);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getLong(1));

            JdbcUtil.executeUpdateSuccess(conn, "set ENABLE_AUTO_SAVEPOINT = false");
            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateFailed(conn, INSERT_SQL + "('f'), ('b'), ('g')", "Duplicate entry");
            JdbcUtil.executeUpdateFailed(conn, "commit", "Communications link failure");

            JdbcUtil.executeUpdateFailed(conn, "rollback", "No operations allowed after connection closed");
        }

        try (Connection conn = getPolardbxConnection0(DB_NAME)) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(conn, SELECT_SQL);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getLong(1));

            JdbcUtil.executeUpdateSuccess(conn, "set ENABLE_AUTO_SAVEPOINT = false");
            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateFailed(conn, INSERT_SQL + "('f'), ('b'), ('g')", "Duplicate entry");
            JdbcUtil.executeUpdateFailed(conn, "DELETE FROM " + TABLE_NAME + " WHERE 1=1", "Communications link failure");

            JdbcUtil.executeUpdateFailed(conn, "commit", "No operations allowed after connection closed");
        }

        try (Connection conn = getPolardbxConnection0(DB_NAME)) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(conn, SELECT_SQL);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getLong(1));
            JdbcUtil.executeUpdateSuccess(conn, "set ENABLE_AUTO_SAVEPOINT = false");
            JdbcUtil.executeUpdateSuccess(conn, "set autocommit = 0");
            JdbcUtil.executeUpdateFailed(conn, INSERT_SQL + "('f'), ('b'), ('g')", "Duplicate entry");
            JdbcUtil.executeUpdateFailed(conn, "set autocommit = 1", "Communications link failure");
        }
    }
}
