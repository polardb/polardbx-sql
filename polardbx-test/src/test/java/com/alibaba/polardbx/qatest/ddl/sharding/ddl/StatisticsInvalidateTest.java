package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author fangwu
 */
public class StatisticsInvalidateTest extends DDLBaseNewDBTestCase {
    private String schemaName;

    public StatisticsInvalidateTest() {
        Random r = new Random();
        schemaName = "statistic_invalidate_test_" + Math.abs(r.nextInt(1000));
    }

    @After
    public void clean() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + schemaName);
        }
    }

    /**
     * test plan in plan cache would be invalidated by drop table
     */
    @Test
    public void testTableStatisticsInvalidatedByDropTable() throws SQLException {
        Random r = new Random();
        String tableName = "statistic_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            //select and check statistics exists
            c2.createStatement().execute("select * from " + tableName + " where id=15");
            Assert.assertTrue(isStatisticExists(schemaName, tableName));

            // drop table
            c3 = prepareConnection(schemaName);
            c3.createStatement().execute("drop table if exists " + tableName);

            // check statistics not exists
            Assert.assertFalse(isStatisticExists(schemaName, tableName));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
        }
    }

    /**
     * test plan in plan cache would be invalidated by drop table
     */
    @Test
    public void testColumnStatisticsInvalidatedByAlterTable() throws SQLException {
        Random r = new Random();
        String tableName = "statistic_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // select and check column statistics exists
            String targetColumnName = "name";
            Assert.assertTrue(isStatisticExists(schemaName, tableName, targetColumnName));

            // alter table
            c3 = prepareConnection(schemaName);
            String alterSql = "alter table " + tableName + " drop column name";
            c3.createStatement().execute(alterSql);

            // check column statistics exists
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);
            Assert.assertFalse(isStatisticExists(schemaName, tableName, targetColumnName));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
        }
    }

    /**
     * test plan in plan cache would be invalidated by drop database
     */
    @Test
    public void testStatisticsInvalidatedByDropDatabase() throws SQLException {
        Random r = new Random();
        String tableName = "statistic_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // query and check statistic exists
            c2.createStatement().execute("select * from " + tableName + " where id=15");
            Assert.assertTrue(isStatisticExists(schemaName, tableName));

            // drop and recreate database
            c3 = getPolardbxConnection();
            c3.createStatement().execute("drop database if exists " + schemaName);
            c3.createStatement().execute("create database if not exists " + schemaName);

            // check statistic exists
            Assert.assertFalse(isStatisticExists(schemaName));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
        }
    }

    /**
     * test plan in baseline would be invalidated by drop database
     */
    @Test
    public void testTableStatisticInvalidatedByRenameTable() throws SQLException {
        Random r = new Random();
        String tableName = "statistic_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // query and check statistic
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);

            Assert.assertTrue(isStatisticExists(schemaName, tableName));

            // rename table
            String newTableName = "new_" + tableName;

            c.createStatement().execute("rename table " + tableName + " to " + newTableName);
            Assert.assertFalse(isStatisticExists(schemaName, tableName));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
        }
    }

    private Connection prepareConnection(String schema) throws SQLException {
        Connection c = getPolardbxConnection();
        c.createStatement().execute("use " + schema);
        return c;
    }

    /**
     * check virtual_statistic view if specified statistics info of schema&table exists
     *
     * @return is statistics info exists
     */
    private boolean isStatisticExists(String schemaName) throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            try {
                c.createStatement().execute("use " + schemaName);
            } catch (SQLException e) {
                return false;
            }
            ResultSet rs = c.createStatement().executeQuery(
                "select * from information_schema.virtual_statistic where schema_name='" + schemaName + "'");
            return rs.next();
        }
    }

    /**
     * check virtual_statistic view if specified statistics info of schema&table exists
     *
     * @return is statistics info exists
     */
    private boolean isStatisticExists(String schemaName, String tableName) throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            try {
                c.createStatement().execute("use " + schemaName);
                c.createStatement().execute("analyze table " + tableName);
            } catch (SQLException e) {
                // ignore
            }
            ResultSet rs = c.createStatement().executeQuery(
                "select * from information_schema.virtual_statistic where schema_name='" + schemaName
                    + "' and table_name='" + tableName + "'");
            return rs.next();
        }
    }

    /**
     * check virtual_statistic view if specified statistics info of schema&table exists
     *
     * @return is statistics info exists
     */
    private boolean isStatisticExists(String schemaName, String tableName, String columnName) throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            try {
                c.createStatement().execute("use " + schemaName);
                c.createStatement().execute("analyze table " + tableName);
            } catch (SQLException e) {
                // ignore
            }
            ResultSet rs = c.createStatement().executeQuery(
                "select * from information_schema.virtual_statistic where schema_name='" + schemaName
                    + "' and table_name='" + tableName + "' and column_name='" + columnName + "'");

            return rs.next();
        }
    }
}
