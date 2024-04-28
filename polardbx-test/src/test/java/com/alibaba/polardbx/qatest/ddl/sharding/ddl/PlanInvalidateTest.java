package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author fangwu
 */
public class PlanInvalidateTest extends DDLBaseNewDBTestCase {
    private String schemaName;

    public PlanInvalidateTest() {
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
    public void testPlanCacheInvalidatedByDropTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into plan cache
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:true") && explain.contains("Source:PLAN_CACHE"));

            // drop table
            c3 = prepareConnection(schemaName);
            c3.createStatement().execute("drop table if exists " + tableName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:false") && explain.contains("Source:PLAN_CACHE"));
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
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test plan in plan cache would be invalidated by drop table
     */
    @Test
    public void testPlanCacheInvalidatedByAlterTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
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

            // make select plan into plan cache
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:true") && explain.contains("Source:PLAN_CACHE"));

            // alter table
            c3 = prepareConnection(schemaName);
            String alterSql = "alter table " + tableName + " add index(name)";
            c3.createStatement().execute(alterSql);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:false") && explain.contains("Source:PLAN_CACHE"));
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
    public void testPlanCacheInvalidatedByDropDatabase() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into plan cache
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:true") && explain.contains("Source:PLAN_CACHE"));

            // drop and recreate database
            c3 = getPolardbxConnection();
            c3.createStatement().execute("drop database if exists " + schemaName);
            c3.createStatement().execute("create database if not exists " + schemaName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:false") && explain.contains("Source:PLAN_CACHE"));
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
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test plan in baseline would be invalidated by drop database
     */
    @Test
    public void testBaselineInvalidatedByDropDatabase() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into baseline
            String sql = "baseline add sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_ACCEPT"));

            // drop and recreate database
            c3 = getPolardbxConnection();
            c3.createStatement().execute("drop database if exists " + schemaName);
            c3.createStatement().execute("create database if not exists " + schemaName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:PLAN_CACHE"));
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
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test plan in baseline would be invalidated by drop table
     */
    @Test
    public void testBaselineInvalidatedByDropTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into baseline
            String sql = "baseline add sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_ACCEPT"));

            // drop and recreate database
            c3 = prepareConnection(schemaName);
            c3.createStatement().execute("drop table if exists " + tableName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:PLAN_CACHE"));
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
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test fixed plan in baseline would be invalidated by drop table
     */
    @Test
    public void testBaselineFixedInvalidatedByDropTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into baseline
            String sql = "baseline fix sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_FIX"));

            // drop and recreate database
            c3 = prepareConnection(schemaName);
            c3.createStatement().execute("drop table if exists " + tableName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:PLAN_CACHE"));
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
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test fixed plan in baseline would be rebuilt by alter table
     */
    @Test
    public void testBaselineFixedInvalidatedByAlterTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
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

            // make select plan into baseline
            String sql = "baseline fix sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_FIX"));

            // drop and recreate database
            c3 = prepareConnection(schemaName);
            String alterSql = "alter table " + tableName + " add index(name)";
            c3.createStatement().execute(alterSql);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_FIX_DDL_HASHCODE_UPDATE"));
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

    private Connection prepareConnection(String schema) throws SQLException {
        Connection c = getPolardbxConnection();
        c.createStatement().execute("use " + schema);
        return c;
    }
}
