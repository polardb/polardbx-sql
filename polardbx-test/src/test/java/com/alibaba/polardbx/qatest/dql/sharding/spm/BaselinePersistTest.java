package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author fangwu
 */
public class BaselinePersistTest extends BaseTestCase {
    private static final String db = "spm_test";
    private static final String tbl = "spm_tbl_test";
    private static final String tblCreate = "CREATE TABLE `%s` (\n"
        + "\t`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
        + "\t`creator` varchar(64) NOT NULL DEFAULT '',\n"
        + "\t`extend` varchar(128) NOT NULL DEFAULT '',\n"
        + "\tPRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB AUTO_INCREMENT = 2 DEFAULT CHARSET = utf8mb4\n";

    private static final String testSqlFormat = "select * from %s a join %s b on a.creator=b.extend";

    @Test
    public void test() throws SQLException {
        check();
        // test baseline
        String testSql = String.format(testSqlFormat, tbl, tbl);
        try (Connection c = getPolardbxConnection(db)) {
            c.createStatement().executeQuery(testSql);
            c.createStatement().executeQuery(testSql);

            String explain = getExplainResult(c, testSql);
            Assert.assertTrue(explain, explain.contains("SPM"));
            c.createStatement().execute("baseline persist");
        }
        check();
        // test baseline fix
        try (Connection c = getPolardbxConnection(db)) {
            c.createStatement().execute("baseline fix sql /*TDDL:a()*/ " + testSql);
            String explain = getExplainResult(c, testSql);
            Assert.assertTrue(explain, explain.contains("FIX"));
        }
        check();
    }

    @Before
    public void buildCatalog() {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
            c.createStatement().execute("create database if not exists " + db + " mode='auto'");
            c.createStatement().execute("use " + db);
            c.createStatement().execute(String.format(tblCreate, tbl));
        } catch (SQLException e) {
            // only print clean error
            e.printStackTrace();
        }
    }

    @After
    public void clean() {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
        } catch (SQLException e) {
            // only print clean error
            e.printStackTrace();
        }
    }

    private void check() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            ResultSet rs = c.createStatement().executeQuery("SELECT COUNT(1) FROM METADB.SPM_PLAN WHERE !accepted");
            rs.next();
            Assert.assertTrue(rs.getInt(1) == 0);
        }
    }
}
