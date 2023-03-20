package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Materialize optimize test for IN subquery
 * CorrelateApply rel node (transformed by in subquery) should push down and looks like `xx in (?)`
 */

public class SubqueryMaterializeTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog(SubqueryMaterializeTest.class);
    private static final String testSchema = "subquery_materialize_testdb";

    @Test
    public void testInSubquery() throws SQLException {
        try {
            prepareCatalog();
            //SELECT *
            //FROM subquery_test_tb1 a
            //WHERE name IN
            //    (SELECT name
            //     FROM subquery_test_tb1
            //     WHERE id<100)
            //  OR id>1000
            String testSql =
                "SELECT * "
                    + "FROM subquery_test_tb1 a "
                    + "WHERE name IN "
                    + "    (SELECT name "
                    + "     FROM subquery_test_tb1 "
                    + "     WHERE id<100) "
                    + "  OR id>1000";

            // eliminate the affection from spm
            String normalHint = "/*TDDL:PUSH_CORRELATE_MATERIALIZED_LIMIT=-1*/";
            String testHint = "/*TDDL:PUSH_CORRELATE_MATERIALIZED_LIMIT=2000*/";

            String explainNormal = getExplain(normalHint + testSql, testSchema);
            String explainTest = getExplain(testHint + testSql, testSchema);

            Assert.assertTrue(explainNormal.contains("CorrelateApply"));
            Assert.assertTrue(explainTest.contains("`name` IN(?)"));
        } finally {
            clearCatalog();
        }
    }

    /**
     * provide explain sql info
     */
    private String getExplain(String sql, String schema) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + schema);
            ResultSet rs = c.createStatement().executeQuery("explain " + sql);
            while (rs.next()) {
                sb.append(rs.getString(1)).append("\n");
            }
        } finally {
            log.info("sql:" + sql);
            log.info("explain:" + sb);
        }
        return sb.toString();
    }

    /**
     * prepare db and table for test
     */
    private void prepareCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists subquery_materialize_testdb");
            c.createStatement().execute("create database subquery_materialize_testdb mode=auto");
            c.createStatement().execute("use subquery_materialize_testdb");
            String createTb = "CREATE TABLE `subquery_test_tb1` (\n"
                + "\t`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                + "\t`name` bigint(20) NOT NULL COMMENT '租户id',\n"
                + "\tPRIMARY KEY USING BTREE (`id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
            c.createStatement().execute(createTb);
        } finally {
            log.info("subquery_materialize_testdb catalog prepared");
        }
    }

    /**
     * clear db after test
     */
    private void clearCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists subquery_materialize_testdb");
        } finally {
            log.info("subquery_materialize_testdb catalog was dropped");
        }
    }
}
