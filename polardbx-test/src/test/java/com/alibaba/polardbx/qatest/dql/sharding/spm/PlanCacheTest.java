package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.CdcIgnore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author fangwu
 */
public class PlanCacheTest extends BaseTestCase {
    private static final String db = "PlanCacheTest";
    private static final String table = "PlanCacheTest";
    private static final String createTbl = "CREATE TABLE `%s` (\n"
        + "`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
        + "`creator` varchar(64) NOT NULL DEFAULT '' ,\n"
        + "`extend` varchar(128) NOT NULL DEFAULT '' ,\n"
        + "PRIMARY KEY (`id`) "
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ";

    private static final String BASELINE_ADD = "baseline add sql /*TDDL:a()*/ select * from %s ";

    private static final String SQL = "select * from %s ";

    @BeforeClass
    public static void buildCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + db);
            c.createStatement().execute("create database if not exists " + db + " mode='auto'");
            c.createStatement().execute("use " + db);
            c.createStatement().execute(String.format(createTbl, table));
        }
    }

    @AfterClass
    public static void clean() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + db);
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "暂时未查到原因，可能是并行跑各种实验室导致。本地无法复现，且对replica实验室无影响")
    public void testClearPlanCacheCommand() throws SQLException {
        try (Connection c = getPolardbxConnection(db)) {
            // Add baseline
            String sql = String.format(BASELINE_ADD, table);
            c.createStatement().execute(sql);

            // Add plancache
            sql = String.format(SQL, table);
            c.createStatement().execute(sql);

            // check plancache size and baseline size
            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.PLAN_CACHE WHERE SCHEMA_NAME='" + db + "'";
            ResultSet rs = c.createStatement().executeQuery(sql);
            rs.next();
            int count = rs.getInt(1);
            rs.close();
            assert count > 0;

            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.SPM WHERE SCHEMA_NAME='" + db + "'";
            rs = c.createStatement().executeQuery(sql);
            rs.next();
            count = rs.getInt(1);
            rs.close();
            assert count > 0;

            // clear plancache
            c.createStatement().execute("clear plancache");

            // check plancache size and baseline size
            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.PLAN_CACHE WHERE SCHEMA_NAME='" + db + "'";
            rs = c.createStatement().executeQuery(sql);
            rs.next();
            count = rs.getInt(1);
            rs.close();
            assert count == 0;

            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.SPM WHERE SCHEMA_NAME='" + db + "'";
            rs = c.createStatement().executeQuery(sql);
            rs.next();
            count = rs.getInt(1);
            rs.close();
            assert count > 0;
        }
    }

    @Test
    public void testClearPlanCacheWhenAnalyzeTable() throws SQLException {
        try (Connection c = getPolardbxConnection(db)) {
            // Add baseline
            String sql = String.format(BASELINE_ADD, table);
            c.createStatement().execute(sql);

            // Add plancache
            sql = String.format(SQL, table);
            c.createStatement().execute(sql);

            // check plancache size and baseline size
            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.PLAN_CACHE WHERE SCHEMA_NAME='" + db + "'";
            ResultSet rs = c.createStatement().executeQuery(sql);
            rs.next();
            int count = rs.getInt(1);
            rs.close();
            assert count > 0;

            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.SPM WHERE SCHEMA_NAME='" + db + "'";
            rs = c.createStatement().executeQuery(sql);
            rs.next();
            count = rs.getInt(1);
            rs.close();
            assert count > 0;

            // clear plancache
            c.createStatement().execute("analyze table " + table);

            // check plancache size and baseline size
            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.PLAN_CACHE WHERE SCHEMA_NAME='" + db + "'";
            rs = c.createStatement().executeQuery(sql);
            rs.next();
            count = rs.getInt(1);
            rs.close();
            assert count == 0;

            sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.SPM WHERE SCHEMA_NAME='" + db + "'";
            rs = c.createStatement().executeQuery(sql);
            rs.next();
            count = rs.getInt(1);
            rs.close();
            assert count > 0;
        }
    }
}
