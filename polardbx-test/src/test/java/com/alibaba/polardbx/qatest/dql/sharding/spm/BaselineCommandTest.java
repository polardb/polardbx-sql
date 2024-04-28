package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author fangwu
 */
public class BaselineCommandTest extends BaseTestCase {
    private static final String db = "BaselineCommandTest";
    private static final String table = "BaselineCommandTest";
    private static final String createTbl = "CREATE TABLE `%s` (\n"
        + "`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
        + "`creator` varchar(64) NOT NULL DEFAULT '' ,\n"
        + "`extend` varchar(128) NOT NULL DEFAULT '' ,\n"
        + "PRIMARY KEY (`id`) "
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ";

    private static final String testSqlTemp = "baseline add sql /*TDDL:a()*/ select * from %s ";

    @Before
    public void buildCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
            c.createStatement().execute("create database if not exists " + db + " mode='drds'");
            c.createStatement().execute("use " + db);
            c.createStatement().execute(String.format(createTbl, table));
        }
    }

    @After
    public void clean() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
        }
    }

    @Test
    public void testAddPlanThatBaselineNotSupported() throws SQLException {
        try (Connection c = getPolardbxConnection(db)) {
            String testSql = String.format(testSqlTemp, table);
            // test plan in plan cache
            c.createStatement().execute(testSql);
        } catch (SQLException e) {
            if (e.getErrorCode() != 7001) {
                Assert.fail("not expected baseline error");
            }
        }
    }
}
