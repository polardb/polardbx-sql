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
public class MqTest extends BaseTestCase {
    private static final String db = "MQ_TEST";
    private static final String table = "test_table";
    private static final String createTbl = "CREATE TABLE `%s` (\n"
        + "\t`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
        + "\t`creator` varchar(64) NOT NULL DEFAULT '' ,\n"
        + "\t`extend` varchar(128) NOT NULL DEFAULT '' ,\n"
        + "\tPRIMARY KEY (`id`),\n"
        + "\tGLOBAL INDEX `gidx_extend` (`extend`) COVERING (`id`)\n"
        + "\t\tPARTITION BY KEY(`extend`)\n"
        + "\t\tPARTITIONS 8\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 \n"
        + "PARTITION BY KEY(`id`)\n"
        + "PARTITIONS 8";

    private static final String insertSql = "insert into %s (creator, extend) values('a', 'a')";
    private static final String testSqlTemp =
        "select * from %s force index(gidx_extend) WHERE (extend IN ('a')) ORDER BY creator desc;";

    @Before
    public void buildCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
            c.createStatement().execute("create database if not exists " + db + " mode='auto'");
            c.createStatement().execute("use " + db);
            c.createStatement().execute(String.format(createTbl, table));
            c.createStatement().execute(String.format(insertSql, table));
        }
    }

    @After
    public void clean() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
        }
    }

    @Test
    public void testOriginalColumns() throws SQLException {
        try (Connection c = getPolardbxConnection(db)) {
            String testSql = String.format(testSqlTemp, table);

            // make sure plan got gsi
            String explain = getExplainResult(c, testSql);
            Assert.assertTrue(explain.contains("IndexScan"));

            // test plan in plan cache
            c.createStatement().execute(testSql);

            // test plan in baseline
            c.createStatement().execute("baseline add sql /*TDDL:a()*/" + testSql);
            c.createStatement().execute(testSql);
        }
    }
}
