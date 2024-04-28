package com.alibaba.polardbx.qatest.dql.sharding.statistic;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author fangwu
 */
public class StatisticTest extends BaseTestCase {
    private static final String db = StatisticTest.class.getSimpleName();
    private static final String table = StatisticTest.class.getSimpleName();
    private static final String createTbl = "CREATE TABLE `%s` (\n"
        + "`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
        + "`creator` varchar(64) NOT NULL DEFAULT '' ,\n"
        + "`extend` varchar(128) NOT NULL DEFAULT '' ,\n"
        + "PRIMARY KEY (`id`) "
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ";

    private static final String testSqlTemp = "baseline add sql /*TDDL:a()*/ select * from %s ";

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
    public void test() throws SQLException, InterruptedException {
        testNDVWithEnableHll();
        testNDVWithOutliersValue();
    }

    /**
     * test NDV source controlled by enable_hll
     */
    public void testNDVWithEnableHll() throws SQLException, InterruptedException {
        try (Connection c = getPolardbxConnection(db);
            Statement stmt = c.createStatement();) {
            // analyze table
            stmt.execute("/*TDDL:ENABLE_HLL=true*/ analyze table " + table);

            // check ndv source from hll
            String statisticTraceInfo =
                getTraceInfoMap(stmt, String.format("select * from %s where creator='json'", table));
            assert statisticTraceInfo != null;
            assert statisticTraceInfo.contains("HLL_SKETCH");
        }
    }

    /**
     * test NDV source controlled by enable_hll
     */
    public void testNDVWithOutliersValue() throws SQLException, InterruptedException {
        try (Connection c = getPolardbxConnection(db);
            Statement stmt = c.createStatement();) {
            // truncate table first
            stmt.execute("truncate table " + table);
            // analyze table
            stmt.execute("/*TDDL:ENABLE_HLL=true*/  analyze table " + table);

            // check ndv source from hll
            String statisticTraceInfo =
                getTraceInfoMap(stmt, String.format("select * from %s where creator='john'", table));
            assert statisticTraceInfo != null;
            assert statisticTraceInfo.contains("HLL_SKETCH");

            // insert rows
            stmt.execute(String.format("insert into %s (creator, extend) values('john', 'json')", table));
            // change enable_hll to false
            stmt.execute("/*TDDL:enable_hll=false*/ analyze table " + table);

            statisticTraceInfo =
                getTraceInfoMap(stmt, String.format("select * from %s where creator='json'", table));
            assert statisticTraceInfo != null;
            assert !statisticTraceInfo.contains("HLL_SKETCH");
        }
    }

    @Nullable
    private String getTraceInfoMap(Statement stmt, String sql) throws SQLException {
        ResultSet rs = stmt.executeQuery("explain cost_trace " + sql);
        while (rs.next()) {
            String statisticTraceInfo = rs.getString(1);
            if (statisticTraceInfo.startsWith("STATISTIC TRACE INFO")) {
                return statisticTraceInfo;
            }
        }
        return null;
    }
}