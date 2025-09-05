package com.alibaba.polardbx.qatest.dql.sharding.spm;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.qatest.BaseTestCase.getPolardbxConnection0;

/**
 * @author fangwu
 */
public class ChoosePlanByCostTest {

    private static final String DB = ChoosePlanByCostTest.class.getSimpleName();
    private static final String TB = "order1";

    private static final String CREATE_TABLE_FORMATTED =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "    `id` BIGINT(11) NOT NULL AUTO_INCREMENT,\n"
            + "    `order_id` VARCHAR(20) DEFAULT NULL,\n"
            + "    `buyer_id` VARCHAR(20) DEFAULT NULL,\n"
            + "    `seller_id` VARCHAR(20) DEFAULT NULL,\n"
            + "    `order_snapshot` LONGTEXT,\n"
            + "    `order_detail` LONGTEXT,\n"
            + "    `test` VARCHAR(20) DEFAULT NULL,\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    GLOBAL INDEX `g_i_buyer` (`buyer_id`) COVERING (`order_id`)\n"
            + "        PARTITION BY KEY(`buyer_id`) PARTITIONS 16,\n"
            + "    GLOBAL INDEX `g_i_seller` (`seller_id`) COVERING (`order_id`)\n"
            + "        PARTITION BY KEY(`seller_id`) PARTITIONS 16,\n"
            + "    KEY `l_i_order` (`order_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
            + "PARTITION BY KEY(`order_id`) PARTITIONS 16";

    private static final String statisticCorrectionSql = "SET STATISTIC_CORRECTIONS='\n"
        + "Catalog:ChoosePlanByCostTest,g_i_buyer_$4897,buyer_id,123\n"
        + "Action:getFrequency\n"
        + "StatisticValue:10000\n"
        + "\n"
        + "Catalog:ChoosePlanByCostTest,g_i_buyer_$4897\n"
        + "Action:getRowCount\n"
        + "StatisticValue:10000\n"
        + "\n"
        + "Catalog:ChoosePlanByCostTest,order1,buyer_id,123\n"
        + "Action:getFrequency\n"
        + "StatisticValue:10000\n"
        + "\n"
        + "Catalog:chooseplanbycosttest,g_i_seller_$78a8,seller_id,1_null\n"
        + "Action:getRangeCount\n"
        + "StatisticValue:10000\n"
        + "\n"
        + "Catalog:chooseplanbycosttest,order1,seller_id,1_null\n"
        + "Action:getRangeCount\n"
        + "StatisticValue:10000\n"
        + "\n"
        + "Catalog:ChoosePlanByCostTest,order1\n"
        + "Action:getRowCount\n"
        + "StatisticValue:10000\n"
        + "\n"
        + "Catalog:ChoosePlanByCostTest,g_i_seller_$78a8\n"
        + "Action:getRowCount\n"
        + "StatisticValue:10000\n"
        + "\n"
        + "Catalog:ChoosePlanByCostTest,order1,id\n"
        + "Action:getCardinality\n"
        + "StatisticValue:10000\n"
        + "'";

    @BeforeClass
    public static void setUp() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB);
            c.createStatement().execute(String.format("create database if not exists %s mode='auto'", DB));
            c.createStatement().execute("use " + DB);
            c.createStatement().execute(String.format(CREATE_TABLE_FORMATTED, TB));
        }
    }

    @AfterClass
    public static void cleanUp() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB);
        }
    }

    @Test
    public void testCostChooseGsiByDifferentEqualValue() throws SQLException {
        String sql = "select * from %s where buyer_id='%s'";
        try (Connection c = getPolardbxConnection0(DB)) {
            // set statistics
            c.createStatement().execute(statisticCorrectionSql);

            // add baseline
            c.createStatement().execute("baseline add sql /*TDDL:a()*/ " + String.format(sql, TB, "123"));
            c.createStatement().execute("baseline add sql /*TDDL:a()*/ " + String.format(sql, TB, "1234"));

            String explain = explain(c, String.format(sql, TB, "123"));
            assertTrue(!explain.contains("g_i_buyer"));

            explain = explain(c, String.format(sql, TB, "1234"));
            assertTrue(explain.contains("g_i_buyer"));
        }
    }

    @Test
    public void testCostChooseGsiByDifferentLimitValue() throws SQLException {
        String sql = "select * from %s where buyer_id='123' limit %s";
        try (Connection c = getPolardbxConnection0(DB)) {
            // set statistics
            c.createStatement().execute(statisticCorrectionSql);

            // add baseline
            c.createStatement().execute("baseline add sql /*TDDL:a()*/ " + String.format(sql, TB, "100"));
            c.createStatement().execute("baseline add sql /*TDDL:a()*/ " + String.format(sql, TB, "1000"));

            String explain = explain(c, String.format(sql, TB, "1000"));
            assertTrue(!explain.contains("g_i_buyer"));

            explain = explain(c, String.format(sql, TB, "100"));
            assertTrue(explain.contains("g_i_buyer"));
        }
    }

    private String explain(Connection c, String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (ResultSet rs = c.createStatement().executeQuery("explain " + sql)) {
            while (rs.next()) {
                sb.append(rs.getString(1)).append("\n");
            }
            return sb.toString();
        }
    }
}
