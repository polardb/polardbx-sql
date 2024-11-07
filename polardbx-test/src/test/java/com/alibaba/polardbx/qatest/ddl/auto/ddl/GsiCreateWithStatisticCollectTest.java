package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * @author fangwu
 */
public class GsiCreateWithStatisticCollectTest extends BaseTestCase {
    private static String DB_NAME = "GsiStatisticTestDB";
    private static String TB_NAME = "GsiStatisticTestTB";

    private static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `l_i_order` (`order_id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`order_id`) partitions 16;";

    private static String ALTER_TABLE =
        "CREATE GLOBAL INDEX `g_i_seller` ON %s (`seller_id`) partition by hash(`seller_id`) partitions 16";

    private static String CHECK_TABLES =
        "SELECT TABLE_ROWS, UPDATE_TIME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";

    private static String INSERT_TABLE = "INSERT INTO %s (order_id, buyer_id, seller_id) VALUES(?, ?, ?)";

    private static String CHECK_STATISTIC =
        "SELECT GMT_MODIFIED, CARDINALITY, HISTOGRAM, TOPN FROM METADB.COLUMN_STATISTICS WHERE SCHEMA_NAME='%s' AND TABLE_NAME='%s'";

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute(String.format(CREATE_TABLE, TB_NAME));
        }
    }

    @Test
    public void test() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // analyze table and assert table rowcount == 0
            c.createStatement().execute("analyze table " + TB_NAME);
            ResultSet rs = c.createStatement().executeQuery(String.format(CHECK_TABLES, DB_NAME, TB_NAME));
            rs.next();
            int rowCount = rs.getInt("TABLE_ROWS");
            Timestamp date = rs.getTimestamp("UPDATE_TIME");

            assert rowCount == 0;

            // disable ENABLE_STATISTIC_FEEDBACK
            c.createStatement().execute("set global ENABLE_STATISTIC_FEEDBACK=false");

            // insert table
            String insertSql = String.format(INSERT_TABLE, TB_NAME);
            PreparedStatement ps = c.prepareStatement(insertSql);
            for (int i = 0; i < 1000; i++) {
                ps.setString(1, StringUtil.getRandomString(10));
                ps.setString(2, StringUtil.getRandomString(10));
                ps.setString(3, StringUtil.getRandomString(10));
                ps.addBatch();
            }
            ps.executeBatch();
            // create gsi
            c.createStatement().execute(String.format(ALTER_TABLE, TB_NAME));

            // assert statistics
            rs = c.createStatement().executeQuery(String.format(CHECK_STATISTIC, DB_NAME, TB_NAME));
            rs.next();
            while (rs.next()) {
                Timestamp gmt = rs.getTimestamp("GMT_MODIFIED");
                int ndv = rs.getInt("CARDINALITY");
                assert gmt.after(date);
                assert ndv > 0;
                String histogram = rs.getString("HISTOGRAM");
                String topN = rs.getString("TOPN");
                checkStatistic(histogram, topN);
            }

            rs.close();
            c.createStatement().execute("set global ENABLE_STATISTIC_FEEDBACK=TRUE");
        }
    }

    private void checkStatistic(String histogram, String topN) {
        if (isHistogramEmpty(histogram) && isTopNEmpty(topN)) {
            Assert.fail("histogram and topn were both empty");
        }
    }

    private boolean isTopNEmpty(String topN) {
        if (StringUtils.isEmpty(topN)) {
            return true;
        }
        if ("null".equalsIgnoreCase(topN)) {
            return true;
        }
        return false;
    }

    private boolean isHistogramEmpty(String histogram) {
        if (StringUtils.isEmpty(histogram)) {
            return true;
        }
        if ("null".equalsIgnoreCase(histogram)) {
            return true;
        }
        if (histogram.contains("\"buckets\":[]")) {
            return true;
        }
        return false;
    }
}
