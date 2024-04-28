package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author fangwu
 */
public class StatisticsTest extends BaseTestCase {
    private static final String DB_NAME = "STAT_TEST_DB";
    private static final String TB_NAME = "STAT_TEST_TB";

    private static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  `create_time` timestamp DEFAULT current_timestamp,\n"
        + "  `create_time1` datetime DEFAULT current_timestamp,\n"
        + "  `create_time2` date,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `l_i_order` (`order_id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`order_id`) partitions 2";

    private static String STATISTIC_VAL =
        "replace INTO metadb.column_statistics (`SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAME`, `CARDINALITY`, `CMSKETCH`, `HISTOGRAM`, `TOPN`, `NULL_COUNT`, `SAMPLE_RATE`) VALUES "
            + "('STAT_TEST_DB','STAT_TEST_TB','create_time',1983673,'AAAAAAAAAAAAAAABAAAAAQAAAABdjWq5AAAAAAAAAAA=','{\"buckets\":[{\"ndv\":664,\"upper\":\"1843857561355288576\",\"lower\":\"1843645821044654080\",\"count\":1393,\"preSum\":0},{\"ndv\":236,\"upper\":\"1843930763955798016\",\"lower\":\"1843864050245566464\",\"count\":1375,\"preSum\":1393}],\"maxBucketSize\":64,\"type\":\"Datetime\",\"sampleRate\":0.9243518}',"
            + "'{\"countArr\":[1698,906,826],\"valueArr\":[1843646895558230016,1843856884344291328,1843857255137542144],\"type\":\"Datetime\",\"sampleRate\":0.924351804778851}',0,0.0026470406),"
            + "('STAT_TEST_DB','STAT_TEST_TB','create_time1',1983673,'AAAAAAAAAAAAAAABAAAAAQAAAABdjWq5AAAAAAAAAAA=','{\"buckets\":[{\"ndv\":664,\"upper\":\"1843857561355288576\",\"lower\":\"1843645821044654080\",\"count\":1393,\"preSum\":0},{\"ndv\":236,\"upper\":\"1843930763955798016\",\"lower\":\"1843864050245566464\",\"count\":1375,\"preSum\":1393}],\"maxBucketSize\":64,\"type\":\"Datetime\",\"sampleRate\":0.9243518}',"
            + "'{\"countArr\":[1698,906,826],\"valueArr\":[1843646895558230016,1843856884344291328,1843857255137542144],\"type\":\"Datetime\",\"sampleRate\":0.924351804778851}',0,0.0026470406),"
            + "('STAT_TEST_DB','STAT_TEST_TB','create_time2',1983673,'AAAAAAAAAAAAAAABAAAAAQAAAABdjWq5AAAAAAAAAAA=','{\"buckets\":[{\"ndv\":664,\"upper\":\"1843857561355288576\",\"lower\":\"1843645821044654080\",\"count\":1393,\"preSum\":0},{\"ndv\":236,\"upper\":\"1843930763955798016\",\"lower\":\"1843864050245566464\",\"count\":1375,\"preSum\":1393}],\"maxBucketSize\":64,\"type\":\"Date\",\"sampleRate\":0.9243518}',"
            + "'{\"countArr\":[1698,906,826],\"valueArr\":[1843646895558230016,1843856884344291328,1843857255137542144],\"type\":\"Date\",\"sampleRate\":0.924351804778851}',0,0.0026470406); "
            + "replace into  metadb.table_statistics (schema_name, table_name, row_count) values('STAT_TEST_DB','STAT_TEST_TB', 10000000)";

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute(String.format(CREATE_TABLE, TB_NAME));
            c.createStatement().execute("analyze table " + TB_NAME);
            c.createStatement().execute(STATISTIC_VAL);
            c.createStatement().execute("reload statistics");
            c.createStatement().execute("set global CACHELINE_INDICATE_UPDATE_TIME = 1702080000");
        }
    }

    @AfterClass
    public static void clean() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("set global CACHELINE_INDICATE_UPDATE_TIME = 0;");
            c.createStatement().execute("set global ENABLE_CACHELINE_COMPENSATION = false");
        }
    }

    @Test
    public void test() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            c.createStatement().execute("set global ENABLE_CACHELINE_COMPENSATION = true");
            Thread.sleep(5000L);
            String sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time>'2015-08-27'";
            hasCompensationTest(c, sql, true);

            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time1>'2015-08-27'";
            hasCompensationTest(c, sql, true);

            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time2>'2015-08-27'";
            hasCompensationTest(c, sql, true);

            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time2='2015-08-27'";
            hasCompensationTest(c, sql, true);

            sql = "explain cost_trace select 1 from STAT_TEST_tB"
                + " where create_time1 between '2015-08-22' and '2015-09-29'";
            hasCompensationTest(c, sql, true);

            c.createStatement().execute("set global ENABLE_CACHELINE_COMPENSATION = false");
            Thread.sleep(5000);
            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time>'2015-08-27'";
            hasCompensationTest(c, sql, false);

            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time1>'2015-08-27'";
            hasCompensationTest(c, sql, false);

            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time2>'2015-08-27'";
            hasCompensationTest(c, sql, false);

            // blacklist check
            c.createStatement().execute("set global ENABLE_CACHELINE_COMPENSATION = true");
            c.createStatement().execute(
                "set global cacheline_compensation_blacklist = "
                    + "'stat_test_db.stat_test_tb.create_time;stat_test_db.stat_test_tb.create_time2;'");
            Thread.sleep(5000);
            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time>'2015-08-27'";
            hasCompensationTest(c, sql, false);

            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time1>'2015-08-27'";
            hasCompensationTest(c, sql, true);

            sql = "explain cost_trace select 1 from STAT_TEST_tB where create_time2>'2015-08-27'";
            hasCompensationTest(c, sql, false);
        } finally {
            try (Connection connection = getPolardbxConnection0()) {
                connection.createStatement().execute("set global ENABLE_CACHELINE_COMPENSATION = true");
                connection.createStatement().execute("set global cacheline_compensation_blacklist = ''");
            }
        }
    }

    @Test
    public void testBigIn() throws SQLException, IOException {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            StringBuilder sql =
                new StringBuilder("explain cost_trace select 1 from STAT_TEST_tB where create_time IN (");
            for (int i = 0; i < 10000; i++) {
                sql.append("'a',");
            }
            sql.setLength(sql.length() - 1);
            sql.append(")");
            ResultSet rs = c.createStatement().executeQuery(sql.toString());
            StringBuilder result = new StringBuilder();
            while (rs.next()) {
                result.append(rs.getString(1)).append("\n");
            }
            rs.close();
            Map<String, String> rsMap = GeneralUtil.decode(result.toString());
            System.out.println(rsMap);
            assert "50000".equals(rsMap.get(
                "Catalog:STAT_TEST_DB,STAT_TEST_TB,create_time,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...\nAction:getFrequency"));
        }
    }

    private void hasCompensationTest(Connection c, String sql, boolean shouldHas) throws SQLException, IOException {
        boolean hasCompensation = false;
        ResultSet rs = c.createStatement().executeQuery(sql);
        while (rs.next()) {
            String line = rs.getString(1);
            if (line.startsWith("STATISTIC TRACE INFO:")) {
                Map<String, String> traceInfo = GeneralUtil.decode(line);
                for (String key : traceInfo.keySet()) {
                    if (key.contains("Action:datetimeTypeCompensation")) {
                        hasCompensation = true;
                        String value = traceInfo.get(key);
                        assert Long.parseLong(value) > 1000;
                        break;
                    }
                }
                break;
            }

        }
        rs.close();
        assert shouldHas ? hasCompensation : !hasCompensation;
    }
}
