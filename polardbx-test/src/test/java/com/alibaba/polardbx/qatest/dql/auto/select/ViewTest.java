package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * @author fangwu
 */
public class ViewTest extends BaseTestCase {
    private static final String DB_NAME = "VIEW_TEST_DB";
    private static final String TB_NAME = "VIEW_TEST_TB";

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

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute(String.format(CREATE_TABLE, TB_NAME));
        }
    }

    @AfterClass
    public static void clean() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
        }
    }

    @Test
    public void testViewAlter() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String sql = "create view view_test1 as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            sql = "select * from view_test1";
            c.createStatement().executeQuery(sql);

            ResultSet rs = c.createStatement().executeQuery("explain " + sql);
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
            }
            assert sb.toString().contains("Source:PLAN_CACHE");

            c.createStatement().execute("alter view view_test1 as select 1 from " + TB_NAME + " limit 1");
            rs = c.createStatement().executeQuery("explain " + sql);
            sb.setLength(0);
            while (rs.next()) {
                sb.append(rs.getString(1));
            }
            assert sb.toString().contains("Source:PLAN_CACHE");
            assert sb.toString().contains("HitCache:false");
        }
    }

    @Test
    public void testViewFix() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            c.createStatement().execute("drop view if exists view_test1");
            String sql = "create view view_test1 as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            sql = " select * from view_test1";
            c.createStatement().executeQuery("baseline fix sql /*TDDL:a()*/" + sql);

            ResultSet rs = c.createStatement().executeQuery("explain " + sql);
            StringBuilder sb = new StringBuilder();
            String source = "";
            String baselineId1 = "";
            String planId1 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId1 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId1 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX");

            c.createStatement().execute("alter view view_test1 as select 1 from " + TB_NAME + " limit 1");
            rs = c.createStatement().executeQuery("explain " + sql);
            sb.setLength(0);
            String baselineId2 = "";
            String planId2 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId2 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId2 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE");
            assert baselineId1.equals(baselineId2);
            assert !planId2.equals(planId1);
        }
    }

    @Test
    public void testViewFixWhenTableChange() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String sql = "create view view_test2 as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            sql = " select * from view_test2";
            c.createStatement().executeQuery("baseline fix sql /*TDDL:a()*/" + sql);

            ResultSet rs = c.createStatement().executeQuery("explain " + sql);
            StringBuilder sb = new StringBuilder();
            String source = "";
            String baselineId1 = "";
            String planId1 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId1 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId1 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX");

            c.createStatement().execute("alter table " + TB_NAME + " add column create_time3 date");
            rs = c.createStatement().executeQuery("explain " + sql);
            sb.setLength(0);
            String baselineId2 = "";
            String planId2 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId2 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId2 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX_DDL_HASHCODE_UPDATE");
            assert baselineId1.equals(baselineId2);
            assert !planId2.equals(planId1);
        }
    }

    @Test
    public void testViewCreateEnable() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            c.createStatement().execute("set global ENABLE_CREATE_VIEW=false");
            String sql = "create view view_test_enable as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            Assert.fail("not found logicalview");
        } catch (Exception e) {
            e.printStackTrace();
            if (!e.getMessage().contains("CREATE VIEW is not ENABLED")) {
                Assert.fail("ENABLE_CREATE_VIEW is false");
            }
        } finally {
            try (Connection c = getPolardbxConnection(DB_NAME)) {
                c.createStatement().execute("set global ENABLE_CREATE_VIEW=true");
            }
        }
    }
}