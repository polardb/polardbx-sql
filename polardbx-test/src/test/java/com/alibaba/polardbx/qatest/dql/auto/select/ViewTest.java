package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.common.utils.GeneralUtil;
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
            assert sb.toString().contains("HitCache:true");

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
}