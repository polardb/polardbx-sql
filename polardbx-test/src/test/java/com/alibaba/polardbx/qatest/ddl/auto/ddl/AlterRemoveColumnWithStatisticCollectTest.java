package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * @author fangwu
 */
public class AlterRemoveColumnWithStatisticCollectTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog(BaseTestCase.class);

    private static String DB_NAME = "AlterRemoveStatisticTestDB";
    private static String TB_NAME = "AlterRemoveStatisticTestTB";

    private static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `l_i_order` (`order_id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`order_id`) partitions 16;";

    private static String ALTER_TABLE = "ALTER TABLE %s DROP COLUMN seller_id";

    private static String CHECK_TABLES =
        "SELECT TABLE_ROWS, from_unixtime(LAST_MODIFY_TIME) as UPDATE_TIME FROM INFORMATION_SCHEMA.virtual_statistic"
            + " WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s'";

    private static String INSERT_TABLE = "INSERT INTO %s (order_id, buyer_id, seller_id) VALUES(?, ?, ?)";

    private static String CHECK_STATISTIC =
        "SELECT COLUMN_NAME, GMT_MODIFIED, CARDINALITY, HISTOGRAM, TOPN FROM METADB.COLUMN_STATISTICS WHERE SCHEMA_NAME='%s' AND TABLE_NAME='%s'";

    private static String CHECK_STATISTIC_MEM =
        "SELECT COLUMN_NAME, CARDINALITY, HISTOGRAM, TOPN FROM INFORMATION_SCHEMA.VIRTUAL_STATISTIC WHERE SCHEMA_NAME='%s' AND TABLE_NAME='%s'";

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

            // analyze table and assert table rowcount == 0
            c.createStatement().execute("analyze table " + TB_NAME);
            ResultSet rs = c.createStatement().executeQuery(String.format(CHECK_TABLES, DB_NAME, TB_NAME));
            rs.next();
            int rowCount = rs.getInt("TABLE_ROWS");
            Timestamp date = rs.getTimestamp("UPDATE_TIME");

            assert rowCount > 0;

            // DROP COLUMN
            c.createStatement().execute(String.format(ALTER_TABLE, TB_NAME));

            // check table rowcount and update time
            rs = c.createStatement().executeQuery(String.format(CHECK_TABLES, DB_NAME, TB_NAME));
            rs.next();
            rowCount = rs.getInt("TABLE_ROWS");

            rs.close();
            assert rowCount > 0;

            // check statistics info in metadb
            rs = c.createStatement().executeQuery(String.format(CHECK_STATISTIC, DB_NAME, TB_NAME));
            rs.next();
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                assert !columnName.equalsIgnoreCase("seller_id");
            }

            rs.close();

            // check statistics info in mem
            rs = c.createStatement().executeQuery(String.format(CHECK_STATISTIC_MEM, DB_NAME, TB_NAME));
            rs.next();
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                assert !columnName.equalsIgnoreCase("seller_id");
            }

            rs.close();
        }
    }

}
