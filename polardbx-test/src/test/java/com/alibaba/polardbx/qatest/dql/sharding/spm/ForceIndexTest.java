package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;

/**
 * @author fangwu
 */
public class ForceIndexTest extends BaseTestCase {
    private static final String DB_NAME = "force_index_test";
    private static final String TBL_NAME1 = "t_order";
    private static final String TBL_NAME2 = "t_order1";
    private static final String TBL_NAME_SINGLE = "t_order_single";

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

    private static final String CREATE_SINGLE_TABLE_FORMATTED =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "    `id` BIGINT(11) NOT NULL AUTO_INCREMENT,\n"
            + "    `order_id` VARCHAR(20) DEFAULT NULL,\n"
            + "    `buyer_id` VARCHAR(20) DEFAULT NULL,\n"
            + "    `seller_id` VARCHAR(20) DEFAULT NULL,\n"
            + "    `order_snapshot` LONGTEXT,\n"
            + "    `order_detail` LONGTEXT,\n"
            + "    `test` VARCHAR(20) DEFAULT NULL,\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    KEY `l_i_order` (`order_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
            + "SINGLE";

    @BeforeClass
    public static void setUp() {
        String createDbSql = "CREATE DATABASE IF NOT EXISTS " + DB_NAME + " mode='auto'";
        try {
            // build db
            Connection conn = getPolardbxConnection0();
            conn.createStatement().execute(createDbSql);

            // build table
            conn.createStatement().execute("use " + DB_NAME);
            conn.createStatement().execute(String.format(CREATE_TABLE_FORMATTED, TBL_NAME1));
            conn.createStatement().execute(String.format(CREATE_TABLE_FORMATTED, TBL_NAME2));
            conn.createStatement().execute(String.format(CREATE_SINGLE_TABLE_FORMATTED, TBL_NAME_SINGLE));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void cleanUp() {
        try {
            // build db
            getPolardbxConnection0().createStatement().execute("drop database " + DB_NAME);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForceIndexSingleTable() throws SQLException {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql = "SELECT * FROM t_order_single force index(l_i_order) where buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("l_i_order"));

            sql = "/*TDDL:index(t_order_single, l_i_order)*/SELECT * FROM t_order_single where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("l_i_order"));

            sql = "/*TDDL:index(t_order_single, primary)*/SELECT * FROM t_order_single where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("force index(primary)"));

            sql =
                "/*TDDL:index(t_order_single, primary, primary)*/SELECT * FROM t_order_single where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("force index(primary)"));
        }
    }

    @Test
    public void testDNHint() throws SQLException {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql =
                "/*TDDL:dn_hint='test hint'*/SELECT * FROM t_order_single force index(l_i_order) where buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("/*+test hint*/"));

            sql =
                "/*TDDL:index(t_order_single, l_i_order) dn_hint='test hint' */SELECT * FROM t_order_single force index(g_i_buyer) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("/*+test hint*/"));
        }
    }

    @Test
    public void testForceIndexGsi() {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql = "SELECT * FROM t_order force index(g_i_seller) where buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("g_i_seller"));

            sql =
                "/*TDDL:index(t_order, g_i_seller)  */SELECT * FROM t_order force index(g_i_buyer) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("g_i_seller"));

            sql = "SELECT * FROM t_order force index(g_i_buyer) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("g_i_buyer"));

            sql =
                "/*TDDL:index(t_order, g_i_buyer) */SELECT * FROM t_order force index(primary) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("g_i_buyer"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForceIndexLocalIndex() {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql = "SELECT * FROM t_order force index(l_i_order) where buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(l_i_order)"));

            sql =
                "/*TDDL:index(t_order, l_i_order)*/ SELECT * FROM t_order force index(g_i_seller) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(l_i_order)"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForceIndexPrimary() {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql = "SELECT * FROM t_order force index(primary) where buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(primary)"));

            sql =
                "/*TDDL:index(t_order, primary)*/SELECT * FROM t_order force index(g_i_buyer) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(primary)"));

            sql = "SELECT * FROM t_order force index(primary.primary) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(primary)"));

            sql =
                "/*TDDL:index(t_order, primary, primary)*/SELECT * FROM t_order force index(g_i_buyer) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(primary)"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForceIndexGsiLocalIndex() {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql = "SELECT * FROM t_order force index(g_i_buyer.primary) where buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(primary)"));

            sql =
                "/*TDDL:index(t_order, g_i_seller, primary)*/ SELECT * FROM t_order force index(g_i_buyer.primary) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(primary)"));

            sql = "SELECT * FROM t_order force index(g_i_buyer.auto_shard_key_buyer_id) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(auto_shard_key_buyer_id)"));

            sql =
                "/*TDDL:index(t_order, g_i_buyer, auto_shard_key_buyer_id)*/ SELECT * FROM t_order force index(g_i_seller.primary) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(auto_shard_key_buyer_id)"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForceIndexGsiByForceLocalIndex() {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql =
                "SELECT * FROM t_order force index(g_i_buyer.auto_shard_key_buyer_id) where buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(auto_shard_key_buyer_id)"));

            sql =
                "/*TDDL:index(t_order, g_i_seller, auto_shard_key_seller_id)*/ SELECT * FROM t_order force index(g_i_buyer.primary) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("g_i_seller"));
            assertTrue(!explain.contains("g_i_buyer"));

            sql = "SELECT * FROM t_order force index(auto_shard_key_buyer_id) where buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("g_i_seller"));
            assertTrue(explain.contains("g_i_buyer"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForceIndexUnPushJoin() {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql =
                "SELECT * FROM t_order t1 force index(g_i_buyer.primary) join  t_order1 t2 force index(primary) on t1.id=t2.id where t1.buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("indexscan(tables=\"g_i_seller_"));
            assertTrue(explain.contains("indexscan(tables=\"g_i_buyer_"));
            assertTrue(explain.contains("force index(primary)"));

            sql = "/*TDDL:index(t_order, g_i_buyer, primary) index(t_order1, g_i_seller, auto_shard_key_seller_id)*/"
                + "SELECT * FROM t_order t1 force index(primary) join  t_order1 t2 force index(primary) on t1.id=t2.id where t1.buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(explain.contains("g_i_seller"));
            assertTrue(explain.contains("g_i_buyer"));
            assertTrue(explain.contains("force index(auto_shard_key_seller_id)"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForceIndexPushedJoin() {
        try (Connection c = getPolardbxConnection0(DB_NAME)) {
            String sql =
                "SELECT * FROM t_order t1 force index(g_i_buyer.primary) join t_order1 t2 force index(primary) on t1.order_id=t2.order_id where t1.buyer_id = '123456'";

            // test explain
            String explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("indexscan(tables=\"g_i_seller_"));
            assertTrue(explain.contains("indexscan(tables=\"g_i_buyer_"));
            assertTrue(explain.contains("force index(primary)"));

            sql =
                "SELECT * FROM t_order t1 force index(l_i_order) join t_order1 t2 force index(primary) on t1.order_id=t2.order_id where t1.buyer_id = '123456'";

            // test explain
            explain = getExplainResult(c, sql).toLowerCase();
            assertTrue(!explain.contains("indexscan(tables=\"g_i_seller_"));
            assertTrue(!explain.contains("indexscan(tables=\"g_i_buyer_"));
            assertTrue(explain.contains("force index(primary)"));
            assertTrue(explain.contains("force index(l_i_order)"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
