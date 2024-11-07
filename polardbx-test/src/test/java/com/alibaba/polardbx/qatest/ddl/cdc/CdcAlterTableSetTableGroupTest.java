package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
public class CdcAlterTableSetTableGroupTest extends CdcBaseTest {

    @Test
    public void testAlterTableSetTableGroup() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_set_tg_test");
            stmt.executeUpdate("create database cdc_set_tg_test mode = 'auto'");
            stmt.executeUpdate("use cdc_set_tg_test");

            testAlterTableSetTableGroup(stmt, "cdc_set_tg_test");
        }
    }

    @Test
    @Ignore
    public void testAlterTableSetTableGroup_ForBroadcast() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_set_bdtg_test");
            stmt.executeUpdate("create database cdc_set_bdtg_test mode = 'auto'");
            stmt.executeUpdate("use cdc_set_bdtg_test");

            for (int i = 0; i < 100; i++) {
                testAlterTableSetTableGroup_ForBroadcast("cdc_set_bdtg_test");
            }
        }
    }

    @Test
    public void testAlterTableGroupAddTables() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_add_tg_test");
            stmt.executeUpdate("create database cdc_add_tg_test mode = 'auto'");
            stmt.executeUpdate("use cdc_add_tg_test");

            testAlterTableGroupAddTables(stmt, "cdc_add_tg_test");
        }
    }

    @Test
    public void testAlterGsiSetTableGroup() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_set_idx_tg_test");
            stmt.executeUpdate("create database cdc_set_idx_tg_test mode = 'auto'");
            stmt.executeUpdate("use cdc_set_idx_tg_test");

            testAlterGsiSetTableGroup(stmt, "cdc_set_idx_tg_test");
        }
    }

    private void testAlterTableSetTableGroup(Statement stmt, String schemaName) throws SQLException {
        stmt.executeUpdate(
            "CREATE TABLE tb1(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb2(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb3(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100),\n"
                + "  PARTITION p3 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb4(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb4_1(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p88 VALUES LESS THAN(20),\n"
                + "  PARTITION p99 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb4_2(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p88 VALUES LESS THAN(20),\n"
                + "  PARTITION p99 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb5(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb6(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb7(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100),\n"
                + "  PARTITION p3 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE tb8(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100),\n"
                + "  PARTITION p33 VALUES LESS THAN(200)\n"
                + ")");

        stmt.executeUpdate(
            "CREATE TABLE tb9(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100),\n"
                + "  PARTITION p33 VALUES LESS THAN(200)\n"
                + ")");

        // 创建表组
        executeAndCheck(schemaName, stmt, "create tablegroup mytg1", "mytg1", false, null, null);

        // 首张表，打标一次
        executeAndCheck(schemaName, stmt, "alter table tb1 set tablegroup = mytg1", "tb1", false,
            "ALTER_TABLE_SET_TABLEGROUP", null);

        // 分区规则一模一样，打标一次
        executeAndCheck(schemaName, stmt, "alter table tb2 set tablegroup = mytg1", "tb2", false,
            "ALTER_TABLE_SET_TABLEGROUP", null);

        // 分区数量不一样，打标一次，且topology不能为空
        executeAndCheck(schemaName, stmt, "alter table tb3 set tablegroup = mytg1 force", "tb3", true,
            "ALTER_TABLE_SET_TABLEGROUP", item -> {
                Assert.assertEquals(item.countBefore1 + 2, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("alter table tb3 partition align to mytg1", item.listAfter1.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE", item.listAfter1.get(1).getSqlKind());

                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter1.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter1.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter1.get(1).getVisibility());
            });

        // 分区名字不一样，打标一次
        executeAndCheck(schemaName, stmt, "alter table tb4 set tablegroup = mytg1 force", "tb4", false,
            "ALTER_TABLE_SET_TABLEGROUP", item -> {
                Assert.assertEquals(item.countBefore1 + 2, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("ALTER TABLE tb4\n"
                    + "\tSET tablegroup = mytg1 FORCE", item.listAfter1.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(1).getSqlKind());

                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter1.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter1.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter1.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt, "alter table tb4_1 set tablegroup = mytg1 force", "tb4_1", false,
            "ALTER_TABLE_SET_TABLEGROUP", item -> {
                Assert.assertEquals(item.countBefore1 + 2, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("ALTER TABLE tb4_1\n"
                    + "\tSET tablegroup = mytg1 FORCE", item.listAfter1.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(1).getSqlKind());

                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter1.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter1.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter1.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt, "alter table tb4_2 set tablegroup = mytg1 force", "tb4_2", false,
            "ALTER_TABLE_SET_TABLEGROUP", item -> {
                Assert.assertEquals(item.countBefore1 + 2, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("ALTER TABLE tb4_2\n"
                    + "\tSET tablegroup = mytg1 FORCE", item.listAfter1.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(1).getSqlKind());

                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter1.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter1.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter1.get(1).getVisibility());
            });

        // move partitions
        Pair<String, String> pair = getMovePartitionInfo("tb1");
        stmt.executeUpdate("alter tablegroup mytg1 move partitions " + pair.getKey() + " to '" + pair.getValue() + "'");

        // 验证分区分布不一样时，不使用force会报错
        try {
            executeAndCheck(schemaName, stmt, "alter table tb5 set tablegroup = mytg1", "tb5", false,
                "ALTER_TABLE_SET_TABLEGROUP", null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "the physical location of"));
        }

        // 仅分区分布不一样，打标一次
        executeAndCheck(schemaName, stmt, "alter table tb5 set tablegroup = mytg1 force", "tb5", true,
            "ALTER_TABLE_SET_TABLEGROUP", null);

        // 分区分布不一样，且分区名字不一样，打标一次
        executeAndCheck(schemaName, stmt, "alter table tb6 set tablegroup = mytg1 force", "tb6", true,
            "ALTER_TABLE_SET_TABLEGROUP", item -> {
                Assert.assertEquals(item.countBefore1 + 2, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("ALTER TABLE tb6\n"
                    + "\tSET tablegroup = mytg1 FORCE", item.listAfter1.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(1).getSqlKind());

                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter1.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter1.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter1.get(1).getVisibility());
            });

        // 分区分布不一样，且分区数量不一样，打标一次
        executeAndCheck(schemaName, stmt, "alter table tb7 set tablegroup = mytg1 force", "tb7", true,
            "ALTER_TABLE_SET_TABLEGROUP", item -> {
                Assert.assertEquals(item.countBefore1 + 2, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("alter table tb7 partition align to mytg1", item.listAfter1.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE", item.listAfter1.get(1).getSqlKind());

                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter1.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter1.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter1.get(1).getVisibility());
            });

        // 分区分布不一样，且分区数量不一样，且分区名字不一样，打标一次
        executeAndCheck(schemaName, stmt, "alter table tb8 set tablegroup = mytg1 force", "tb8", true,
            "ALTER_TABLE_SET_TABLEGROUP", item -> {
                Assert.assertEquals(item.countBefore1 + 2, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("alter table tb8 partition align to mytg1", item.listAfter1.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE", item.listAfter1.get(1).getSqlKind());

                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter1.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter1.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter1.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter1.get(1).getVisibility());
            }
        );

        executeAndCheck(schemaName, stmt, "alter table tb9 set tablegroup = ''", "tb9", false,
            "ALTER_TABLE_SET_TABLEGROUP", null, true);
    }

    private void testAlterTableSetTableGroup_ForBroadcast(String schemaName) {
        String tableName = "b5oDS" + RandomUtils.nextLong();
        String sql1 = "create table if not exists t_broadcast1(id bigint primary key) broadcast";
        String sql2 = "CREATE TABLE  `" + tableName + "` ( "
            + "`mDWkT6` BIGINT    NULL COMMENT 'ar', "
            + "`wdyOP` TIMESTAMP   COMMENT 'k', "
            + "`zpwUjQc9U` SMALLINT  UNSIGNED   , "
            + "`d5FH9l` MEDIUMINT  UNSIGNED   COMMENT 'Ul', "
            + "`8` CHAR     COMMENT '4aDrxnFx8dz', "
            + "`7` DATE NOT NULL COMMENT '6vbpP6mX' UNIQUE KEY, "
            + "`Nb6yT` INT ( 1 )   NULL COMMENT '2z', "
            + "`wu` INT ( 4 )   NOT NULL , "
            + "KEY `d1ZahLZL`  ( `d5FH9l`, `7`  ) ) "
            + "PARTITION BY LIST ( `mDWkT6` ) ( "
            + "     PARTITION `nfuwVhEp` VALUES IN ( 90 , 30 )   , "
            + "     PARTITION `iCA1ChLJt` VALUES IN ( 34 , 94 ) , "
            + "     PARTITION `PVAjR39k` VALUES IN ( 59 , 7 ))";
        String sql3 = "CREATE TABLEGROUP IF NOT EXISTS `xbvBwnqc91`";
        String sql4 = "ALTER TABLE `" + tableName + "` BROADCAST";
        String sql5 = "ALTER TABLE `" + tableName + "` set tablegroup= `xbvBwnqc91`";

        JdbcUtil.executeUpdate(tddlConnection, sql1);
        JdbcUtil.executeUpdate(tddlConnection, sql2);
        JdbcUtil.executeUpdate(tddlConnection, sql3);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> future1 = executor.submit(() -> JdbcUtil.executeUpdate(tddlConnection, sql4));
        Future<?> future2 = executor.submit(() -> JdbcUtil.executeUpdate(tddlConnection, sql5));
        try {
            future2.get();
            Assert.fail("this sql should failed by reason: change tablegroup of broadcast table is not allow."
                + " sql: " + sql5);
        } catch (ExecutionException | InterruptedException ignored) {
        }
    }

    private void testAlterTableGroupAddTables(Statement stmt, String schemaName) throws SQLException {
        Map<String, String> map = getMasterGroupStorageMap();
        Optional<Map.Entry<String, String>> randomEntry = map.entrySet()
            .stream()
            .skip((int) (map.size() * Math.random()))
            .findFirst();

        String tableName1 = "tb_add_1";
        String tableName2 = "tb_add_2";
        String tableName3 = "tb_add_3";
        String tableName4 = "tb_add_4";
        String tableName5 = "tb_add_5";
        String tableName5_1 = "tb_add_5_1";
        String tableName5_2 = "tb_add_5_2";
        String tableName6 = "tb_add_6";
        String tableName7 = "tb_add_7";
        String tableName8 = "tb_add_8";
        String tableName9 = "tb_add_9";
        String tableName10 = "tb_add_10";
        String tableName11 = "tb_add_11";
        String tableName12 = "tb_add_12";
        String tableName13 = "tb_add_13";
        String tableName14 = "tb_add_14";
        String tableName15 = "tb_add_15";

        // same partition rule
        stmt.executeUpdate(
            "CREATE TABLE " + tableName1 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName2 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName3 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");

        // different partition rule for single add
        // 1. different partition count
        // 2. different partition name
        // 3. different locality
        stmt.executeUpdate(
            "CREATE TABLE " + tableName4 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100),\n"
                + "  PARTITION p3 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName5 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName5_1 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p88 VALUES LESS THAN(20),\n"
                + "  PARTITION p99 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName5_2 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p88 VALUES LESS THAN(20),\n"
                + "  PARTITION p99 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName6 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100),\n"
                + "  PARTITION p23 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName7 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100),\n"
                + "  PARTITION p3 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName8 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100),\n"
                + "  PARTITION p33 VALUES LESS THAN(200)\n"
                + ")");

        // different partition rule for batch add
        // 1. same partition rule
        // 2. different partition count
        // 3. different partition name
        // 4. different locality
        stmt.executeUpdate(
            "CREATE TABLE " + tableName9 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName10 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName11 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100),\n"
                + "  PARTITION p3 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName12 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName13 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100),\n"
                + "  PARTITION p33 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName14 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100),\n"
                + "  PARTITION p3 VALUES LESS THAN(200)\n"
                + ")");
        stmt.executeUpdate(
            "CREATE TABLE " + tableName15 + "(a int) PARTITION BY RANGE(a)\n"
                + " (\n"
                + "  PARTITION p11 VALUES LESS THAN(20),\n"
                + "  PARTITION p22 VALUES LESS THAN(100),\n"
                + "  PARTITION p33 VALUES LESS THAN(200)\n"
                + ")");

        Pair<String, String> pair = getMovePartitionInfo(tableName7);
        stmt.executeUpdate("alter table " + tableName7 +
            " move partitions " + pair.getKey() + " to '" + pair.getValue() + "'");

        pair = getMovePartitionInfo(tableName8);
        stmt.executeUpdate("alter table " + tableName8 +
            " move partitions " + pair.getKey() + " to '" + pair.getValue() + "'");

        pair = getMovePartitionInfo(tableName14);
        stmt.executeUpdate("alter table " + tableName14 +
            " move partitions " + pair.getKey() + " to '" + pair.getValue() + "'");

        pair = getMovePartitionInfo(tableName15);
        stmt.executeUpdate("alter table " + tableName15 +
            " move partitions " + pair.getKey() + " to '" + pair.getValue() + "'");

        executeAndCheck(schemaName, stmt, "create tablegroup mytg2",
            "mytg2", false, null, null);

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s", tableName1),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", null);

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s,%s", tableName2, tableName3),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", null);

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s force", tableName4),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", item -> {
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter2.get(0).getDdlSql());
                Assert.assertEquals("alter table tb_add_4 partition align to mytg2",
                    item.listAfter2.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", item.listAfter2.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE", item.listAfter2.get(1).getSqlKind());

                Assert.assertNull(item.listAfter2.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter2.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter2.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter2.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s force", tableName5),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", item -> {
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter2.get(0).getDdlSql());
                Assert.assertEquals("/*+TDDL:CMD_EXTRA(SKIP_TABLEGROUP_VALIDATOR=true)*/\n"
                        + "ALTER TABLE tb_add_5\n"
                        + "\tSET tablegroup = mytg2 FORCE",
                    item.listAfter2.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", item.listAfter2.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter2.get(1).getSqlKind());

                Assert.assertNull(item.listAfter2.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter2.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter2.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter2.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s force", tableName5_1),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", item -> {
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter2.get(0).getDdlSql());
                Assert.assertEquals("/*+TDDL:CMD_EXTRA(SKIP_TABLEGROUP_VALIDATOR=true)*/\n"
                        + "ALTER TABLE tb_add_5_1\n"
                        + "\tSET tablegroup = mytg2 FORCE",
                    item.listAfter2.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", item.listAfter2.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter2.get(1).getSqlKind());

                Assert.assertNull(item.listAfter2.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter2.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter2.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter2.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s force", tableName5_2),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", item -> {
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter2.get(0).getDdlSql());
                Assert.assertEquals("/*+TDDL:CMD_EXTRA(SKIP_TABLEGROUP_VALIDATOR=true)*/\n"
                        + "ALTER TABLE tb_add_5_2\n"
                        + "\tSET tablegroup = mytg2 FORCE",
                    item.listAfter2.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", item.listAfter2.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter2.get(1).getSqlKind());

                Assert.assertNull(item.listAfter2.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter2.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter2.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter2.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s force", tableName6),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", item -> {
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter2.get(0).getDdlSql());
                Assert.assertEquals("alter table tb_add_6 partition align to mytg2",
                    item.listAfter2.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", item.listAfter2.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE", item.listAfter2.get(1).getSqlKind());

                Assert.assertNull(item.listAfter2.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter2.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter2.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter2.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s force", tableName7),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", item -> {
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter2.get(0).getDdlSql());
                Assert.assertEquals("alter table tb_add_7 partition align to mytg2",
                    item.listAfter2.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", item.listAfter2.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE", item.listAfter2.get(1).getSqlKind());

                Assert.assertNull(item.listAfter2.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter2.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter2.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter2.get(1).getVisibility());
            });

        executeAndCheck(schemaName, stmt,
            String.format("ALTER TABLEGROUP mytg2 ADD TABLES %s force", tableName8),
            "mytg2", false, "ALTER_TABLEGROUP_ADD_TABLE", item -> {
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 2, item.listAfter2.size());

                Assert.assertEquals(item.sql, item.listAfter2.get(0).getDdlSql());
                Assert.assertEquals("alter table tb_add_8 partition align to mytg2",
                    item.listAfter2.get(1).getDdlSql());

                Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", item.listAfter2.get(0).getSqlKind());
                Assert.assertEquals("ALTER_TABLE", item.listAfter2.get(1).getSqlKind());

                Assert.assertNull(item.listAfter2.get(0).getMetaInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getMetaInfo());

                Assert.assertNotNull(item.listAfter2.get(0).getDdlExtInfo());
                Assert.assertNotNull(item.listAfter2.get(1).getDdlExtInfo());

                Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(),
                    item.listAfter2.get(0).getVisibility());
                Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(),
                    item.listAfter2.get(1).getVisibility());
            });

        executeAndCheck2(schemaName, stmt,
            String.format("alter tablegroup mytg2 add tables %s,%s,%s,%s,%s,%s,%s force",
                tableName9, tableName10, tableName11, tableName12, tableName13, tableName14, tableName15), "mytg2");
    }

    private void testAlterGsiSetTableGroup(Statement stmt, String schemaName) throws SQLException {
        String tableName1 = "t_gsi_test_1_" + RandomUtils.nextLong();
        String tableName2 = "t_gsi_test_2_" + RandomUtils.nextLong();
        String tableName3 = "t_gsi_test_3_" + RandomUtils.nextLong();
        String tableName4 = "t_gsi_test_4_" + RandomUtils.nextLong();
        String tableName5 = "t_gsi_test_5_" + RandomUtils.nextLong();
        String tableName6 = "t_gsi_test_6_" + RandomUtils.nextLong();
        String tableName7 = "t_gsi_test_7_" + RandomUtils.nextLong();
        String tableName8 = "t_gsi_test_8_" + RandomUtils.nextLong();

        stmt.executeUpdate("CREATE TABLE " + tableName1 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid),\n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 4"
            + ")");
        stmt.executeUpdate("CREATE TABLE " + tableName2 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid), \n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 4"
            + ")");
        stmt.executeUpdate("CREATE TABLE " + tableName3 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid), \n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 10"
            + ")");
        stmt.executeUpdate("CREATE TABLE " + tableName4 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid), \n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 9"
            + ")");
        stmt.executeUpdate("CREATE TABLE " + tableName5 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid), \n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 9"
            + ")");
        stmt.executeUpdate("CREATE TABLE " + tableName6 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid), \n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 4"
            + ")");
        stmt.executeUpdate("CREATE TABLE " + tableName7 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid), \n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 4"
            + ")");
        stmt.executeUpdate("CREATE TABLE " + tableName8 + "(\n"
            + " id bigint not null auto_increment,\n"
            + " bid bigint,\n"
            + " name varchar(30),\n"
            + " primary key(id),\n"
            + " index idx_bid (bid), \n"
            + " global index idx_name(name) PARTITION BY KEY (`name`) PARTITIONS 7"
            + ")");

        executeAndCheck(schemaName, stmt, "create tablegroup mytg3", "mytg3", false, null, null);
        executeAndCheck(schemaName, stmt, "create tablegroup mytg4", "mytg4", false, null, null);
        executeAndCheck(schemaName, stmt, "create tablegroup mytg5", "mytg5", false, null, null);

        executeAndCheck(schemaName, stmt, String.format("ALTER TABLEGROUP mytg3 ADD TABLES %s,%s",
            tableName1, tableName2), "mytg3", false, "ALTER_TABLEGROUP_ADD_TABLE", null);

        executeAndCheck(schemaName, stmt, String.format("ALTER TABLEGROUP mytg4 ADD TABLES %s,%s",
            tableName1 + ".idx_bid", tableName2 + ".idx_bid"), "mytg4", false, "ALTER_TABLEGROUP_ADD_TABLE", null);

        executeAndCheck(schemaName, stmt, "alter table " + tableName1 + ".idx_name set tablegroup = mytg5",
            tableName1, false, null, null);

        executeAndCheck(schemaName, stmt, "alter table " + tableName2 + ".idx_name set tablegroup = mytg5",
            tableName2, false, null, null);

        // 不再支持GSI：it's not support to change the GSI's tablegroup to mytg5 due to need repartition
        /*executeAndCheck(schemaName, stmt, "alter table " + tableName3 + ".idx_name set tablegroup = mytg5 force",
            tableName3, false, null, null);*/

        // 不再支持GSI：it's not support to change the GSI's tablegroup to mytg5 due to need repartition
        /*executeAndCheck(schemaName, stmt, String.format("alter tablegroup mytg5 add tables %s,%s,%s force",
            tableName4 + ".idx_name", tableName5 + ".idx_name", tableName6 + ".idx_name"), "mytg5", false, null, null);*/

        String gsiTableName4Table7 = getTableNameForGsi(schemaName, tableName7, "idx_name");
        executeAndCheck(schemaName, stmt, "alter table " + gsiTableName4Table7 + " set tablegroup = mytg5",
            tableName7, false, null, item -> {
                String s = "ALTER TABLE " + tableName7 + ".idx_name SET tablegroup = mytg5";
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 1, item.listAfter2.size());
                Assert.assertEquals(s, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
            });

        // 不再支持GSI：it's not support to change the GSI's tablegroup to mytg5 due to need repartition
        /*String gsiTableName4Table8 = getTableNameForGsi(schemaName, tableName8, "idx_name");
        executeAndCheck(schemaName, stmt, "alter table " + gsiTableName4Table8 + " set tablegroup = mytg5 force",
            tableName8, false, null, item -> {
                String s = "ALTER TABLE " + tableName8 + ".idx_name SET tablegroup = mytg5 FORCE";
                Assert.assertEquals(item.countBefore1 + 1, item.listAfter1.size());
                Assert.assertEquals(item.countBefore2 + 1, item.listAfter2.size());
                Assert.assertEquals(s, item.listAfter1.get(0).getDdlSql());
                Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", item.listAfter1.get(0).getSqlKind());
                Assert.assertNull(item.listAfter1.get(0).getMetaInfo());
            });*/
    }

    private String getTableNameForGsi(String dbName, String tableName, String gsiName) throws SQLException {
        List<String> list = getGsiList(dbName, tableName);
        return list.stream().filter(i -> i.startsWith(gsiName)).findFirst().get();
    }

    private void executeAndCheck(String schemaName, Statement stmt, String sql, String tableName, boolean hasTopology,
                                 String sqlKind,
                                 Consumer<Item> consumer) throws SQLException {
        executeAndCheck(schemaName, stmt, sql, tableName, hasTopology, sqlKind, consumer, false);
    }

    private void executeAndCheck(String schemaName, Statement stmt, String sql, String tableName, boolean hasTopology,
                                 String sqlKind,
                                 Consumer<Item> consumer, boolean skipCheckSqlText)
        throws SQLException {
        int countBefore1 = getDdlRecordInfoList(schemaName, tableName).size();
        int countBefore2 = getDdlRecordInfoList(schemaName, null).size();

        stmt.executeUpdate(sql);

        List<DdlRecordInfo> listAfter1 = getDdlRecordInfoList(schemaName, tableName);
        List<DdlRecordInfo> listAfter2 = getDdlRecordInfoList(schemaName, null);

        if (consumer != null) {
            Item item = Item.builder()
                .countBefore1(countBefore1)
                .countBefore2(countBefore2)
                .sql(sql)
                .listAfter1(listAfter1)
                .listAfter2(listAfter2).build();
            consumer.accept(item);
        } else {
            Assert.assertEquals(countBefore1 + 1, listAfter1.size());
            Assert.assertEquals(countBefore2 + 1, listAfter2.size());
            if (!skipCheckSqlText) {
                Assert.assertEquals(sql, listAfter1.get(0).getDdlSql());
            } else {
                Assert.assertTrue(listAfter1.get(0).getDdlSql().toLowerCase().indexOf("implicit") != -1);
            }
            if (StringUtils.isNotBlank(sqlKind)) {
                Assert.assertEquals(sqlKind, listAfter1.get(0).getSqlKind());
            }
            Assert.assertEquals(hasTopology, listAfter1.get(0).getMetaInfo() != null);
        }
    }

    private void executeAndCheck2(String schemaName, Statement stmt, String ddl, String tableName) throws SQLException {
        int countBefore1 = getDdlRecordInfoList(schemaName, tableName).size();
        int countBefore2 = getDdlRecordInfoList(schemaName, null).size();

        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        List<DdlRecordInfo> listAfter1 = getDdlRecordInfoList(schemaName, tableName);
        List<DdlRecordInfo> listAfter2 = getDdlRecordInfoList(schemaName, null);

        Assert.assertEquals(countBefore1 + 1, listAfter1.size());
        Assert.assertEquals(countBefore2 + 6, listAfter2.size());

        Assert.assertEquals("ALTER_TABLEGROUP_ADD_TABLE", listAfter2.get(0).getSqlKind());
        Assert.assertEquals(tableName, listAfter2.get(0).getTableName());
        Assert.assertEquals(sql, listAfter1.get(0).getDdlSql());
        Assert.assertEquals(sql, listAfter2.get(0).getDdlSql());
        Assert.assertNull(listAfter2.get(0).getMetaInfo());
        Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(), listAfter2.get(0).getVisibility());

        String tab1 = listAfter2.get(1).getTableName();
        String tab2 = listAfter2.get(2).getTableName();
        String tab3 = listAfter2.get(3).getTableName();
        String tab4 = listAfter2.get(4).getTableName();
        String tab5 = listAfter2.get(5).getTableName();

        Assert.assertEquals(getSql(tab1), listAfter2.get(1).getDdlSql());
        Assert.assertEquals(getSql(tab2), listAfter2.get(2).getDdlSql());
        Assert.assertEquals(getSql(tab3), listAfter2.get(3).getDdlSql());
        Assert.assertEquals(getSql(tab4), listAfter2.get(4).getDdlSql());
        Assert.assertEquals(getSql(tab5), listAfter2.get(5).getDdlSql());

        Assert.assertNotNull(listAfter2.get(1).getMetaInfo());
        Assert.assertNotNull(listAfter2.get(2).getMetaInfo());
        Assert.assertNotNull(listAfter2.get(3).getMetaInfo());
        Assert.assertNotNull(listAfter2.get(4).getMetaInfo());
        Assert.assertNotNull(listAfter2.get(5).getMetaInfo());

        Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(), listAfter2.get(1).getVisibility());
        Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(), listAfter2.get(2).getVisibility());
        Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(), listAfter2.get(3).getVisibility());
        Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(), listAfter2.get(4).getVisibility());
        Assert.assertEquals(CdcDdlMarkVisibility.Private.getValue(), listAfter2.get(5).getVisibility());

        String kind1 = listAfter2.get(1).getSqlKind();
        String kind2 = listAfter2.get(2).getSqlKind();
        String kind3 = listAfter2.get(3).getSqlKind();
        String kind4 = listAfter2.get(4).getSqlKind();
        String kind5 = listAfter2.get(5).getSqlKind();
        checkSqlKind(tab1, kind1);
        checkSqlKind(tab2, kind2);
        checkSqlKind(tab3, kind3);
        checkSqlKind(tab4, kind4);
        checkSqlKind(tab5, kind5);

        Assert.assertEquals(Sets.newHashSet("tb_add_11", "tb_add_12", "tb_add_13", "tb_add_14", "tb_add_15"),
            Sets.newHashSet(tab1, tab2, tab3, tab4, tab5));
    }

    private String getSql(String tableName) {
        if (StringUtils.equalsAny(tableName, "tb_add_12")) {
            return "/*+TDDL:CMD_EXTRA(SKIP_TABLEGROUP_VALIDATOR=true)*/\n"
                + "ALTER TABLE tb_add_12\n"
                + "\tSET tablegroup = mytg2 FORCE";
        }
        if (StringUtils.equalsAny(tableName, "tb_add_11", "tb_add_13", "tb_add_14", "tb_add_15")) {
            return "alter table " + tableName + " partition align to mytg2";
        }

        throw new RuntimeException("invalid table name " + tableName);
    }

    private void checkSqlKind(String tableName, String sqlKind) {
        if ("tb_add_12".equalsIgnoreCase(tableName)) {
            Assert.assertEquals("ALTER_TABLE_SET_TABLEGROUP", sqlKind);
        } else if ("tb_add_11".equalsIgnoreCase(tableName)) {
            Assert.assertEquals("ALTER_TABLE", sqlKind);
        } else if ("tb_add_13".equalsIgnoreCase(tableName)) {
            Assert.assertEquals("ALTER_TABLE", sqlKind);
        } else if ("tb_add_14".equalsIgnoreCase(tableName)) {
            Assert.assertEquals("ALTER_TABLE", sqlKind);
        } else if ("tb_add_15".equalsIgnoreCase(tableName)) {
            Assert.assertEquals("ALTER_TABLE", sqlKind);
        } else {
            throw new RuntimeException("invalid table name" + tableName);
        }
    }

    private Pair<String, String> getMovePartitionInfo(String tableName) throws SQLException {
        // 变更分区分布
        Map<String, String> map = getMasterGroupStorageMap();
        String fromStorage = null;
        String toStorage = null;
        String partition = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String temp = getFirstLevelPartitionByGroupName(entry.getKey(), tableName);
            if (StringUtils.isNotBlank(temp)) {
                fromStorage = entry.getValue();
                partition = temp;
                break;
            }
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!StringUtils.equals(fromStorage, entry.getValue())) {
                toStorage = entry.getValue();
            }
        }

        return Pair.of(partition, toStorage);
    }

    @Getter
    @Setter
    @Builder
    private static class Item {
        int countBefore1;
        int countBefore2;
        String sql;
        List<DdlRecordInfo> listAfter1;
        List<DdlRecordInfo> listAfter2;
    }
}
