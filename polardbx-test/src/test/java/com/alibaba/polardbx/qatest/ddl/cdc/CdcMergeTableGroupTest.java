package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-10-17 14:19
 **/
public class CdcMergeTableGroupTest extends CdcBaseTest {

    @Test
    public void testMergeTableGroup() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_merge_tg_test");
            stmt.executeUpdate("create database cdc_merge_tg_test mode = 'auto'");
            stmt.executeUpdate("use cdc_merge_tg_test");

            stmt.executeUpdate("create tablegroup mytg1");
            stmt.executeUpdate("create tablegroup mytg2");
            stmt.executeUpdate("create tablegroup mytg3");
            stmt.executeUpdate(
                "CREATE TABLE tb0(a int) PARTITION BY RANGE(a)\n"
                    + " (\n"
                    + "  PARTITION p1 VALUES LESS THAN(20),\n"
                    + "  PARTITION p2 VALUES LESS THAN(100)\n"
                    + ") tablegroup = mytg1");
            stmt.executeUpdate(
                "CREATE TABLE tb1(a int) PARTITION BY RANGE(a)\n"
                    + " (\n"
                    + "  PARTITION p1 VALUES LESS THAN(20),\n"
                    + "  PARTITION p2 VALUES LESS THAN(100)\n"
                    + ") tablegroup = mytg1");
            stmt.executeUpdate(
                "CREATE TABLE tb2(a int) PARTITION BY RANGE(a)\n"
                    + " (\n"
                    + "  PARTITION p1 VALUES LESS THAN(20),\n"
                    + "  PARTITION p2 VALUES LESS THAN(100)\n"
                    + ") tablegroup = mytg2");
            stmt.executeUpdate(
                "CREATE TABLE tb3(a int) PARTITION BY RANGE(a)\n"
                    + " (\n"
                    + "  PARTITION p1 VALUES LESS THAN(20),\n"
                    + "  PARTITION p2 VALUES LESS THAN(100)\n"
                    + ") tablegroup = mytg3");

            // 变更分区分布
            Map<String, String> map = getMasterGroupStorageMap();
            String fromStorage = null;
            String toStorage = null;
            String partition = null;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String temp = getFirstLevelPartitionByGroupName(entry.getKey(), "tb1");
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
            stmt.executeUpdate("alter tablegroup mytg1 move partitions " + partition + " to '" + toStorage + "'");
            executeAndCheck1(stmt, "merge tablegroups mytg2 into mytg3", "mytg3");
            executeAndCheck2(stmt, "merge tablegroups mytg1 into mytg3 force", "mytg3", Sets.newHashSet("tb0", "tb1"));
        }
    }

    private void executeAndCheck1(Statement stmt, String sql, String tableGroupName)
        throws SQLException {
        List<DdlRecordInfo> listBefore = getDdlRecordInfoList("cdc_merge_tg_test", null);

        stmt.executeUpdate(sql);

        List<DdlRecordInfo> listAfter = getDdlRecordInfoList("cdc_merge_tg_test", null);

        Assert.assertEquals(listBefore.size() + 1, listAfter.size());
        Assert.assertEquals(sql, listAfter.get(0).getDdlSql());
        Assert.assertEquals("MERGE_TABLEGROUP", listAfter.get(0).getSqlKind());
        Assert.assertEquals(tableGroupName, listAfter.get(0).getTableName());
        Assert.assertNull(listAfter.get(0).getMetaInfo());
    }

    private void executeAndCheck2(Statement stmt, String sql, String tableGroupName, Set<String> tables)
        throws SQLException {
        List<DdlRecordInfo> listBefore = getDdlRecordInfoList("cdc_merge_tg_test", null);

        stmt.executeUpdate(sql);

        List<DdlRecordInfo> listAfter = getDdlRecordInfoList("cdc_merge_tg_test", null);

        Assert.assertEquals(listBefore.size() + 3, listAfter.size());

        Assert.assertEquals(sql, listAfter.get(0).getDdlSql());
        Assert.assertEquals("MERGE_TABLEGROUP", listAfter.get(0).getSqlKind());
        Assert.assertEquals(tableGroupName, listAfter.get(0).getTableName());
        Assert.assertNull(listAfter.get(0).getMetaInfo());

        Assert.assertTrue(StringUtils.containsIgnoreCase(listAfter.get(1).getDdlSql(), "move partitions"));
        Assert.assertEquals("ALTER_TABLEGROUP", listAfter.get(1).getSqlKind());
        Assert.assertTrue(tables.contains(listAfter.get(1).getTableName()));
        Assert.assertNotNull(listAfter.get(1).getMetaInfo());

        Assert.assertTrue(StringUtils.containsIgnoreCase(listAfter.get(2).getDdlSql(), "move partitions"));
        Assert.assertEquals("ALTER_TABLEGROUP", listAfter.get(2).getSqlKind());
        Assert.assertTrue(tables.contains(listAfter.get(2).getTableName()));
        Assert.assertNotNull(listAfter.get(2).getMetaInfo());

    }
}
