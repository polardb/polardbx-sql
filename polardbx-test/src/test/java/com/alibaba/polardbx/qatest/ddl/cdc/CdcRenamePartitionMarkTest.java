package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
public class CdcRenamePartitionMarkTest extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_rename_partition_test");
            stmt.executeUpdate("create database cdc_rename_partition_test mode = 'auto'");
            stmt.executeUpdate("use cdc_rename_partition_test");

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
                    + "  PARTITION p2 VALUES LESS THAN(100)\n"
                    + ")");
            stmt.executeUpdate(
                "CREATE TABLE tb4(a int) PARTITION BY RANGE(a)\n"
                    + " (\n"
                    + "  PARTITION p1 VALUES LESS THAN(20),\n"
                    + "  PARTITION p2 VALUES LESS THAN(100)\n"
                    + ")");
            stmt.executeUpdate(
                "CREATE TABLE tb5(a int) PARTITION BY RANGE(a)\n"
                    + " (\n"
                    + "  PARTITION p1 VALUES LESS THAN(20),\n"
                    + "  PARTITION p2 VALUES LESS THAN(100)\n"
                    + ")");

            executeAndCheck(stmt, "alter table tb1 rename partition p1 to p11", "tb1");
            executeAndCheck(stmt, "alter table tb2 rename partition p1 to p11", "tb2");
            executeAndCheck(stmt, "alter table tb3 rename partition p1 to p11", "tb3");
            executeAndCheck(stmt, "alter table tb4 rename partition p2 to p21", "tb4");
            executeAndCheck(stmt, "alter table tb5 rename partition p2 to p21", "tb5");

            stmt.executeUpdate("create tablegroup tg_xxx");
            stmt.executeUpdate("alter tablegroup tg_xxx add tables tb1,tb2,tb3");
            executeAndCheck(stmt, "alter tablegroup tg_xxx rename partition p11 to p1", "tg_xxx");
        }
    }

    private void executeAndCheck(Statement stmt, String ddl, String tableName)
        throws SQLException {
        List<DdlRecordInfo> listBefore_0 = getDdlRecordInfoList("cdc_rename_partition_test", null);
        List<DdlRecordInfo> listBefore_1 = getDdlRecordInfoList("cdc_rename_partition_test", tableName);

        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        List<DdlRecordInfo> listAfter_0 = getDdlRecordInfoList("cdc_rename_partition_test", null);
        List<DdlRecordInfo> listAfter_1 = getDdlRecordInfoList("cdc_rename_partition_test", tableName);

        Assert.assertEquals(listBefore_0.size() + 1, listAfter_0.size());
        Assert.assertEquals(listBefore_1.size() + 1, listAfter_1.size());
        assertSqlEquals(sql, listAfter_1.get(0).getDdlSql());
        Assert.assertNull(listAfter_0.get(0).getMetaInfo());
    }
}
