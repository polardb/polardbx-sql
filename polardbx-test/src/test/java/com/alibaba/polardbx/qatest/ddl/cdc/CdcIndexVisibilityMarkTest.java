package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.DdlScope;
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
public class CdcIndexVisibilityMarkTest extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_index_visibility_test");
            stmt.executeUpdate("create database cdc_index_visibility_test  mode = 'auto'");
            stmt.executeUpdate("use cdc_index_visibility_test");

            stmt.executeUpdate("create table t_order (\n"
                + "  `id` bigint(11),\n"
                + "  `order_id` varchar(20),\n"
                + "  `buyer_id` varchar(20),\n"
                + "  global index `g_order_id` (order_id) partition by key(order_id) invisible\n"
                + ") partition by hash(`id`)");
            stmt.executeUpdate(
                "alter table t_order add global index `g_buyer_id`(buyer_id) partition by hash(buyer_id) invisible");
            executeAndCheck(stmt, buildTokenHints() + "alter table t_order alter index `g_order_id` visible");
            executeAndCheck(stmt, buildTokenHints() + "alter table t_order alter index `g_order_id` invisible");
            executeAndCheck(stmt, buildTokenHints() + "alter table t_order alter index `g_order_id` visible");
            executeAndCheck(stmt, buildTokenHints() + "alter table t_order alter index `g_order_id` invisible");
        }
    }

    private void executeAndCheck(Statement stmt, String ddl) throws SQLException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        String currentSchema = getCurrentDbName(stmt);

        List<DdlRecordInfo> list = getDdlRecordInfoListByToken(tokenHints);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(sql, list.get(0).getDdlSql());
        Assert.assertEquals(currentSchema, list.get(0).getSchemaName());
        Assert.assertEquals("t_order", list.get(0).getTableName());
        Assert.assertEquals(DdlScope.Schema.getValue(), list.get(0).getDdlExtInfo().getDdlScope());
    }
}
