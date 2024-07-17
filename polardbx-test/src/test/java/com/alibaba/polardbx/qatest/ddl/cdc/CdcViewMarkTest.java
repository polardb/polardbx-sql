package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
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
public class CdcViewMarkTest extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_view_test");
            stmt.executeUpdate("create database cdc_view_test mode = 'auto'");
            stmt.executeUpdate("use cdc_view_test");

            DdlCheckContext checkContext = newDdlCheckContext();
            checkContext.updateAndGetMarkList("cdc_view_test");

            executeAndCheck(checkContext, stmt, "create view cdc_v1 as select 1", "cdc_v1");
            executeAndCheck(checkContext, stmt, "drop view cdc_v1", "cdc_v1");

            stmt.executeUpdate("create table t1(id bigint)");
            stmt.executeUpdate("insert into t1(id)values(1)");
            stmt.executeUpdate("insert into t1(id)values(2)");

            checkContext.updateAndGetMarkList("cdc_view_test");
            executeAndCheck(checkContext, stmt, "create materialized view cdc_mymv as select id from t1", "cdc_mymv");
            executeAndCheck(checkContext, stmt, "REFRESH MATERIALIZED VIEW cdc_mymv", "cdc_mymv");
            executeAndCheck(checkContext, stmt, "DROP MATERIALIZED VIEW cdc_mymv", "cdc_mymv");
        }
    }

    private void executeAndCheck(DdlCheckContext checkContext, Statement stmt, String sql, String tableName)
        throws SQLException {
        stmt.executeUpdate(sql);
        String currentSchema = getCurrentDbName(stmt);

        List<DdlRecordInfo> beforeList = checkContext.getMarkList(currentSchema);
        List<DdlRecordInfo> afterList = checkContext.updateAndGetMarkList(currentSchema);

        Assert.assertEquals(beforeList.size() + 1, afterList.size());
        Assert.assertEquals(sql, afterList.get(0).getDdlSql());
        Assert.assertEquals(currentSchema, afterList.get(0).getSchemaName());
        Assert.assertEquals(tableName, afterList.get(0).getTableName());
        Assert.assertEquals(DdlScope.Schema.getValue(), afterList.get(0).getDdlExtInfo().getDdlScope());
    }
}
