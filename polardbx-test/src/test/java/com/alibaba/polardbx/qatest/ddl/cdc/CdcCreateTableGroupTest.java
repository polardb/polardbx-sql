package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-02 21:39
 **/
public class CdcCreateTableGroupTest extends CdcBaseTest {
    @Test
    public void testCreateTableGroupWithLocality() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists cdc_tablegroup_test");
        JdbcUtil.executeUpdate(tddlConnection, "create database cdc_tablegroup_test mode = auto");
        JdbcUtil.executeUpdate(tddlConnection, "use cdc_tablegroup_test");

        Map<String, String> map = getMasterGroupStorageMap();
        String dnId = map.entrySet().stream().findAny().get().getValue();

        String sql = "CREATE TABLEGROUP str_key_tg PARTITION BY KEY ( VARCHAR(255)) "
            + "PARTITIONS 16 LOCALITY = 'dn=" + dnId + "'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        List<DdlRecordInfo> list = getDdlRecordInfoList("cdc_tablegroup_test", "str_key_tg");
        Assert.assertEquals(sql, list.get(0).getDdlSql());

    }
}
