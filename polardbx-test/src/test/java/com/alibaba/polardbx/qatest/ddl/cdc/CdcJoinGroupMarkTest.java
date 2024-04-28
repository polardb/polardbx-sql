package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import org.apache.commons.lang3.RandomUtils;
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
@CdcIgnore(ignoreReason = "join group没有用起来，CDC不再进行测试；"
    + "alter join group会触发表所属表组的变更，目前也没有透传表组到下游的能力，会导致上下游表组无法对齐")
public class CdcJoinGroupMarkTest extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException {
        String joinGroup = "cdc_joingroup_" + RandomUtils.nextLong();
        String createJoinGroup = buildTokenHints() + "create joingroup " + joinGroup;
        String dropJoinGroup = buildTokenHints() + "drop joingroup " + joinGroup;
        String alterJoinGroupAddTables = buildTokenHints() + "alter joingroup " + joinGroup + " add tables t1 , t2";
        String alterJoinGroupRemoveTables = buildTokenHints() + "alter joingroup " + joinGroup + " remove tables t1,t2";

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_joingroup_test");
            stmt.executeUpdate("create database cdc_joingroup_test  mode = 'auto'");
            stmt.executeUpdate("use cdc_joingroup_test");
            stmt.executeUpdate("create table t1("
                + "id bigint primary key, "
                + "name varchar(200)) "
                + "partition by key(name) partitions 8");
            stmt.executeUpdate(
                "create table t2(id bigint primary key, name varchar(200)) "
                    + "partition by key(name) partitions 4");

            executeAndCheck(stmt, createJoinGroup);
            executeAndCheck(stmt, alterJoinGroupAddTables);
            executeAndCheck(stmt, alterJoinGroupRemoveTables);
            executeAndCheck(stmt, dropJoinGroup);
        }
    }

    private void executeAndCheck(Statement stmt, String ddl) throws SQLException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        List<DdlRecordInfo> list = getDdlRecordInfoListByToken(tokenHints);
        Assert.assertEquals(sql, list.get(0).getDdlSql());
        Assert.assertEquals(1, list.size());
    }
}
