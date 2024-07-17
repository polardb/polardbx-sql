package com.alibaba.polardbx.qatest.ddl.cdc.repartition;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-13 10:10
 **/
public class CdcSubPartition_NormalDdlTest extends CdcRePartitionBaseTest {

    public CdcSubPartition_NormalDdlTest() {
        dbName = "cdc_sub_partition_normal";
    }

    @Test
    public void testSubPartitionNormal() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "create database " + dbName + " mode = auto");
        JdbcUtil.executeUpdate(tddlConnection, "use " + dbName);

        try (Statement stmt = tddlConnection.createStatement()) {
            // ================ 测试常规DDL操作 ================ //
            for (SubPartitionType partitionType : SubPartitionType.values()) {
                DdlCheckContext checkContext = newDdlCheckContext();
                checkContext.updateAndGetMarkList(dbName);

                testNormalDdl(checkContext, stmt, partitionType);
            }
        }
    }

    private void testNormalDdl(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test normal ddl with partition type " + partitionType);
        String tableName;
        tableName = createTable(checkContext, stmt, partitionType, true);
        doNormalDdl(stmt, tableName);
        dropTable(stmt, tableName);

        tableName = createTable(checkContext, stmt, partitionType, false);
        doNormalDdl(stmt, tableName);
        dropTable(stmt, tableName);
    }

    private void doNormalDdl(Statement stmt, String tableName) throws SQLException {
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //--------------------------------------------------------------------------------
        //-----------------------------------Test Columns---------------------------------
        //--------------------------------------------------------------------------------
        // 测试 加列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format(
            "alter table %s add column add1 varchar(20) not null default '111'", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 加列，指定列顺序
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format(
            "alter table %s add column add2 varchar(20) not null default '222' after b", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 同时加减列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add column add3 bigint default 0,drop column add2", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 更改列精度
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s modify add1 varchar(50) not null default '111'", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 更改列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format(
            "alter table %s change column add1 add111 varchar(50) not null default '111'", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 减列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add111", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 减列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add3", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        //--------------------------------------------------------------------------------
        //-------------------------------Test Local Indexes-------------------------------
        //--------------------------------------------------------------------------------
        // 测试 加索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add index idx_test(`b`)", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 加唯一键索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add unique idx_job(`c`)", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // 测试 加索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("create index idx_gmt on %s(`d`)", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        // Test Step
        // 对于含有聚簇索引的表，引擎不支持一个语句里drop两个index，所以一个语句包含两个drop的sql就不用测试了
        // 否则会报错：optimize error by Do not support multi ALTER statements on table with clustered index
        // tokenHints = buildTokenHints();
        // sql = tokenHints + String.format("alter table %s drop index idx_test", tableName1);
        // stmt.execute(sql);
        // Assert.assertEquals(sql, getDdlRecordSql(tokenHints));

        // 测试 删除索引
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop index idx_gmt on %s", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        //--------------------------------------------------------------------------------
        //------------------------------------Test Truncate ------------------------------
        //--------------------------------------------------------------------------------
        // 如果是带有GSI的表，会报错: Does not support truncate table with global secondary index，so use t_ddl_test_zzz for test
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("truncate table %s", tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        //--------------------------------------------------------------------------------
        //------------------------------------Test rename --------------------------------
        //--------------------------------------------------------------------------------
        // 如果是带有GSI的表，会报错: Does not support modify primary table 't_ddl_test' cause global secondary index exists,so use t_ddl_test_yyy for test

        tokenHints = buildTokenHints();
        String newTableName = tableName + "_new";
        sql = tokenHints + String.format("rename table %s to %s", tableName, newTableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        sql = "rename table " + newTableName + " to " + tableName;
        stmt.execute(sql);
    }
}
