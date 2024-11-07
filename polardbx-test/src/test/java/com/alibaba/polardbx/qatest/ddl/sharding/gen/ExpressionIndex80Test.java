package com.alibaba.polardbx.qatest.ddl.sharding.gen;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

@ReplicaIgnore(ignoreReason = "not support 80 expression index in replica")
public class ExpressionIndex80Test extends DDLBaseNewDBTestCase {

    @Test
    public void testCreate() {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "expr_create_80_test";
        String createSql = String.format(
            "create table `%s` (a int primary key, b int, c int, d varchar(64), e varchar(64), index cc((b + c))) DBPARTITION BY hash(`a`)",
            tableName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        JdbcUtil.dropTable(tddlConnection, tableName);
        createSql = String.format(
            "create table `%s` (a int primary key, b int, c int, d varchar(64), e varchar(64), f json DEFAULT NULL, "
                + " index `idx_1` ((cast(json_extract(`f`,_utf8mb4'$[*]') as char(64) array))),"
                + " key `idx_2` ((cast(json_extract(`f`,_utf8mb4'$[*]') as char(64) array))),"
                + " unique index `idx_3` ((cast(json_extract(`f`,_utf8mb4'$[*]') as char(64) array))),"
                + " unique key `idx_4` ((cast(json_extract(`f`,_utf8mb4'$[*]') as char(64) array)))) DBPARTITION BY hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String alterSql = String.format(
            "alter table `%s` add index `idx_5` ((cast(json_extract(`f`,_utf8mb4'$[*]') as char(64) array)));",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);

        String createIndexSql =
            String.format("create index `idx_6` on %s ((cast(json_extract(`f`,_utf8mb4'$[*]') as char(64) array)));",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndexSql);

        String checkTableSql = String.format("check table %s", tableName);
        List<List<Object>> checkTableResult =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(checkTableSql, tddlConnection));

        Assert.assertTrue(checkTableResult.stream().allMatch(o -> o.get(3).toString().equalsIgnoreCase("OK")));
    }
}
