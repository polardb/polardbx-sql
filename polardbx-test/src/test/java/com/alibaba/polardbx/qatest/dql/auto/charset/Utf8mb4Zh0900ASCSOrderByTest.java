package com.alibaba.polardbx.qatest.dql.auto.charset;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class Utf8mb4Zh0900ASCSOrderByTest extends AutoReadBaseTestCase {
    @Before
    public void createTable() {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.executeSuccess(mysqlConnection, "drop table if exists t_utf8mb4_zh_0900_as_cs");
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists t_utf8mb4_zh_0900_as_cs");
        JdbcUtil.executeSuccess(mysqlConnection,
            "create table t_utf8mb4_zh_0900_as_cs (pk bigint primary key auto_increment, a varchar(255) character set utf8mb4 collate utf8mb4_zh_0900_as_cs)");
        JdbcUtil.executeSuccess(tddlConnection,
            "create table t_utf8mb4_zh_0900_as_cs (pk bigint primary key auto_increment, a varchar(255) character set utf8mb4 collate utf8mb4_zh_0900_as_cs) partition by hash(pk) partitions 3");
    }

    @Test
    public void test() {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.executeSuccess(mysqlConnection,
            "insert into t_utf8mb4_zh_0900_as_cs (a) values ('一'),('二'),('三'),('四'),('五'),('六'),('七'),('八'),('九'),('十')");
        JdbcUtil.executeSuccess(tddlConnection,
            "insert into t_utf8mb4_zh_0900_as_cs (a) values ('一'),('二'),('三'),('四'),('五'),('六'),('七'),('八'),('九'),('十')");

        // '八','二','九','六','七','三','十','四','五','一'
        selectContentSameAssert("select a from t_utf8mb4_zh_0900_as_cs order by a;", null, tddlConnection,
            mysqlConnection);
    }

    @After
    public void dropTable() {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.executeSuccess(mysqlConnection, "drop table if exists t_utf8mb4_zh_0900_as_cs");
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists t_utf8mb4_zh_0900_as_cs");
    }
}
