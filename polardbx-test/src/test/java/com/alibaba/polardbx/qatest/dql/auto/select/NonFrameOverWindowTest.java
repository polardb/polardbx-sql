/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXAutoDBName1;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 单元测试包含的测试内容：
 * * 1. 测试正常情况下的函数是否能正确执行
 * -> sum and sum_v2
 * -> max and min
 * -> avg_v2 and count_v2
 * -> bitOr and bitAnd and bitXor
 * -> rank and row_number
 * 2. 测试current row and current row的情况
 * 3. 测试partition by列表
 * 4. 测试partition by为string等其他类型的情况
 */
public class NonFrameOverWindowTest extends AutoReadBaseTestCase {
    private static final String TABLE_NAME = "drds_test_noframe_window";

    // 注意null值
    // 测试数据格式
    // null, 1, null
    // null, 1, 2
    //  0, 1, 1
    // --chunk break
    //  0, 1, 2
    // --chunk break
    //  0, 1, 1
    //  0, 2, null
    // --chunk break
    //  0, 2, 2
    // --chunk break
    //  1, 1, 1

    @BeforeClass
    public static void prepareNormalData() throws Exception {
        String createSql =
            "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=false)*/CREATE TABLE if not exists `" + TABLE_NAME + "` (\n"
                + "\t`id1` int(10) DEFAULT NULL,\n"
                + "\t`id2` int(10) DEFAULT NULL,\n"
                + "\t`id3` int(10) DEFAULT NULL,\n"
                + "        KEY `auto_shard_key_id1` USING BTREE (`id1`)\n"
                + ") ";

        String sql = "delete from  " + TABLE_NAME;
        String insertSql = "insert into " + TABLE_NAME + " values "
            + "(NULL,1,NULL),"
            + "(NULL,1,2),"
            + "(0,1,1),"
            + "(0,1,2),"
            + "(0,2,null),"
            + "(0,2,2),"
            + "(1,1,1)";
        String dropTable = "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=false)*/drop table if exists " + TABLE_NAME;

        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXAutoDBName1());
            //drop
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
            //create
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + " partition by hash(`id1`)");
            //insert
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            JdbcUtil.executeUpdateSuccess(mysqlConnection, dropTable);

            JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);

            JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        }

    }

    @Test
    public void testSumAndSumV2() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format("select *,sum(id2) over (partition by id1),sum(id3) over (partition by id1) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testMaxAndMin() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format("select *,max(id2) over (partition by id1),min(id2) over (partition by id1) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testAvgV2AndCountV2() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format("select *,avg(id1) over (partition by id1),count(id2) over (partition by id1) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testRank() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *, rank() over (partition by id1 order by id2) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testRankWithMultiColumns() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            String.format("select *, rank() over (partition by id1 order by id2, id3 desc) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testDenseRank() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *, DENSE_RANK() over (partition by id1 order by id2) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testRowNumber() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *, row_number() over (partition by id1) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJoin() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select * from (select DENSE_RANK() over (partition by id1 order by id2) w1 from "
                + TABLE_NAME + ") a "
                + " join " + ExecuteTableSelect.selectBaseOneTable()[2][0] + " b on a.w1 = b.pk");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
