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

package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Created by xiaowen.guoxw on 2017/4/6.
 */

public class SelectJoinWithFunction extends AutoReadBaseTestCase {

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect
            .selectBaseOneBaseTwo());
    }

    public SelectJoinWithFunction(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }
//
//    /**
//     * @TCDescription : where exist( subQuery )
//     * @TestStep : 分别在tddl上和mysql上作同样的操作
//     * @ExpectResult : tddl上数据和mysql上数据一致
//     * @author chenghui.lch
//     * @since 5.1.20
//     */
//    @Test
//    public void existsFuncTest(){
//
//        // join_test_tbl_a, join_test_tbl_b 需要以下两条数据
//        // | 1016 | jack |
//        // | 1066 | jack |
//
//        String sql = "";
//
//        // ============不相关的子查询=============
//        // exist 里是不相关的子查询， 子查询结果为常量
//        sql = "select * from join_test_tbl_a where EXISTS( select NULL )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // exist 里是不相关的子查询
//        sql = "select * from join_test_tbl_a where EXISTS( select 1 from join_test_tbl_b )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // exist 里是不相关的子查询, 子查询返回空结果集
//        sql = "select * from join_test_tbl_a where EXISTS( select 1 from join_test_tbl_b  where id = 0 )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // exist 里是不相关的子查询， 子查询结果为常量, 顶层带聚合查询
//        sql = "select count(*) as count from join_test_tbl_a where EXISTS( select NULL )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // exist 里是不相关的子查询, 顶层带聚合查询
//        sql = "select count(*) as count from join_test_tbl_a where EXISTS( select 1 from join_test_tbl_b )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // exist 里是不相关的子查询, 子查询返回空结果集, 顶层带聚合查询
//        sql = "select count(*) as count from join_test_tbl_a where EXISTS( select 1 from join_test_tbl_b  where id = 0 )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // ============相关的子查询=============
//
//        // exist 里是相关的子查询, 相关的查询条件与内层子查询无关,子查询返回空结果集
//        sql = "select * from join_test_tbl_a a where EXISTS( select 1 from join_test_tbl_b b where a.id = 0 )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // exist 里是相关的子查询, 相关的查询条件与内层子查询无关,子查询返回非空空结果集
//        sql = "select * from join_test_tbl_a a where EXISTS( select 1 from join_test_tbl_b b where a.id = 1066 )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//        // exist 里是相关的子查询, 相关的查询条件与内层子查询相关,子查询返回非空空结果集
//        sql = "select * from join_test_tbl_a a where a.id = 1066 and EXISTS( select 1 from join_test_tbl_b b where a.id=b.id  )";
//        selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
//
//    }

    /**
     * @since 5.1.20
     */
    @Test
    public void existsFuncTest() {

        // join_test_tbl_a, join_test_tbl_b 需要以下两条数据
        // | 1016 | jack |
        // | 1066 | jack |

        String sql = "";

        // ============不相关的子查询=============
        // exist 里是不相关的子查询， 子查询结果为常量
        sql = String.format("select * from %s where EXISTS( select NULL )", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // exist 里是不相关的子查询
        sql = String.format("select * from %s where EXISTS( select 1 from %s )", baseOneTableName, baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // exist 里是不相关的子查询, 子查询返回空结果集
        sql = String.format("select * from %s where EXISTS( select 1 from %s  where pk = 0 )", baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // exist 里是不相关的子查询， 子查询结果为常量, 顶层带聚合查询
        sql = String.format("select count(*) as count from %s where EXISTS( select NULL )", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // exist 里是不相关的子查询, 顶层带聚合查询
        sql = String.format("select count(*) as count from %s where EXISTS( select 1 from %s )", baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // exist 里是不相关的子查询, 子查询返回空结果集, 顶层带聚合查询
        sql = String
            .format("select count(*) as count from %s where EXISTS( select 1 from %s  where pk = 0 )", baseOneTableName,
                baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // ============相关的子查询=============

        // exist 里是相关的子查询, 相关的查询条件与内层子查询无关,子查询返回空结果集
        /** Unsupported
         sql = String.format("select * from %s a where EXISTS( select 1 from %s b where a.pk = 0 )", baseOneTableName, baseTwoTableName);
         selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
         */

        /** unsupported
         // exist 里是相关的子查询, 相关的查询条件与内层子查询无关,子查询返回非空空结果集
         sql = String.format("select * from %s a where EXISTS( select 1 from %s b where a.pk = 1 )", baseOneTableName, baseTwoTableName);
         selectContentSameAssert(sql,null, mysqlConnection, tddlConnection,true);
         */

        // exist 里是相关的子查询, 相关的查询条件与内层子查询相关,子查询返回非空空结果集
        sql = String.format("select * from %s a where a.pk = 1 and EXISTS( select 1 from %s b where a.pk=b.pk  )",
            baseOneTableName, baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }
}
