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

package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * 没数据时查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithNoDataTest extends ReadBaseTestCase {

    ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    long pk = 1l;
    int id = 1;

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithNoDataTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasTest() throws Exception {
        String sql =
            "select varchar_test as xingming ,integer_test as pid from  " + baseOneTableName + "  as nor where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void OrWithDifFiledTest() throws Exception {
        long pk = 2l;
        int id = 3;

        String sql = "select * from " + baseOneTableName + " where pk= ? or integer_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void conditionWithGreaterTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk>? order by pk";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(0 + ""));
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where pk>=? order by pk";
        param.clear();
        param.add(Long.parseLong(0 + ""));
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk in (1,2,3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void LikeAnyTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test like 'hello%'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like '%lo%'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like '%he%o%'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void OrderByAscTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test= ? order by integer_test asc";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "integer_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void GroupByTest() throws Exception {
        String sql = "select count(pk) ,varchar_test as n from  " + baseOneTableName + "   group by varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void LimitWithStart() throws Exception {
        int start = 5;
        int limit = 6;
        String sql = "SELECT * FROM " + baseOneTableName + " order by pk LIMIT " + start + "," + limit;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
