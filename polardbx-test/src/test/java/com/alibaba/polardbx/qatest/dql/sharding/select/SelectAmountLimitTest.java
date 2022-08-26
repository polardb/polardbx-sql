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

/**
 * select limit测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectAmountLimitTest extends ReadBaseTestCase {

    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectAmountLimitTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectWithLimit() throws Exception {
        int start = 5;
        int limit = 50;
        String sql = "select * from " + baseOneTableName + " where varchar_test= ?  order by pk  limit ?,?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(start);
        param.add(limit);
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectWithSelectLimit() throws Exception {
        int start = 5;
        int limit = 1;
        String sql = "select * from " + baseOneTableName + " as nor1 ,(select pk from " + baseOneTableName
            + " order by pk limit ?,?) as nor2 where nor1.pk=nor2.pk";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(limit);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectWithLimitOffset() throws Exception {
        int start = 10;
        int limit = 50;

        String sql = "select * from " + baseOneTableName + " where varchar_test= ?  order by pk  limit ? offset ?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(limit);
        param.add(start);

        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectOrderByWithLimitOffset() throws Exception {
        int start = 10;
        int limit = 50;
        String sql =
            "select * from " + baseOneTableName + " where varchar_test= ?  order by integer_test limit ? offset ?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(limit);
        param.add(start);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectByPkWithLimitOffset() throws Exception {
        int start = 0;
        int limit = 1;
        String sql = "select * from " + baseOneTableName + " where pk=? limit ? offset ?";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(limit);
        param.add(start);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }
}
