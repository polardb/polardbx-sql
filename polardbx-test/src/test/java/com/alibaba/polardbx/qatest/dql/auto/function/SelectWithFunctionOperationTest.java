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
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 函数运算查询
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithFunctionOperationTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithFunctionOperationTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testFunctionPlusMusTest() throws Exception {
        String sql = "SELECT MAX(pk)+MIN(pk)  as a FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT MAX(pk)+MIN(pk) as plus FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT MAX(pk)-MIN(pk) as a FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT MAX(pk)-MIN(pk) mus FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT MAX(pk)--MIN(pk) mus FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testFunctionMultiplicationDivisionTest() throws Exception {
        String sql = "SELECT sum(pk)/max(integer_test)  as a FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT sum(pk)/count(*) as Division FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT max(integer_test)*count(integer_test) as c FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT avg(integer_test)*count(*) Multiplication FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testFunctionModTest() throws Exception {
        String sql = "SELECT sum(pk)%count(*) sd FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testFunctionPlus() throws Exception {
        String sql = "SELECT pk FROM " + baseOneTableName + " where pk=1+1";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testFunctionMixTest() throws Exception {

        String sql = "SELECT sum(pk)/max(integer_test)+count(*) as b  FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql =
            "SELECT max(integer_test)*count(integer_test)/8-count(*)*2+min(integer_test) as c FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testFunctionAddDupTest() throws Exception {
        String sql = "SELECT pk+1 as b  FROM " + baseOneTableName + " order by pk";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
