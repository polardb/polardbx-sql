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

package com.alibaba.polardbx.qatest.dql.auto.spill;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * orderby各种类型的测试
 */


public class SpillSortTest extends AutoReadBaseTestCase {

    String hint = " /*+TDDL:cmd_extra(ENABLE_SPILL=true)*/";
    String normaltblOrderTableName;
    String orderListParam;

    @Parameters(name = "{index}:table={0}:orderby:{1}")
    public static List<String[]> runTable() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTableOrderbyList());
    }

    public SpillSortTest(String tableName, String orderList) {
        normaltblOrderTableName = tableName;
        orderListParam = orderList;
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void selectFieldWithTabTest() throws Exception {
        String[] columnParam = {orderListParam};
        for (int i = 0; i < columnParam.length; i++) {
            String sql =
                hint + String.format("select * from %s order by %s asc", normaltblOrderTableName, orderListParam);
            selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
        }
    }

    /**
     * @since 5.1.18
     */

    @Test
    @Ignore("bug")
    public void selectTwoColumnDescTest() throws Exception {
        String sql = hint + String.format("select integer_test,count(DISTINCT " + orderListParam
                + " ) from %s group by integer_test order by integer_test, count(DISTINCT %s ) desc",
            normaltblOrderTableName,
            orderListParam);
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = hint + String.format("select integer_test,count(DISTINCT " + orderListParam
                + " ) from %s group by integer_test order by count(DISTINCT %s ), integer_test desc",
            normaltblOrderTableName,
            orderListParam);
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.18
     */

    @Test
    @Ignore("bug")
    public void selectTwoColumnAscTest() throws Exception {
        String sql = hint + String.format("select integer_test,count(DISTINCT " + orderListParam
                + " ) from %s group by integer_test order by integer_test, count(DISTINCT %s ) ",
            normaltblOrderTableName,
            orderListParam);
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = hint + String.format("select integer_test,count(DISTINCT " + orderListParam
                + " ) from %s group by integer_test order by count(DISTINCT %s ), integer_test ",
            normaltblOrderTableName,
            orderListParam);
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void selectBlobFieldWithTabTest() throws Exception {
        String sql =
            hint + String.format("select * from %s order by %s asc ,pk", normaltblOrderTableName, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
    }

    static final String hintValue = "/*+TDDL:cmd_extra(ENABLE_CBO=true ENABLE_SPILL=true)*/";

    @Test
    public void mergeSortTest() throws Exception {
        String sql =
            hintValue + String.format("select * from %s order by %s asc", normaltblOrderTableName, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select * from %s order by %s asc limit 0",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select * from %s order by %s asc limit 1000",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select * from %s order by %s asc limit 1001",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select * from %s order by %s asc limit 2000",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
    }

    @Test
    public void mergeSortWithLimitTest() throws Exception {
        String sql = hintValue + String.format("select * from %s order by %s asc limit 0, 0",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select * from %s order by %s asc limit 3, 0",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select * from %s order by %s asc limit 0,1000",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select * from %s order by %s asc limit 1000,1000",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select * from %s order by %s asc limit 1001",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);
    }

    @Test
    public void topNTest() throws Exception {
        String sql = hintValue + String.format("select %s from %s group by %s order by %s desc limit 0, 0",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s asc limit 3, 0",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s desc limit 0,1",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s asc limit 0,1000",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s asc limit 1001",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
    }

    @Test
    public void limitTest() throws Exception {
        String sql = hintValue + String.format("select %s from %s group by %s order by %s asc limit 0, 0",
            orderListParam, normaltblOrderTableName,
            orderListParam, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select %s from %s group by %s order by %s desc limit 3, 0",
            orderListParam, normaltblOrderTableName,
            orderListParam, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = hintValue + String.format("select %s from %s group by %s order by %s asc limit 0,1",
            orderListParam, normaltblOrderTableName,
            orderListParam, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s order by %s desc limit 0,1000",
            orderListParam, normaltblOrderTableName,
            orderListParam, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s order by %s asc limit 1001",
            orderListParam, normaltblOrderTableName,
            orderListParam, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
    }

    @Test
    public void memSortTest() throws Exception {
        String sql = hintValue + String.format("select %s from %s group by %s order by %s desc ",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s asc",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s desc",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s desc",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s ",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s asc",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s desc",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);

        sql = hintValue + String.format("select %s from %s group by %s  order by %s desc",
            orderListParam,
            normaltblOrderTableName,
            orderListParam,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
    }
}
