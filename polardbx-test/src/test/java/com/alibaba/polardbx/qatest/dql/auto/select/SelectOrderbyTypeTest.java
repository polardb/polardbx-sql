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
import com.alibaba.polardbx.qatest.data.ColumnDataRandomGenerateRule;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import groovy.sql.Sql;
import org.apache.calcite.sql.SqlIdentifier;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * orderby各种类型的测试
 */

public class SelectOrderbyTypeTest extends AutoReadBaseTestCase {

    String normaltblOrderTableName;
    String orderListParam;

    @Parameters(name = "{index}:table={0}:orderby:{1}:pushSort={2}")
    public static List<Object[]> runTable() {
        String[][] tableNameOrderByList = ExecuteTableSelect.selectBaseOneTableOrderbyList();
        List<Object[]> paramList =
            Arrays.stream(tableNameOrderByList)
                .map(row -> Stream.concat(Arrays.stream(row), Stream.of(true)).toArray(Object[]::new))
                .collect(Collectors.toList());
        paramList.addAll(
            Arrays.stream(tableNameOrderByList)
                .map(row -> Stream.concat(Arrays.stream(row), Stream.of(false)).toArray(Object[]::new))
                .collect(Collectors.toList())
        );
        return paramList;
    }

    String hintValue;

    public SelectOrderbyTypeTest(String tableName, String orderList, boolean pushSort) {
        if (pushSort) {
            hintValue = "/*+TDDL:cmd_extra(ENABLE_CBO=true, ENABLE_PUSH_SORT=true)*/";
        } else {
            hintValue = "/*+TDDL:cmd_extra(ENABLE_CBO=true, ENABLE_PUSH_SORT=false)*/";
        }
        normaltblOrderTableName = tableName;
        orderListParam = orderList;
    }

    /**
     * @since 5.1.18
     */
    @Test
    @Ignore("bug")
    public void selectFieldWithTabTest() throws Exception {
        String[] columnParam = {orderListParam};
        for (int i = 0; i < columnParam.length; i++) {
            String sql = String.format("select * from %s order by %s asc", normaltblOrderTableName, orderListParam);
            selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
        }
    }

    /**
     * @since 5.1.18
     */

    @Test
    @Ignore("bug")
    public void selectTwoColumnDescTest() throws Exception {
        String sql = String.format("select integer_test,count(DISTINCT " + orderListParam
                + " ) from %s group by integer_test order by integer_test, count(DISTINCT %s ) desc",
            normaltblOrderTableName,
            orderListParam);
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format("select integer_test,count(DISTINCT " + orderListParam
                + " ) from %s group by integer_test order by count(DISTINCT %s ), integer_test desc",
            normaltblOrderTableName,
            orderListParam);
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.18
     */

    @Test
    @Ignore
    public void selectTwoColumnAscTest() throws Exception {
        String sql = String.format("select integer_test,count(DISTINCT " + orderListParam
                + " ) from %s group by integer_test order by integer_test, count(DISTINCT %s ) ",
            normaltblOrderTableName,
            orderListParam);
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format("select integer_test,count(DISTINCT " + orderListParam
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
        String sql = String.format("select * from %s order by %s asc ,pk", normaltblOrderTableName, orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam);
    }

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
    public void limitWithoutHintTest() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear plancache");

        String sql = String.format("select * from %s order by `%s`,`pk` asc limit 5, 1",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);

        sql = String.format("select * from %s order by `%s`,`pk`asc limit 100, 1",
            normaltblOrderTableName,
            orderListParam);
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderListParam, true);
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

    public void dropTableIfExists(String tableName, Connection connection) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(connection, sql);
    }

    @Test
    public void selectOrderByVarbinaryTest() {
        ColumnDataRandomGenerateRule columnDataRandomGenerateRule = new ColumnDataRandomGenerateRule();
        String binaryTestTable = "binary_test";
        dropTableIfExists(binaryTestTable, mysqlConnection);
        dropTableIfExists(binaryTestTable, tddlConnection);
        String sql1 = "CREATE TABLE " + SqlIdentifier.surroundWithBacktick(binaryTestTable) + "(\n" +
            " `a` binary(255), \n" +
            " `b` varbinary(255) \n" +
            ") ENGINE = InnoDB DEFAULT CHARSET=utf8;";
        String sql = "CREATE TABLE " + SqlIdentifier.surroundWithBacktick(binaryTestTable) + "(\n" +
            " `a` binary(255), \n" +
            " `b` varbinary(255) \n" +
            ") ENGINE = InnoDB DEFAULT CHARSET=utf8 ";
        if (usingNewPartDb()) {
            sql += " partition BY hash(`a`);";
        } else {
            sql += " dbpartition BY hash(`a`);";
        }

        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        for (int i = 0; i < ColumnDataRandomGenerateRule.charS.length; i++) {
            sql = "insert into " + SqlIdentifier.surroundWithBacktick(binaryTestTable) + " values('"
                + columnDataRandomGenerateRule.binary_testRandom() + "','"
                + columnDataRandomGenerateRule.var_binary_testRandom() + "');";
            JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            String querySql1 =
                hintValue + "SELECT a, b FROM " + SqlIdentifier.surroundWithBacktick(binaryTestTable) + " ORDER BY a";
            selectOrderAssert(querySql1, null, mysqlConnection, tddlConnection);
            String querySql2 =
                hintValue + "SELECT a, b FROM " + SqlIdentifier.surroundWithBacktick(binaryTestTable) + " ORDER BY b";
            selectOrderAssert(querySql2, null, mysqlConnection, tddlConnection);
        }
    }
}