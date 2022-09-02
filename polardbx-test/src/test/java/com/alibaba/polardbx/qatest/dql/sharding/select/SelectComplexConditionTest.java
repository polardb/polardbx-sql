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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * 复杂条件查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectComplexConditionTest extends ReadBaseTestCase {

    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectComplexConditionTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void conditionWithMutilCompareTest() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select * from " + baseOneTableName + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        sql = "select * from " + baseOneTableName
            + " where pk>=? and pk>? and varchar_test  like ? or datetime_test >?";
        param.clear();
        param.add(start1);
        param.add(start2);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select * from " + baseOneTableName
            + " where pk>? or integer_test<? and varchar_test like ? and datetime_test= ?";
        param.clear();
        param.add(start1);
        param.add(50);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        // MySQL doesn't support float comparison in prepare mode.
        sql = "select * from " + baseOneTableName
            + " where pk<=? and integer_test>?  or varchar_test  like ? and timestamp_test =?";
        param.clear();
        param.add(start1);
        param.add(50);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void conditionWithMutilCompare1Test() throws Exception {
        long start1 = 7;
        String sql = "select tinyint_1bit_test,bit_test from " + baseOneTableName + " where pk = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void sumBetweenAndInGroupTest() throws Exception {
        String sql =
            "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select integer_test, sum(pk) as p, sum(float_test) as flo from "
                + baseOneTableName
                + " as a where float_test >=? and timestamp_test between ? and ? and(varchar_test like ? and integer_test in(?,?,?,?,?,?)) group by integer_test";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        param.add(0);
        param.add(10);
        param.add(20);
        param.add(30);
        param.add(40);
        param.add(100);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        sql =
            "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select integer_test, sum(pk) as p, sum(float_test) as flo ,varchar_test,timestamp_test,datetime_test  from "
                + baseOneTableName
                + " as a "
                + "where (( timestamp_test >= ? and timestamp_test<= ? and varchar_test like ? and integer_test in(?,?,?,?,?,?)) and float_test>=?) group by integer_test";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        param.add(0);
        param.add(10);
        param.add(20);
        param.add(30);
        param.add(40);
        param.add(100);
        param.add(columnDataGenerator.float_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void betweenAndInGroupTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select integer_test as sid, timestamp_test  from "
            + baseOneTableName
            + " as a where float_test >=? and "
            + "timestamp_test between ? and ? and(varchar_test like ? and integer_test in(?,?,?,?,?,?)) group by integer_test,timestamp_test";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        param.add(0);
        param.add(10);
        param.add(20);
        param.add(30);
        param.add(40);
        param.add(100);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        sql =
            "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select integer_test as sid, varchar_test ,timestamp_test,datetime_test  from "
                + baseOneTableName
                + " as a "
                + "where (( timestamp_test >= ? and timestamp_test<= ? and varchar_test like ? and integer_test in(?,?,?,?,?,?)) and float_test>=?) group by integer_test,timestamp_test, varchar_test, datetime_test";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        param.add(0);
        param.add(10);
        param.add(20);
        param.add(30);
        param.add(40);
        param.add(100);
        param.add(columnDataGenerator.float_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void betweenGroupAndTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select varchar_test, timestamp_test from "
            + baseOneTableName + " as a where float_test>=? and timestamp_test  BETWEEN ? and ? "
            + "group by varchar_test, timestamp_test";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        sql = "select varchar_test , timestamp_test from " + baseOneTableName + " as a where float_test>=? and "
            + "timestamp_test >=? and timestamp_test<=?  group by varchar_test , timestamp_test";
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void betweenOrderAnd() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select timestamp_test, float_test, integer_test from "
            + baseOneTableName + " as a where " + "integer_test = ? and timestamp_test between ? and ? and "
            + "(varchar_test = ? and timestamp_test > ?) order by timestamp_test";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.timestamp_testValue);

        selectOrderByNotPrimaryKeyAssert(sql,
            param,
            mysqlConnection,
            tddlConnection,
            "timestamp_test",
            true);

        sql =
            "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select timestamp_test, float_test, integer_test,varchar_test from "
                + baseOneTableName
                + " as a where "
                + "((timestamp_test >= ? and timestamp_test<= ? and varchar_test= ?  and integer_test =? )"
                + " and float_test>=? ) order by timestamp_test asc";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.float_testValue);
        selectOrderByNotPrimaryKeyAssert(sql,
            param,
            mysqlConnection,
            tddlConnection,
            "timestamp_test",
            true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByOrderBy() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ SELECT integer_test,sum(pk) as sumPk,varchar_test from "
            + baseOneTableName + " where integer_test between"
            + " ? and ? GROUP BY integer_test,varchar_test ORDER BY integer_test";
        List<Object> param = new ArrayList<Object>();
        param.add(0);
        param.add(20);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ SELECT integer_test,sum(pk) as sumPk,varchar_test from "
            + baseOneTableName + " where integer_test between"
            + " ? and ? GROUP BY integer_test,varchar_test ORDER BY integer_test,varchar_test";
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ SELECT integer_test,sum(pk) as sumPk,varchar_test from "
            + baseOneTableName + " where integer_test between"
            + " ? and ? GROUP BY integer_test,varchar_test ORDER BY varchar_test ,integer_test";
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void queryWithOrFilterExistInAndFilter1() throws Exception {
        String sql = "select varchar_test ,pk from " + baseOneTableName + " where pk=7 and ( pk =7 or pk is null)";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.21
     */
    @Test
    @Ignore("ComparativeAND leads to an empty enumeration set")
    public void queryWithOrFilterExistInAndFilter2() throws Exception {
        String sql = "select varchar_test ,pk from " + baseOneTableName + " where pk=7 and ( pk =7 or pk=8) and pk > 4";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
