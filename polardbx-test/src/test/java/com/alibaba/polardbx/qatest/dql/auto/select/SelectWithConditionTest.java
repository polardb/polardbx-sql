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
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectConutAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * 带条件的选择查询
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithConditionTest extends AutoReadBaseTestCase {

    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    public SelectWithConditionTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void inTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk in (1,2,3)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void inCastTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk in ( CAST( + CAST( 1 AS SIGNED ) AS SIGNED ) )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void betweenTest() throws Exception {
        int start = 5;
        int end = 15;
        String sql = "select pk,integer_test,varchar_test from " + baseOneTableName
            + " where integer_test between ? and ?";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(end);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void notBetweenTest() throws Exception {
        int start = 5;
        int end = 15;
        String sql = "select  pk,integer_test,varchar_test  from " + baseOneTableName
            + " where integer_test not between ? and ?";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(end);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void columnBindTest() throws Exception {
        int start = 5;
        int end = 15;
        String sql = "select ? as a,? as b,? as c from " + baseOneTableName + " where integer_test between ? and ?";
        List<Object> param = new ArrayList<Object>();
        param.add("PK");
        param.add("varchar_test");
        param.add("integer_test");
        param.add(start);
        param.add(end);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void constant() throws Exception {
        String sql = "select 1 a,2 from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectFilterTest() throws Exception {
        String sql = "select integer_test=integer_test as a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectFilterTest2() throws Exception {
        String sql = "select integer_test=1 a from " + baseOneTableName + " order by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void selectFilterTest3() throws Exception {
        String sql = "select integer_test and 1 a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectFilterTest4() throws Exception {
        String sql = "select 1=1 a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectFilterTest5() throws Exception {
        String sql = "select integer_test in (1,2,3) a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectFilterTest6() throws Exception {
        String sql = "select integer_test not in (1,2,3) a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void inWithParamTest() throws Exception {
        String sql = "select pk,varchar_test  from " + baseOneTableName + " where pk in (?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(1 + ""));
        param.add(Long.parseLong(2 + ""));
        param.add(Long.parseLong(3 + ""));
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void NotInTest() throws Exception {
        String sql = "select  pk,varchar_test   from " + baseOneTableName + " where pk not in (?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(1 + ""));
        param.add(Long.parseLong(2 + ""));
        param.add(Long.parseLong(3 + ""));
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void isTrueTest() throws Exception {
        String sql = "select   pk,varchar_test, integer_test from " + baseOneTableName + " where pk is true";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select integer_test is true a,varchar_test from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectNotTest() throws Exception {
        String sql = "select not pk a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectNotFunTest() throws Exception {
        String sql = "select not max(pk) a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectNotConstantTest() throws Exception {
        String sql = "select not (1+1) a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectNotWhereConstantTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where not (integer_test=1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectNotWhereConstantOrTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where not (integer_test=1 or integer_test=2)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void isNotTrueTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk is not true";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByWithCountTest() throws Exception {
        String sql = "select count(pk),varchar_test from " + baseOneTableName + " group by varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByWithAscTest() throws Exception {
        if (isMySQL80()) {// group by has not supported in MySQL8
            return;
        }
        String sql = "select count(pk),varchar_test from " + baseOneTableName + " group by varchar_test asc";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByWithMinMaxTest() throws Exception {
        String sql = "select varchar_test,min(pk) from " + baseOneTableName + " group by varchar_test";

        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test");

        sql = "select varchar_test,max(pk) from " + baseOneTableName + " group by varchar_test";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByAvgTest() throws Exception {

        String sql = "select varchar_test,avg(pk) from " + baseOneTableName + " group by varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupBySumTest() throws Exception {
        String sql = "select varchar_test,sum(pk) from " + baseOneTableName + " group by varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void groupByShardColumnsSumTest() throws Exception {
        String sql = "select varchar_test,sum(pk) from " + baseOneTableName + " group by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByShardColumnsCountDistinctTest() throws Exception {
        String sql = "select pk,count(distinct varchar_test) count from " + baseOneTableName + " group by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupDateFunctionTest() throws Exception {
        String sql = "select sum(integer_test) value  from " + baseOneTableName + " group by date_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void distinctShardColumns() throws Exception {
        String sql = "select distinct pk from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countDistinctShardColumns() throws Exception {
        String sql = "select count(distinct pk) c from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countDistinctShardColumnsWithoutAlias() throws Exception {
        String sql = "select count(distinct pk) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void havingTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk)>? order by varchar_test ";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void havingFunTest() throws Exception {
        if (baseOneTableName.contains("oneGroup_oneAtom")) {
            String sql = "select varchar_test from " + baseOneTableName + " group by varchar_test having sum(pk) > ?";
            List<Object> param = new ArrayList<Object>();
            param.add(40l);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orderByTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test= ? order by pk";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orderByAscTest() throws Exception {
        String orderByKey = "integer_test";
        String sql = "select * from " + baseOneTableName + " where varchar_test= ? order by " + orderByKey + " asc";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, orderByKey);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orderByDescTest() throws Exception {
        String orderByKey = "integer_test";
        String sql = "select * from " + baseOneTableName + " where varchar_test= ? order by integer_test desc";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, orderByKey);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orderByNotAppointFieldTest() throws Exception {
        String sql = "select varchar_test,pk from " + baseOneTableName + " where varchar_test= ? order by integer_test";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orderByMutilValueTest() throws Exception {
        String sql = "select varchar_test,pk from " + baseOneTableName
            + " where varchar_test= ? order by date_test,integer_test";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByOrderbyTest() throws Exception {
        String orderKey = "varchar_test";
        String sql = "select varchar_test,count(a.pk) as c from " + baseOneTableName
            + " as a group by varchar_test order by " + orderKey;
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderKey);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByOrderbyFunctionCloumTest() throws Exception {
        String orderKey = "count(pk)";
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test order by count(pk)";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, orderKey);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test= ? and integer_test> ?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.integer_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndOrTest() throws Exception {
        int i = 2;
        String sql = "select * from " + baseOneTableName
            + " where (varchar_test= ? or varchar_test in (?)) and (integer_test= ? or integer_test > ?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.varchar_testValueTwo);
        param.add(i);
        param.add(i + 1);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orWithSameFiledTest() throws Exception {
        long pk = 2l;
        long opk = 3l;

        String sql = "select * from " + baseOneTableName + " where pk= ? or pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(opk);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void distinctTest() throws Exception {
        String sql = "select distinct varchar_test from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void distinctWithCountDistinctTest() throws Exception {
        {
            String sql = "select count(distinct integer_test) from " + baseOneTableName + " as t1 where pk>1";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
        {
            String sql = "select count(distinct integer_test) from " + baseOneTableName + " where pk>1";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        {
            String sql = "select count(distinct integer_test) k from " + baseOneTableName + " as t1 where pk>1";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void distinctOrderByTest() throws Exception {
        // order by和distinct不一致，只能用临时表
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select  distinct varchar_test from " + baseOneTableName
            + " where varchar_test=? order by integer_test";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        // TODO
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        // try {
        // rc = tddlQueryData(sql, param);
        // Assert.assertEquals(1, resultsSize(rc));
        // } finally {
        // if (rc != null) {
        // rc.close();
        // }
        //
        // }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orWithDifFiledTest() throws Exception {
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
    public void orWithGroupTest() throws Exception {
        long pk = 2l;
        int id = 3;

        String sql = "select * from " + baseOneTableName + " where (pk= ? or pk > ?) or integer_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(pk + 1);
        param.add(id);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void limitWithStart() throws Exception {
        int start = 5;
        int limit = 6;
        String sql = "SELECT * FROM " + baseOneTableName + " order by pk LIMIT " + start + "," + limit;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectLimit() throws Exception {
        int start = 5;
        int limit = 1;
        String sql = "select * from " + baseTwoTableName + " as nor1 ,(select pk from " + baseOneTableName
            + " where varchar_test=? limit ?,?) as nor2 where nor1.integer_test=nor2.pk";

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(start);
        param.add(limit);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void limitWithoutStart() throws Exception {
        int limit = 50;
        String sql = "SELECT * FROM " + baseOneTableName + " LIMIT " + limit;
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByScalarFunction() throws Exception {
        String sql = "SELECT  COUNT(1) daily_illegal,DATE_FORMAT(date_test, '%Y-%m-%d') d ,varchar_test FROM "
            + baseOneTableName + " group by DATE_FORMAT(date_test, '%Y-%m-%d'),varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupByLimitTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName + " group by varchar_test  limit 1";
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orderByLimitTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test=? order by pk limit 10 ";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test=? order by pk desc limit 10 ";
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void dateTypeWithLimit() throws Exception {
        String sql = "select * from "
            + baseOneTableName
            + " where date_test	>? and date_test	 <? and varchar_test like ? order by date_test	 desc limit 2,5";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.date_testStartValue);
        param.add(columnDataGenerator.date_testEndValue);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName
            + " where date_test>=? and date_test <=? and varchar_test like ? order by date_test desc limit 2,5";
        param.clear();
        param.add(columnDataGenerator.date_testStartValue);
        param.add(columnDataGenerator.date_testEndValue);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName
            + " where date_test>? and date_test <? and varchar_test like ? order by date_test desc limit 10,5";

        param.clear();
        param.add(columnDataGenerator.date_testStartValue);
        param.add(columnDataGenerator.date_testEndValue);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void timestampTypeWithLimit() throws Exception {
        String sql = "select * from "
            + baseOneTableName
            + " where timestamp_test>? and timestamp_test <? and varchar_test like ? order by timestamp_test desc limit 2,5";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from "
            + baseOneTableName
            + " where timestamp_test>=? and timestamp_test <=? and varchar_test like ? order by timestamp_test desc limit 2,5";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from "
            + baseOneTableName
            + " where timestamp_test>? and timestamp_test <? and varchar_test like ? order by timestamp_test desc limit 10,5";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void datetimeTypeWithLimit() throws Exception {
        String sql = "select * from "
            + baseOneTableName
            + " where datetime_test>? and datetime_test <? and varchar_test like ? order by datetime_test desc limit 2,5";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from "
            + baseOneTableName
            + " where datetime_test>=? and datetime_test <=? and varchar_test like ? order by datetime_test desc limit 2,5";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from "
            + baseOneTableName
            + " where datetime_test>? and datetime_test <? and varchar_test like ? order by datetime_test desc limit 10,5";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from "
            + baseOneTableName
            + " where (datetime_test>=? or datetime_test <=?) and varchar_test like ? order by datetime_test desc limit 2,5";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from "
            + baseOneTableName
            + " where (datetime_test>? or datetime_test <?) and varchar_test like ? order by datetime_test desc limit 10,5";
        param.clear();
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        param.add(columnDataGenerator.varchar_testValue);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectSubqueryistinct() throws Exception {
        String sql = "select distinct t.varchar_test as dname from (select varchar_test from " + baseOneTableName
            + ") t";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectSubqueryCountDistinct() throws Exception {
        String sql = "select count(distinct varchar_test) as dname from (select varchar_test from " + baseOneTableName
            + ") t";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectSubqueryDistinctSubstring() throws Exception {
        String sql = "select distinct substring(t.varchar_test,1,4) as dname from (select varchar_test from "
            + baseOneTableName + ") t";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectJoinistinct() throws Exception {
        String sql = "select n.varchar_test from " + baseOneTableName + " n , " + baseTwoTableName
            + " s where n.integer_test = s.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectJoinCountDistinct() throws Exception {
        String sql = "select count(distinct t.varchar_test) as dname from (select n.varchar_test from "
            + baseOneTableName + " n , " + baseTwoTableName + " s where n.integer_test = s.pk ) t";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectJoinDistinctSubstring() throws Exception {
        String sql =
            " /*+TDDL({'extra':{'ALLOW_TEMPORARY_TABLE':'TRUE'}})*/select distinct substring(t.varchar_test,1,4) as dname from (select n.varchar_test from "
                + baseOneTableName
                + " n , "
                + baseTwoTableName
                + " s where n.integer_test = s.integer_test ) t";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectJoinSubqueryDistinct() throws Exception {
        String sql =
            "select distinct t.varchar_test as dname from (select n.varchar_test from (select distinct integer_test,varchar_test from "
                + baseOneTableName
                + " a) n , (select distinct integer_test,varchar_test from "
                + baseTwoTableName
                + " a) s where n.integer_test = s.integer_test ) t";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void selectGroupByShardColumn() throws Exception {
        String sql = "select varchar_test as SUM ,pk from " + baseOneTableName
            + " STUDENT group by pk  order by pk desc";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "pk");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectSimpleDistinct() throws Exception {
        String sql = "select distinct integer_test+1 as nid ,varchar_test as dname from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectFunctionDistinct() throws Exception {
        String sql = "select count(distinct integer_test+1+varchar_test) as cnt from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectFunctionDistinct_Group() throws Exception {
        String sql = "select count(distinct integer_test) as cnt, varchar_test from " + baseOneTableName
            + " group by varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectIndexHint() throws Exception {
        String sql = "select integer_test,varchar_test from " + baseOneTableName + " force index(primary)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selectFilterPushBug() throws Exception {
        String sql = "SELECT t2.pk , t2.integer_test, t2.varchar_test FROM (SELECT integer_test,varchar_test FROM "
            + baseOneTableName
            + " WHERE integer_test >1 and pk > 1) t1, "
            + baseOneTableName
            + " t2 WHERE t2.integer_test = t1.integer_test and t2.pk > 100 AND  t2.varchar_test <> t1.varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "SELECT t2.pk , t2.integer_test, t2.varchar_test FROM (SELECT integer_test,varchar_test FROM "
            + baseOneTableName + " WHERE integer_test >1 and pk > 1) t1, " + baseOneTableName
            + " t2 WHERE t2.integer_test = t1.integer_test and t2.pk> 100 AND  t2.varchar_test <> t1.varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }
}
