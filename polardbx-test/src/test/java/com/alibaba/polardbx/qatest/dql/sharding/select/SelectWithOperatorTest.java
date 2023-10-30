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
 * 操作符查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */


public class SelectWithOperatorTest extends ReadBaseTestCase {

    ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithOperatorTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void greaterTest() throws Exception {
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
    public void lessTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk < ?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(columnDataGenerator.pkMaxDataSize + ""));
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        sql = "select * from " + baseOneTableName + " where pk <= ?";
        param.clear();
        param.add(Long.parseLong(columnDataGenerator.pkMaxDataSize - 1 + ""));
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        param.clear();
        param.add(0);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where ? <= pk";
        param.clear();
        param.add(Long.parseLong(10 + ""));
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void lessAndGreatTest() throws Exception {
        int start = 5;
        int end = 15;

        String sql = "select * from " + baseOneTableName + " where pk >=? and pk< ?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(start + ""));
        param.add(Long.parseLong(end + ""));
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where integer_test >=? and integer_test< ?";
        param.clear();
        param.add(start);
        param.add(end);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void lessAndGreatWithStringTest() throws Exception {
        int start = 5;
        String varcharString = "a";
        String sql = "select * from " + baseOneTableName + " where pk >=? and varchar_test>?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(start + ""));
        param.add(varcharString);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test<?";
        param.clear();
        param.add(varcharString);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test >?";
        param.clear();
        param.add(varcharString);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void multiCompareTest() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        float fl = 0.15f;
        String sql = "select * from " + baseOneTableName + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql =
            "select * from " + baseOneTableName + " where pk>=? and pk>? and varchar_test like ? or timestamp_test >?";
        param.clear();
        param.add(start1);
        param.add(start2);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        sql =
            "select * from " + baseOneTableName + " where pk>=? and pk>? and varchar_test like ? or timestamp_test >?";
        param.clear();
        param.add(start1);
        param.add(start2);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName
            + " where pk>? or (integer_test<? and varchar_test like ? and timestamp_test= ?)";
        param.clear();
        param.add(start1);
        param.add(1500);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName
            + " where pk<=? and integer_test>?  or varchar_test like ? and timestamp_test =? or float_test=?";
        param.clear();
        param.add(start1);
        param.add(516);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        param.add(fl);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        sql = "select * from " + baseOneTableName
            + " where pk<=? and integer_test>?  or varchar_test like ? and datetime_test =? or float_test=?";
        param.clear();
        param.add(start1);
        param.add(516);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(fl);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void lessAndGreatNoDataTest() throws Exception {
        long start = 5;
        long end = 15;
        String sql = "select * from " + baseOneTableName + " where pk<? and pk>?";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(end);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void cloumnCompareTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk > integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void notEqualsTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk <> ?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(0 + ""));

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        sql = "select * from " + baseOneTableName + " where pk != ?";
        param.clear();
        param.add(Long.parseLong(0 + ""));
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void notEqualsWithInTest() throws Exception {
        String sql = "select * from " + baseOneTableName
            + " where pk in(?,?,?) and integer_test !=? and integer_test !=? and varchar_test like ?";
        List<Object> param = new ArrayList<Object>();
        param.add(1l);
        param.add(2l);
        param.add(3l);
        param.add(100);
        param.add(700);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void nullSafeEqualTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test <=> ?";
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bitwiseAndTest() throws Exception {
        String sql = "select count(*) from " + baseOneTableName
            + " where pk in (?,?,?) and varchar_test =? and integer_test & ? = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(1);
        param.add(2);
        param.add(3);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(300);
        param.add(300);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bitwiseLikeTest() throws Exception {
        String sql = "select count(*) from " + baseOneTableName
            + " where pk in (?,?,?) and varchar_test like ? and integer_test & ? = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(1);
        param.add(2);
        param.add(3);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(300);
        param.add(300);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void xorTest() throws Exception {
        String sql = "select count(*) from " + baseOneTableName + " where pk ^ integer_test > ?";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select count(*) from " + baseOneTableName + " where pk xor integer_test > ?";
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void multiplicationTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where integer_test > pk * float_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void dieTest() throws Exception {
        String sql = "select * from " + baseOneTableName + "  where integer_test % ? = ? and varchar_test like ? ";
        List<Object> param = new ArrayList<Object>();
        param.add(10);
        param.add(1);
        param.add(columnDataGenerator.varchar_tesLikeValueOne);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void divisionTest() throws Exception {
        String sql = "select sum(integer_test)/sum(pk)  as avg from " + baseOneTableName + "  where varchar_test =? ";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complicateCalcuationTest() throws Exception {
        String sql =
            "select integer_test/(pk+1)*float_test as c from " + baseOneTableName + " where varchar_test=? order by pk";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void constantCalcuationTest() throws Exception {
        String sql = "select 20*2 a ,integer_test from " + baseOneTableName + " where varchar_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void calcuationTest() throws Exception {
        String sql = "select 20*2 a ,integer_test from " + baseOneTableName + " where varchar_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void calcuationWhereTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where integer_test>=?+?";
        List<Object> param = new ArrayList<Object>();
        param.add(10);
        param.add(20);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        sql = "select * from " + baseOneTableName + " where ?+?!=integer_test";
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void and() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test like ? && pk > ? ";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(500);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void caseWhen() throws Exception {
        {
            String sql = "select case pk when 500 then 600 else 700 end pk1 from " + baseOneTableName;

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        {
            String sql = "select case pk when 500 then 600 when 1 then 3 else 700 end pk1 from " + baseOneTableName;

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        {
            String sql = "select case pk when 500 then 600 when 1 then 3 end pk1 from " + baseOneTableName;

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        {
            String sql = "select case  when pk=500 then 600 else 700 end pk1 from " + baseOneTableName;

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        {
            String sql = "select case  when pk=500 then 600 when pk>700 then 900 else 700 end pk1 from "
                + baseOneTableName;

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        {
            String sql = "select case  when pk=500 then 600 when pk>700 then 900 end pk1 from " + baseOneTableName;

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        {
            String sql = "select case  when pk=500 then 600 when pk>700 then 900 else null end pk1 from "
                + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }
}
