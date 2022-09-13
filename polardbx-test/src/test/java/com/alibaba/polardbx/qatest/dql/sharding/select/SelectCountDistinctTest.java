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
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * count(distinct)测试
 *
 * @author chenhui
 * @since 5.1.14
 */

public class SelectCountDistinctTest extends ReadBaseTestCase {

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectCountDistinctTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void test1CountDistinct() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test)) from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void test1CountDistinctWithAlias() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test)) as integer_test from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testManyCountDistinct() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql =
            "select count(distinct(integer_test)),count(distinct(date_test)),count(integer_test),count(date_test) from "
                + baseOneTableName + " group by pk having pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    @Test
    public void testGroupByCountDistinct() throws Exception {

        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql =
            "select pk, sum(distinct(integer_test)),count(distinct(date_test)),sum(integer_test),count(date_test) from "
                + baseOneTableName + "  where pk>=? and pk>? and pk <=? and pk<?  group by pk ";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testCountDistinct() throws Exception {
        String sql =
            "select pk, count(distinct(integer_test)),count(distinct(date_test)),count(integer_test),count(date_test) from "
                + baseOneTableName + " group by pk";
        List<Object> param = new ArrayList<Object>();

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testAddCountDistinct() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test))+count(distinct(date_test)) from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testSubCountDistinct() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test))-count(distinct(date_test)) from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testMultiCountDistinct() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test))*count(distinct(date_test)) from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testDivCountDistinct() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test))/count(distinct(date_test)) from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testCountDistinctAddSum() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test))+sum(pk) from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void testCountDistinctGroupby() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        String sql = "select count(distinct(integer_test))+sum(pk) from " + baseOneTableName
            + " where pk>=? and pk>? and pk <=? and pk<? group by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }
}
