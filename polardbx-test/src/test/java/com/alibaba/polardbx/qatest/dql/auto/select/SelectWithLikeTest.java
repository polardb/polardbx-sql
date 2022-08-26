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
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectConutAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * like条件查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithLikeTest extends AutoReadBaseTestCase {
    ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithLikeTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void LikeAnyTest() throws Exception {

        String sql = "select * from " + baseOneTableName + "  where varchar_test like '"
            + columnDataGenerator.varchar_tesLikeValueOne + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select * from " + baseOneTableName + " where varchar_test like '"
            + columnDataGenerator.varchar_tesLikeValueTwo + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like '"
            + columnDataGenerator.varchar_tesLikeValueThree + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void LikeOneTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where varchar_test like 'hello123_'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like '_e34324_'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like '_iha_r_'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void LikeSpecificTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where varchar_test like 'adaabcwer'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like 'ZHuoXUE'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void LikeBinaryTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where varchar_test like binary 'zhuoxue'";
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void MatchCharTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where varchar_test like 'zhuoxue\\_yll'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like 'zhuoxue\\%yll'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void MatchCharWithEscapeTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where varchar_test like 'zhuoxue#_yll' escape '#'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like 'zhuoxue#%yll' escape '#'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void NotLikeTest() throws Exception {

        String sql = "select * from " + baseOneTableName + "  where varchar_test not like 'zhuo%'";
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test not like 'uo%'";
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test not like 'zhuoxu_'";
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test not like 'abdfee_'";
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test not like 'zhuoxue'";
        selectConutAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void likeWithAndTest() throws Exception {

        int id = 10;
        String sql = "select * from " + baseOneTableName + " where varchar_test like 'zhuoxue' and integer_test >" + id;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        id = 1000;
        sql = "select varchar_test,pk from " + baseOneTableName
            + " where varchar_test like 'zhuoxue\\%yll' and integer_test  <" + 10;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void likeWithLimit() throws Exception {

        String sql = "select * from " + baseOneTableName + " where varchar_test like ? limit ?";
        List<Object> param = new ArrayList<Object>();
        param.add("zhuo%");
        param.add(10);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName + " where varchar_test like ? limit ?,?";
        param.clear();
        param.add("%uo%");
        param.add(15);
        param.add(25);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql =
            "select * from " + baseOneTableName + " as tab  where varchar_test like ? and timestamp_test > ? limit ?,?";
        param.clear();
        param.add("%uo%");
        param.add(columnDataGenerator.timestamp_testValue);
        param.add(15);
        param.add(25);
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);

        sql = "select * from " + baseOneTableName
            + " as tab where (varchar_test like ? and timestamp_test > ?) limit ?,?";
        selectConutAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void likeWithOrder() throws Exception {

        String sql = "SELECT * from " + baseOneTableName
            + " where varchar_test like ? and timestamp_test> ? and timestamp_test< ? order by timestamp_test";
        List<Object> param = new ArrayList<Object>();
        param.add("%zh%xue%");
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "timestamp_test");

        sql = "SELECT * from " + baseOneTableName
            + " where varchar_test like ? and timestamp_test> ? and timestamp_test< ? order by timestamp_test desc";
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "timestamp_test");

        sql = "SELECT * from " + baseOneTableName
            + " as tab where (varchar_test like ? and (timestamp_test> ? and timestamp_test< ?)) order by  timestamp_test desc";
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "timestamp_test");

        sql = "SELECT * from "
            + baseOneTableName
            + " as tab where (varchar_test like '%zhuo%' and (timestamp_test> '2011-1-1' and timestamp_test< '2018-7-9')) order by  timestamp_test desc ";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "timestamp_test");

        sql = "SELECT * from "
            + baseOneTableName
            + " as tab where (varchar_test like ? and (timestamp_test> ? and timestamp_test< ?)) order by  timestamp_test desc ";
        param.clear();
        param.add("%zh%xue%");
        param.add(columnDataGenerator.timestamp_testStartValue);
        param.add(columnDataGenerator.timestamp_testEndValue);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "timestamp_test");
    }

}
