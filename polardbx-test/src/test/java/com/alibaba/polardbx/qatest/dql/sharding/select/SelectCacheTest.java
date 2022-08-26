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
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * select缓存测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectCacheTest extends ReadBaseTestCase {

    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    //    private String[] columnParam   = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
    Object[] stringName = {
        columnDataGenerator.getValueByColumnName("varchar_test"),
        columnDataGenerator.getValueByColumnName("varchar_test"),
        columnDataGenerator.getValueByColumnName("varchar_test")};

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectCacheTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectWhereTest() throws Exception {
        for (int i = 0; i < 4; i++) {
            String sql = "select * from " + baseOneTableName + " where integer_test=?";
            List<Object> param = new ArrayList<Object>();
            param.add(i);

            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

            sql = "select * from " + baseOneTableName + " where integer_test>? and varchar_test =?";
            param.clear();
            param.add(i);
            param.add(columnDataGenerator.varchar_testValue);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

            sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select * from " + baseOneTableName
                + " where integer_test=? or pk =?";
            param.clear();
            param.add(i);
            param.add(Long.parseLong(i + 1 + ""));
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectAliasTest() throws Exception {
        for (long i = 0; i < 4; i++) {
            String sql = "select * from  " + baseOneTableName + "  nor where nor.pk=?";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectOrderTest() throws Exception {
        for (long i = 0; i < 4; i++) {
            String sql = "select * from " + baseOneTableName
                + " where pk> ? and varchar_test is not null order by  varchar_test ";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");

            sql = "select * from " + baseOneTableName + " where pk >? order by varchar_test desc";
            selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectLimitTest() throws Exception {
        for (int i = 0; i < stringName.length; i++) {
            String orderByColumn = "integer_test";
            String sql = "select  *   from " + baseOneTableName + " where varchar_test= ? order by " + orderByColumn
                + " limit 2";
            List<Object> param = new ArrayList<Object>();
            param.add(stringName[i]);
            selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, orderByColumn);

            sql = "select " + orderByColumn + "  from " + baseOneTableName
                + " where varchar_test= ? order by integer_test desc limit 2";
            selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, orderByColumn);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectLGroupTest() throws Exception {
        String orderByColumn = "count(pk)";

        for (int i = 0; i < stringName.length; i++) {
            String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select timestamp_test,count(pk) from "
                + baseOneTableName + " where varchar_test=? group by timestamp_test  order by timestamp_test,"
                + orderByColumn;
            List<Object> param = new ArrayList<Object>();
            param.add(stringName[i]);
            selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, orderByColumn);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectOperatorTest() throws Exception {
        for (int i = 0; i < 4; i++) {
            String sql = "select * from " + baseOneTableName + " where integer_test>?";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

            sql = "select * from " + baseOneTableName + " where integer_test >=? and integer_test<?";
            param.clear();
            param.add(i);
            param.add(i + 10);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectBetweenTest() throws Exception {
        for (int i = 0; i < 4; i++) {
            String sql = "select * from " + baseOneTableName + " where integer_test between ? and ?";
            List<Object> param = new ArrayList<Object>();
            param.add(i - 3);
            param.add(i + 4);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

            sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select * from " + baseOneTableName
                + " where integer_test between ? and ? order by varchar_test";
            param.clear();
            param.add(i - 3);
            param.add(i + 4);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectLikeTest() throws Exception {
        for (int i = 0; i < stringName.length; i++) {
            String sql = "select * from " + baseOneTableName + " where varchar_test like ?";
            List<Object> param = new ArrayList<Object>();
            param.add(stringName[i]);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);

            sql = "select * from " + baseOneTableName + " where varchar_test like ? and integer_test>" + 3;
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }
    }

}
