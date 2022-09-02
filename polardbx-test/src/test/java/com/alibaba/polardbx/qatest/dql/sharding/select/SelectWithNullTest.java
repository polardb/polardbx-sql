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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Null值查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithNullTest extends ReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithNullTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void isNull() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test  is null";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void isNotNull() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test  is not null";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void equalNullTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test  = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection, true);

        sql = "select * from " + baseOneTableName + " where varchar_test  =null";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    public void quoteNullTest() throws Exception {
        String sql = "select QUOTE(?) a from " + baseOneTableName;
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select QUOTE(null) a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void asciiNULLTest() throws Exception {
        String sql = String.format("select ASCII(varchar_test) as a from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selectNullTest() {

    }

    /**
     * @since 5.3.12
     */
    @Test
    public void shardingKeyIsNullTest() throws Exception {
        String sql = String.format("explain sharding select * from select_base_one_multi_db_multi_tb where pk is null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void shardingKeyEqualNullTest() throws Exception {
        String sql = String.format("explain sharding select * from select_base_one_multi_db_multi_tb where pk = null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
