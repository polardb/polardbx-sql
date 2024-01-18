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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 字符函数
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectCharacterFunctionTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectCharacterFunctionTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void concatTest() {
        String sql = "select * from " + baseOneTableName + " where varchar_test =concat(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("hell");
        param.add("o1234");
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        sql = "select * from " + baseOneTableName + " where varchar_test like concat (?,?,?)";
        param.clear();
        param.add("hell");
        param.add("o");
        param.add("1234");
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void concatArbitrarilyTest() {
        String sql = "select * from " + baseOneTableName + " where varchar_test like concat(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("%llo");
        param.add("123_");
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void ifnullTest() {
        String sql = "select ifnull(pk,varchar_test) as notNullName from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select ifnull(varchar_test,'ni') as notNullName from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select ifnull(varchar_test,'pk') as notNullName from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void ifnullTestTypeNotSame() {
        String sql = "select ifnull(varchar_test,pk) as notNullName from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void convTest() {
        String sql = String.format("select conv(integer_test,16,2) as a from %s where pk=1", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void asciiTest() {
        String sql = String.format("select ASCII(varchar_test) as a from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bit_lengthTest() {
        String sql = String.format("select BIT_LENGTH(varchar_test) as a from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void binTest() {
        String sql = String.format("select bin(varchar_test) as a from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void unPushedBinTest() {
        String sql = String.format("select t1.varchar_test, bin(concat(t1.varchar_test, t2.varchar_test)) as a " +
                "from %s t1 , %s t2 where t1.varchar_test = t2.varchar_test and t1.varchar_test=\"\";",
            baseOneTableName, baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void trimTest() {
        String sql = String.format("select trim(both 'x' from 'xxxaaxx') as a from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void trimOrderByTest() {
        String sql = String.format("/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_PUSH_PROJECT=false*/"
            + "select pk, varchar_test, trim(varchar_test) as a from %s order by a", baseOneTableName);
        System.out.println(sql);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void trimTest2() {
        String sql = String.format("select trim('x' from 'xxxaaxx') as a from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void trimTest3() {
        String sql = String.format("select trim('x' from 'xxxaaxx') as a from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
