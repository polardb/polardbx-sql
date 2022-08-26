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
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * 值比较的条件查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithFilterTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithFilterTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void greaterTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk)>? order by varchar_test ";
        List<Object> param = new ArrayList<Object>();
        param.add(5L);

        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void greaterEqualTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk)>=? order by varchar_test ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void lessTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk)<? order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(90L);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void lessEqualTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk)<=? order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(90L);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void equalTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk)=? order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(54L);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test", true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void inTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk) in (?,?,?,?) order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(35);
        param.add(52);
        param.add(53);
        param.add(60);

        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test", true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void notEqualTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by varchar_test having count(pk) != ? order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void andTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having count(pk) != ? and count(pk) < ? order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(51L);
        param.add(90);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void groupOrTest() throws Exception {
        String sql = "select varchar_test,count(pk) from "
            + baseOneTableName
            + " where (pk > 2 or pk <500) AND varchar_test is not null group by  varchar_test having count(pk) in (?,?,?) order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(54);
        param.add(70);
        param.add(80);

        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test", true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void orTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having count(pk) != ? or count(pk) < ? order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void andOrTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having (count(pk) != ? or count(pk) < ?) and count(pk)>? order by varchar_test";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        param.add(5L);
        selectOrderByNotPrimaryKeyAssert(sql, param, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void constantTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having 1 order by varchar_test";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void constantTest2() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having true order by varchar_test";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void constantTest3() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having 'true' order by varchar_test";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test", true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void constantAndTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having 1 and 2 order by varchar_test";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test");
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void constantOrTest() throws Exception {
        String sql = "select varchar_test,count(pk) from " + baseOneTableName
            + " group by  varchar_test having 1 or 2 order by varchar_test";
        selectOrderByNotPrimaryKeyAssert(sql, null, mysqlConnection, tddlConnection, "varchar_test");
    }

}
