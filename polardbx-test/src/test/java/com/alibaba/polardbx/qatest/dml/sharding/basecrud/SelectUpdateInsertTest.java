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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteCHNTableName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 包含insert，update，select基本查询
 *
 * @author xiaowen.guoxw
 * @since 5.0.1
 */

public class SelectUpdateInsertTest extends CrudBasedLockTestCase {

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteCHNTableName.allBaseTypeOneTable(ExecuteCHNTableName.UPDATE_DELETE_BASE_AUTONIC));
    }

    public SelectUpdateInsertTest(String customerTable) {
        this.baseOneTableName = customerTable;
    }

    @Before
    public void initData() {
        String sql = "delete from " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectAllFieldTest() {
        String sql = String.format("insert into %s (姓名,性别,单号) values (?, ?, ?)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, Arrays.asList(new Object[] {"郭晓文", "女", RANDOM_ID}));
        sql = String.format("select 姓名,性别,单号  from %s where 单号=%d", baseOneTableName, RANDOM_ID);

        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectAllFieldWithFuncTest() {
        String sql = String.format("insert into %s (姓名,性别,单号) values (?, ?, ?)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, Arrays.asList(new Object[] {"郭晓文", "女", RANDOM_ID}));
        sql = String
            .format("select  姓名, 性别, 单号, count(*) from %s where 单号=%d group by 单号, 姓名, 性别", baseOneTableName,
                RANDOM_ID);

        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectSomeFieldTest() {
        String sql = String.format("insert into %s (姓名,性别,单号) values (?, ?, ?)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, Arrays.asList(new Object[] {"郭晓文", "女", RANDOM_ID}));
        sql = String.format("select 姓名, 性别 from %s where 单号=%d", baseOneTableName, RANDOM_ID);

        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("select 姓名, 性别 from %s ", baseOneTableName);
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectWithQuotationTest() {
        String name = "郭\\'晓\\'文";
        String sql = String.format("insert into %s (姓名,性别,单号) values (?, ?, ?)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, Arrays.asList(new Object[] {name, "女", RANDOM_ID}));

        sql = String.format("select 姓名,性别 from %s", baseOneTableName);
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectWithMaxTest() {
        String name = "郭\\'晓\\'文";
        String sql = String.format("insert into %s (姓名,性别,单号) values (?, ?, ?)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, Arrays.asList(new Object[] {name, "女", RANDOM_ID}));

        sql = String.format("select max(编号) from %s where 姓名='%s'", baseOneTableName, name);
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.2.4
     */
    @Test
    public void insertTestAllFieldTest() {
        String sql = "insert into " + baseOneTableName + "(编号, 姓名, 性别, 单号) values(?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add("中文名字" + RANDOM_ID);
        param.add("女");
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);

        sql = "select * from " + baseOneTableName + " where 编号=" + RANDOM_ID;
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertTestMultiValuesTest() {
        StringBuilder sql = new StringBuilder("insert into " + baseOneTableName + "(编号, 姓名, 性别, 单号)  values(?,?,?,?)");
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID + 0);
        param.add("中文名字" + RANDOM_ID + "0");
        param.add("女");
        param.add(columnDataGenerator.integer_testValue);

        for (int i = 1; i < 40; i++) {
            sql.append(",(?,?,?,?)");
            param.add(RANDOM_ID + i);
            param.add("中文名字" + RANDOM_ID + i);
            param.add("女");
            param.add(columnDataGenerator.integer_testValue);
        }

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql.toString(), param);
        String selectSql = "select * from " + baseOneTableName + " order by 编号";
        selectOrderAssert(selectSql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertTestIgnoreTest() {
        String sql = String.format("insert into %s (编号, 姓名, 性别, 单号) values (?,?,?,?)", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add("中文名字" + RANDOM_ID);
        param.add("女");
        param.add(columnDataGenerator.integer_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);

        sql = String.format("insert ignore into %s (编号, 姓名, 性别, 单号) values (?,?,?,?)", baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);

        String selectSql = "select * from " + baseOneTableName;
        selectOrderAssert(selectSql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertTestGmtStringTest() {
        String sql = String.format("insert into %s (编号, 姓名, 性别, 单号, 订单时间) values (?,?,?,?,?)", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add("中文名字" + RANDOM_ID);
        param.add("女");
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);
        String selectSql = "select * from " + baseOneTableName;
        selectOrderAssert(selectSql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertTestWithSetTest() {
        String sql = String.format("insert into %s  set 单号=1, 姓名='郭晓文'", baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String selectSql = "select 姓名,性别,单号,订单时间 from " + baseOneTableName + " order by 编号";
        selectOrderAssert(selectSql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertTestWithNullTest() {
        String sql = "insert into " + baseOneTableName + " (编号, 姓名, 性别, 单号) values(?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        param.add(null);
        param.add(null);
        param.add(null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);
        sql = "select 姓名,性别,单号 from " + baseOneTableName + " order by 编号";
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void insertTestSubstringTest() {
        String sql = "insert into " + baseOneTableName + " (编号, 姓名, 性别, 单号) values(?,substring(?,-5),?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add("中文名字" + RANDOM_ID);
        param.add("女");
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);
        sql = "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }
    /*
     * 以上从InsertTest迁移过来
     */
}
