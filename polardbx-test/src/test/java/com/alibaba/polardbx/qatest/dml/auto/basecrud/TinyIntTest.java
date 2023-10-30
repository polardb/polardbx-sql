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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * TinyInt类型测试
 *
 * @author chenhui
 * @since 5.1.18
 */

public class TinyIntTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.TINY_INT_TEST));

    }

    public TinyIntTest(String tableName) throws SQLException {
        baseOneTableName = tableName;
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }

    @Before
    public void prepare() throws Exception {
        String sql = String.format("delete from %s", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void testGetObject() throws Exception {
        String sql = "insert into " + baseOneTableName + "(tinyintr,tinyintr_1,tinyintr_3) values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(101);
        param.add(102);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null,
            mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void testGetInt() throws Exception {
        String sql = "insert into " + baseOneTableName + "(tinyintr,tinyintr_1,tinyintr_3) values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(101);
        param.add(102);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null,
            mysqlConnection, tddlConnection);
    }
}
