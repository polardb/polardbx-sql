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
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * Set类型测试
 *
 * @author chenhui
 * @since 5.1.7
 */

public class SetTypeTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.SET_TYPE_TEST));
    }

    public SetTypeTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void prepare() throws SQLException {

        if (baseOneTableName.startsWith("broadcast")) {
            //JdbcUtil.setTxPolicy(ITransactionPolicy.FREE, tddlConnection);
        }

        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
    }

    /**
     * @since 5.1.7
     */
    @Test
    public void testInsertOneValue() {
        String sql = "insert into " + baseOneTableName + "(set_test,pk) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.7
     */
    @Test
    public void testInsertMultiValue() {
        String sql = "insert into " + baseOneTableName + "(set_test,pk) values(?,?),(?,?),(?,?),(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID);

        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID + 1);

        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID + 2);

        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID + 3);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.7
     */
    @Test
    @Ignore("kelude跑不过")
    public void testInsertWithWrongValue() {
        String sql = "insert into " + baseOneTableName + "(set_test,pk) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("a,b,chenhui,world");
        param.add(RANDOM_ID);
        try {
            JdbcUtil.updateDataTddl(tddlConnection, sql,
                param);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Data truncated for column 'set_test' at row 1");
        }
    }

    /**
     * @since 5.1.7
     */
    @Test
    public void testInsertMultiWithMissingValue() {
        String sql = "insert into " + baseOneTableName + "(set_test,pk) values(?,?),(?,?),(?,?),(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("b,chenhui,hello");
        param.add(RANDOM_ID);

        param.add("a,chenhui,hello");
        param.add(RANDOM_ID + 1);

        param.add("a,b,hello");
        param.add(RANDOM_ID + 2);

        param.add("a,b,chenhui");
        param.add(RANDOM_ID + 3);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.7
     */
    @Test
    public void testInsertMultiValueWithNormalSql() {
        String sql = "insert into " + baseOneTableName + "(set_test,pk) values(?,?),(?,?),(?,?),(?,?)";
        sql = String.format("insert into %s(set_test,pk) values(%s,%d),(%s,%d),(%s,%d),(%s,%d)",
            baseOneTableName,
            "'a,b,chenhui,hello'",
            RANDOM_ID,
            "'a,b,hello'",
            RANDOM_ID + 1,
            "'a,chenhui,hello'",
            RANDOM_ID + 2,
            "'a,b,chenhui'",
            RANDOM_ID + 3);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }
}
