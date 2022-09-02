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
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * 批量Update测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class BatchUpdateTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {

        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public BatchUpdateTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateAll() throws Exception {

        String sql = "UPDATE "
            + baseOneTableName
            + " SET varchar_test=?,char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?"
            + ",timestamp_test=?,year_test=?";

        List<List<Object>> params = new ArrayList<List<Object>>();

        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOne() throws Exception {

        String sql = "UPDATE "
            + baseOneTableName
            + " SET varchar_test=?,char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=? WHERE pk=?";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
            param.add(10);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome() throws Exception {

        String sql = "UPDATE "
            + baseOneTableName
            + " SET varchar_test=?,char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=? WHERE pk BETWEEN ? AND ?";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
            param.add(9);
            param.add(19);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome2() throws Exception {

        String sql = "UPDATE "
            + baseOneTableName
            + " SET varchar_test=?,char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=? WHERE pk<? and pk>?";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
            param.add(15);
            param.add(10);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome3() throws Exception {

        String sql = "UPDATE "
            + baseOneTableName
            + " SET varchar_test=?,char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=?  WHERE (pk>9 or pk<19) and pk>?";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 1; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
//			param.add(9);
//			param.add(19);
            param.add(12);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome4() throws Exception {

        String sql = "UPDATE "
            + baseOneTableName
            + " SET varchar_test=?,char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=?   WHERE pk in (?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
            param.add(6);
            param.add(12);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

}
