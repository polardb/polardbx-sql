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
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.TWO;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * 以下从ComplexBatchUpdateTest迁移过来，多线程update
 */


public class ComplexBatchUpdateTwoTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    private static final String HINT1 = "";
    //多线程执行
    private static final String HINT2 =
        "/*+TDDL:CMD_EXTRA(UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ ";

    private final String HINT;

    @Parameters(name = "{index}:hint={0},table1={1}, table2={2}")
    public static List<String[]> prepareData() {
        List<String[]> allTests = new ArrayList<>();
        final List<String[]> tableNames = Arrays.asList(
            new String[][] {
                //相同库表会直接下推
                {
                    ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_ONE_TB_SUFFIX,
                    ExecuteTableName.UPDATE_DELETE_BASE + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {
                    ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_MUTIL_TB_SUFFIX,
                    ExecuteTableName.UPDATE_DELETE_BASE + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {
                    ExecuteTableName.UPDATE_DELETE_BASE + MULTI_DB_ONE_TB_SUFFIX,
                    ExecuteTableName.UPDATE_DELETE_BASE + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {
                    ExecuteTableName.UPDATE_DELETE_BASE + MUlTI_DB_MUTIL_TB_SUFFIX,
                    ExecuteTableName.UPDATE_DELETE_BASE + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            }
        );
        tableNames.forEach(strings -> allTests.add(new String[] {HINT2, strings[0], strings[1]}));

        return allTests;
    }

    public ComplexBatchUpdateTwoTest(String tHint, String baseOneTableName, String baseTwoTableName) {
        HINT = tHint;
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateOne() {

        String sql = HINT + "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.varchar_test=?,a.char_test=?,a.blob_test=?,a.integer_test=?,a.tinyint_test=?,a.tinyint_1bit_test=?,a.smallint_test=?,a."
            + "mediumint_test=?,a.bit_test=?,a.bigint_test=?,a.float_test=?,a.double_test=?,a.decimal_test=?,a.date_test=?,a.time_test=?,a.datetime_test=?  "
            + ",a.timestamp_test=?,a.year_test=? WHERE a.pk=? and a.pk = b.pk";
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
            param.add(9);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateSome() {

        String sql = HINT + "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.varchar_test=?,a.char_test=?,a.blob_test=?,a.integer_test=?,a.tinyint_test=?,a.tinyint_1bit_test=?,a.smallint_test=?,a."
            + "mediumint_test=?,a.bit_test=?,a.bigint_test=?,a.float_test=?,a.double_test=?,a.decimal_test=?,a.date_test=?,a.time_test=?,a.datetime_test=?  "
            + ",a.timestamp_test=?,a.year_test=? WHERE a.pk BETWEEN ? AND ? and a.pk = b.pk";
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
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateSome2() {
        String sql = HINT + "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.varchar_test=?,a.char_test=?,a.blob_test=?,a.integer_test=?,a.tinyint_test=?,a.tinyint_1bit_test=?,a.smallint_test=?,a."
            + "mediumint_test=?,a.bit_test=?,a.bigint_test=?,a.float_test=?,a.double_test=?,a.decimal_test=?,a.date_test=?,a.time_test=?,a.datetime_test=?  "
            + ",a.timestamp_test=?,a.year_test=? WHERE a.pk<? and a.pk>?  and a.pk = b.pk";
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
            param.add(19);
            param.add(9);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateSome3() {
        String sql = HINT + "UPDATE "
            + baseOneTableName
            + " a , "
            + baseTwoTableName
            + " b "
            + " SET a.varchar_test=?,a.char_test=?,a.blob_test=?,a.integer_test=?,a.tinyint_test=?,a.tinyint_1bit_test=?,a.smallint_test=?,a."
            + "mediumint_test=?,a.bit_test=?,a.bigint_test=?,a.float_test=?,a.double_test=?,a.decimal_test=?,a.date_test=?,a.time_test=?,a.datetime_test=?  "
            + ",a.timestamp_test=?,a.year_test=? WHERE a.pk in (?,?)  and a.pk = b.pk";
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            columnDataGenerator.getAllColumnValueExceptPk(param);
            param.add(19);
            param.add(9);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection,
            tddlConnection, sql, params, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

}
