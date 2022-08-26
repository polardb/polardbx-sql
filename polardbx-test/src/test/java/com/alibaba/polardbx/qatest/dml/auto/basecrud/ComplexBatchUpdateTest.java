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
import com.alibaba.polardbx.qatest.util.ConfigUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
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
 * 复杂批量Update测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class ComplexBatchUpdateTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table1={0}, table2={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeTwoStrictSameTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public ComplexBatchUpdateTest(String baseOneTableName, String baseTwoTableName) {
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
     * 以下从ComplexBatchUpdateTest迁移过来
     */

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateOne() throws Exception {

        String sql = "UPDATE "
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

        if (ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            executeBatchOnMysqlAndTddl(mysqlConnection,
                tddlConnection, sql, params, true);
            sql = "SELECT * FROM " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
            sql = "select * from " + baseTwoTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
        } else {
            JdbcUtil.updateDataBatchFailed(tddlConnection, sql, params, "not supported");
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateSome() throws Exception {

        String sql = "UPDATE "
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

        if (ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            executeBatchOnMysqlAndTddl(mysqlConnection,
                tddlConnection, sql, params, true);
            sql = "SELECT * FROM " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
            sql = "select * from " + baseTwoTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
        } else {
            JdbcUtil.updateDataBatchFailed(tddlConnection, sql, params, "Merge is not supported");
        }

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateSome2() throws Exception {
        String sql = "UPDATE "
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

        if (ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            executeBatchOnMysqlAndTddl(mysqlConnection,
                tddlConnection, sql, params, true);
            sql = "SELECT * FROM " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
            sql = "select * from " + baseTwoTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
        } else {
            JdbcUtil.updateDataBatchFailed(tddlConnection, sql, params, "Merge is not supported");
        }

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexUpdateSome3() throws Exception {
        String sql = "UPDATE "
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

        if (ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            executeBatchOnMysqlAndTddl(mysqlConnection,
                tddlConnection, sql, params, true);
            sql = "SELECT * FROM " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
            sql = "select * from " + baseTwoTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
        } else {
            JdbcUtil.updateDataBatchFailed(tddlConnection, sql, params, "Merge is not supported");
        }

    }

}
