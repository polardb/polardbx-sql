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

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * 子查询
 *
 * @author fangwu
 * @since 5.0.1
 */

public class SubQueryExecutorCorrecnessTest extends CrudBasedLockTestCase {

    ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.allBaseTypeTwoTableForSubquery(
            ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public SubQueryExecutorCorrecnessTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;

    }

    private List<Object> buildNullParams(List<ColumnEntity> columns) {
        Random r = new Random();
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, Math.abs(r.nextInt()));
        param.set(2, null);
        return param;
    }

    private void truncateTable(String tableName) {
        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            "delete from " + tableName,
            Lists.newArrayList());
    }

    @Test
    public void testNullInUnnull() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select pk from " + baseTwoTableName + " )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnull() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNull() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmpty() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormal() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNull() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNull() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmpty() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormal() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNull() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNull() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmpty() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmpty() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where exists (select * from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNull() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where exists (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormal() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where exists (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTMinNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test > (select MIN(integer_test) from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmpty2() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test >= (select (integer_test-1) from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test = (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    // @Test
    // public void testScalarNormalEqualsMulrow(){
    // /**
    // * prepare data
    // */
    // truncateTable(baseOneTableName);
    //
    // List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
    // String dataSql = buildInsertColumnsSQLWithTableName(columns,
    // baseOneTableName);
    // List<Object> params = buildNullParams(columns);
    //
    // executeOnMysqlAndTddl(mysqlConnection, polarDbXConnection,
    // dataSql, params);
    //
    // tableDataPrepare(baseTwoTableName, 20,
    // TableColumnGenerator.getAllTypeColum(),PK_COLUMN_NAME, mysqlConnection,
    // polarDbXConnection);
    //
    // String sql =
    // "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
    // + baseOneTableName + " where pk = (select pk from " + baseTwoTableName +
    // ")";
    // selectContentSameAssert(sql, null, mysqlConnection,
    // polarDbXConnection, true);
    //
    // }

    @Test
    public void testScalarNullGTNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test > (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test >= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test < (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test <= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test = any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test = any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test = any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test = any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk = any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk = any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    // @Test
    // public void testScalarNormalEqualsMulrow(){
    // /**
    // * prepare data
    // */
    // truncateTable(baseOneTableName);
    //
    // List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
    // String dataSql = buildInsertColumnsSQLWithTableName(columns,
    // baseOneTableName);
    // List<Object> params = buildNullParams(columns);
    //
    // executeOnMysqlAndTddl(mysqlConnection, polarDbXConnection,
    // dataSql, params);
    //
    // tableDataPrepare(baseTwoTableName, 20,
    // TableColumnGenerator.getAllTypeColum(),PK_COLUMN_NAME, mysqlConnection,
    // polarDbXConnection);
    //
    // String sql =
    // "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
    // + baseOneTableName + " where pk = (select pk from " + baseTwoTableName +
    // ")";
    // selectContentSameAssert(sql, null, mysqlConnection,
    // polarDbXConnection, true);
    //
    // }

    @Test
    public void testScalarNullGTAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test > any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test > any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test > any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test >= any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test >= any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test >= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test < any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test < any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test < any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test <= any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test <= any (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test <= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test = all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test = all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test = all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test = all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk = all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk = all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    // @Test
    // public void testScalarNormalEqualsMulrow(){
    // /**
    // * prepare data
    // */
    // truncateTable(baseOneTableName);
    //
    // List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
    // String dataSql = buildInsertColumnsSQLWithTableName(columns,
    // baseOneTableName);
    // List<Object> params = buildNullParams(columns);
    //
    // executeOnMysqlAndTddl(mysqlConnection, polarDbXConnection,
    // dataSql, params);
    //
    // tableDataPrepare(baseTwoTableName, 20,
    // TableColumnGenerator.getAllTypeColum(),PK_COLUMN_NAME, mysqlConnection,
    // polarDbXConnection);
    //
    // String sql =
    // "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
    // + baseOneTableName + " where pk = (select pk from " + baseTwoTableName +
    // ")";
    // selectContentSameAssert(sql, null, mysqlConnection,
    // polarDbXConnection, true);
    //
    // }

    @Test
    public void testScalarNullGTAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test > all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test > all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test > all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk > all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test >= all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test >= all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test >= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk >= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test < all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test < all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test < all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk < all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test <= all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " where integer_test <= all (select integer_test from "
                + baseTwoTableName
                + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where integer_test <= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormal() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNull() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmpty() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " where pk <= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testNullInUnnullCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select pk from "
                + baseTwoTableName
                + " where a.pk = pk )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnullCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNullCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmptyCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a  where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormalCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNullCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNullCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmptyCorrelate() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormalCorrelate() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNullCorrelate() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNullCorrelate() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmptyCorrelate() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test in (select integer_test from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmptyCorrelate() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where exists (select * from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNullCorrelate() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where exists (select integer_test from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormalCorrelate() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where exists (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = (select integer_test from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = (select integer_test from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk = (select integer_test from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk = (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk > (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk > (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk > (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk >= (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk < (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk < (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk < (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk <= (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = any (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk = any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk = any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > any (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk > any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk > any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk > any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= any (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= any (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < any (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk < any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk < any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk < any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= any (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= any (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= any (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = all (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test = all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk = all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk = all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test > all (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk > all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk > all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk > all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test >= all (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= all (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk >= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test < all (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName + " a where pk < all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk < all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk < all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where integer_test <= all (select pk from "
                + baseTwoTableName
                + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormalCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= all (select pk from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNullCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmptyCorrelate() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test  from "
                + baseOneTableName
                + " a where pk <= all (select integer_test from "
                + baseTwoTableName
                + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testNullInUnnullProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,  integer_test in (select pk from "
                + baseTwoTableName + " ) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnullProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNullProjectProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,   integer_test in (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmptyProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,  integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormalProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNullProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,  integer_test in (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNullProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,  integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmptyProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormalProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNullProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNullProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmptyProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test in (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmptyProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, exists (select * from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNullProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, exists (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormalProject() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, exists (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test = (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test = (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test = (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test = (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk = (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,  pk = (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test > (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test , integer_test > (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test > (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk > (select pk from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk > (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk > (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test >= (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test >= (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test >= (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk >= (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk >= (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk >= (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test , integer_test < (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test < (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test , integer_test < (select pk from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk < (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk < (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk < (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test <= (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test <= (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test , integer_test <= (select pk from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk <= (select pk from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk <= (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk <= (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk > any (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= any (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= any (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test = all (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = all (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk = all (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test > all (select integer_test from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > all (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > all (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= all (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= all (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test < all (select pk from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < all (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= all (select pk from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormalProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk <= all (select pk from "
                + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNullProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmptyProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= all (select integer_test from "
                + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testNullInUnnullCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select pk from "
                + baseTwoTableName + " where a.pk = pk )  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnullCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk )  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNullCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmptyCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormalCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNullCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNullCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmptyCorrelateProject() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormalCorrelateProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNullCorrelateProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNullCorrelateProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test in (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmptyCorrelateProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test in (select integer_test from "
                + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmptyCorrelateProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,exists (select * from "
                + baseTwoTableName + "  where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNullCorrelateProject() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,exists (select integer_test from "
                + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormalCorrelateProject() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,exists (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = (select integer_test from "
                + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test = (select integer_test from "
                + baseTwoTableName + "  where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = (select integer_test from "
                + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk <= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test > any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk > any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk > any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, pk > any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk >= any (select pk from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormalCorrelateProjectProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test, integer_test <= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= any (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk <= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= any (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test = all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test = all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test = all (select pk from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test = all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk = all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk = all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test > all (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk > all (select pk from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,pk > all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk > all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test >= all (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= all (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk >= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test < all (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < all (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk < all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test ,integer_test <= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,integer_test <= all (select pk from "
                + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormalCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= all (select pk from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNullCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmptyCorrelateProject() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql =
            "/*+TDDL: cmd_extra(rbo_rule_type=\"SUBQUERY_EXECUTOR\", MERGE_UNION_SIZE=1)*/ select pk,varchar_test,integer_test,pk <= all (select integer_test from "
                + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testNullInUnnullJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select pk from " + baseTwoTableName + " )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnullJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNullJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmptyJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormalJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNullJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNullJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmptyJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormalJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNullJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNullJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmptyJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test in (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmptyJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where exists (select * from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNullJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where exists (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormalJoin() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where exists (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk = (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    // @Test
    // public void testScalarNormalEqualsMulrowJoin(){
    // /**
    // * prepare data
    // */
    // truncateTable(baseOneTableName);
    //
    // List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
    // String dataSql = buildInsertColumnsSQLWithTableName(columns,
    // baseOneTableName);
    // List<Object> params = buildNullParams(columns);
    //
    // executeOnMysqlAndTddl(mysqlConnection, polarDbXConnection,
    // dataSql, params);
    //
    // tableDataPrepare(baseTwoTableName, 20,
    // TableColumnGenerator.getAllTypeColum(),PK_COLUMN_NAME, mysqlConnection,
    // polarDbXConnection);
    //
    // String sql = "  select pk,varchar_test,integer_test  from " +
    // baseOneTableName + " where pk = (select pk from " + baseTwoTableName +
    // ")";
    // selectContentSameAssert(sql, null, mysqlConnection,
    // polarDbXConnection, true);
    //
    // }

    @Test
    public void testScalarNullGTNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName + " where pk > (select pk from "
            + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName + " where pk < (select pk from "
            + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk = any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk = any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    // @Test
    // public void testScalarNormalEqualsMulrowJoin(){
    // /**
    // * prepare data
    // */
    // truncateTable(baseOneTableName);
    //
    // List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
    // String dataSql = buildInsertColumnsSQLWithTableName(columns,
    // baseOneTableName);
    // List<Object> params = buildNullParams(columns);
    //
    // executeOnMysqlAndTddl(mysqlConnection, polarDbXConnection,
    // dataSql, params);
    //
    // tableDataPrepare(baseTwoTableName, 20,
    // TableColumnGenerator.getAllTypeColum(),PK_COLUMN_NAME, mysqlConnection,
    // polarDbXConnection);
    //
    // String sql = "  select pk,varchar_test,integer_test  from " +
    // baseOneTableName + " where pk = (select pk from " + baseTwoTableName +
    // ")";
    // selectContentSameAssert(sql, null, mysqlConnection,
    // polarDbXConnection, true);
    //
    // }

    @Test
    public void testScalarNullGTAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= any (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= any (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test = all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk = all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk = all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    // @Test
    // public void testScalarNormalEqualsMulrowJoin(){
    // /**
    // * prepare data
    // */
    // truncateTable(baseOneTableName);
    //
    // List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
    // String dataSql = buildInsertColumnsSQLWithTableName(columns,
    // baseOneTableName);
    // List<Object> params = buildNullParams(columns);
    //
    // executeOnMysqlAndTddl(mysqlConnection, polarDbXConnection,
    // dataSql, params);
    //
    // tableDataPrepare(baseTwoTableName, 20,
    // TableColumnGenerator.getAllTypeColum(),PK_COLUMN_NAME, mysqlConnection,
    // polarDbXConnection);
    //
    // String sql = "  select pk,varchar_test,integer_test  from " +
    // baseOneTableName + " where pk = (select pk from " + baseTwoTableName +
    // ")";
    // selectContentSameAssert(sql, null, mysqlConnection,
    // polarDbXConnection, true);
    //
    // }

    @Test
    public void testScalarNullGTAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test > all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk > all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test >= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk >= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test < all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk < all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where integer_test <= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormalJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= all (select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNullJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmptyJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " where pk <= all (select integer_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testNullInUnnullCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select pk from " + baseTwoTableName + " where a.pk = pk )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnullCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNullCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmptyCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a  where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormalCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNullCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNullCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmptyCorrelateJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormalCorrelateJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNullCorrelateJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNullCorrelateJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmptyCorrelateJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test in (select integer_test from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmptyCorrelateJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where exists (select * from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNullCorrelateJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where exists (select integer_test from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormalCorrelateJoin() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where exists (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = (select integer_test from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = (select integer_test from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk = (select integer_test from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk = (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk = any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk = any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= any (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= any (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test = all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk = all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk = all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test > all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk > all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test >= all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalNotEqualAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk != all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk >= all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test < all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk < all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where integer_test <= all (select pk from " + baseTwoTableName + "  where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormalCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= all (select pk from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNullCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmptyCorrelateJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test  from " + baseOneTableName
            + " a where pk <= all (select integer_test from " + baseTwoTableName + " where a.pk = pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testNullInUnnullProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,  integer_test in (select pk from " + baseTwoTableName
            + " ) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnullProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNullProjectProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,   integer_test in (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmptyProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,  integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormalProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test, integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNullProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,  integer_test in (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNullProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,  integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmptyProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test, integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormalProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test, integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNullProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNullProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmptyProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test, integer_test in (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmptyProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test, exists (select * from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNullProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, exists (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormalProjectJoin() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test, exists (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test = (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test = (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test = (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test = (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk = (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,  pk = (select integer_test from " + baseTwoTableName
            + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test > (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test , integer_test > (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test > (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk > (select pk from " + baseTwoTableName + ") from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk > (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk > (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test >= (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test >= (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test >= (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk >= (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk >= (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk >= (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test , integer_test < (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test < (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test , integer_test < (select pk from " + baseTwoTableName
            + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk < (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk < (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk < (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test <= (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test <= (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test , integer_test <= (select pk from " + baseTwoTableName
            + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk <= (select pk from " + baseTwoTableName + ") from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk <= (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,pk <= (select integer_test from " + baseTwoTableName
            + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk = any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk = any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test > any (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk > any (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test ,pk > any (select integer_test from " + baseTwoTableName
            + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk > any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= any (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk >= any (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test < any (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < any (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk < any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= any (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= any (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= any (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= any (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test = all (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test = all (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test = all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk = all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,pk = all (select integer_test from " + baseTwoTableName
            + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test > all (select integer_test from "
            + baseTwoTableName + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test > all (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk > all (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk > all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk > all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= all (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk >= all (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test, integer_test < all (select pk from " + baseTwoTableName
            + ") from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < all (select pk from " + baseTwoTableName + ")  from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk < all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= all (select integer_test from "
            + baseTwoTableName + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= all (select pk from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormalProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,pk <= all (select pk from " + baseTwoTableName + ") from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNullProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmptyProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= all (select integer_test from " + baseTwoTableName
            + ")  from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testNullInUnnullCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select pk from " + baseTwoTableName
            + " where a.pk = pk )  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInHasnullCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk )  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInNullCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNullInEmptyCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQL(columns);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNormalCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInHasNullCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInNullCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testEmptyInEmptyCorrelateProjectJoin() {
        /**
         * clean data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNormalCorrelateProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test ,integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInNullCorrelateProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInHasNullCorrelateProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test in (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testNormalInEmptyCorrelateProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test,integer_test in (select integer_test from "
            + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * exists test
     */

    @Test
    public void testExistsEmptyCorrelateProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        String sql = "  select pk,varchar_test,integer_test ,exists (select * from " + baseTwoTableName
            + "  where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNullCorrelateProjectJoin() {
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        truncateTable(baseTwoTableName);

        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,exists (select integer_test from " + baseTwoTableName
            + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testExistsNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test,exists (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * scalar test
     */

    @Test
    public void testScalarNullEqualNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = (select integer_test from "
            + baseTwoTableName + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test = (select integer_test from "
            + baseTwoTableName + "  where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk = (select integer_test from " + baseTwoTableName
            + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk = (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk > (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk > (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk > (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGENormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGENullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk < (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk < (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk < (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLENullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLENormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLENullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk <= (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * ALL
     */

    @Test
    public void testScalarNullEqualAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test = any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk = any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk = any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test > any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test > any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test, pk > any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test ,pk > any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, pk > any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test ,pk >= any (select pk from " + baseTwoTableName
            + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test < any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNormalCorrelateProjectProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk < any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test, integer_test <= any (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= any (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,pk <= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAnyEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= any (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * all
     */

    @Test
    public void testScalarNullEqualAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test = all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test = all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullEqualAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test ,integer_test = all (select pk from " + baseTwoTableName
            + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test = all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test ,pk = all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk) from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalEqualsAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk = all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test > all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGTAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test > all (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test ,pk > all (select pk from " + baseTwoTableName
            + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test ,pk > all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGTAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk > all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "  select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullGEAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test >= all (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk >= all (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalGEAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk >= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <
     */

    @Test
    public void testScalarNullLTAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test < all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLTAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,integer_test < all (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < all (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        tableDataPrepare(baseTwoTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "  select pk,varchar_test,integer_test,pk < all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLTAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk < all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * <=
     */

    @Test
    public void testScalarNullLEAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test ,integer_test <= all (select integer_test from "
            + baseTwoTableName + " where a.pk = pk) from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNullLEAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);
        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,integer_test <= all (select pk from " + baseTwoTableName
            + "  where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNormalCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= all (select pk from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllNullCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        dataSql = buildInsertColumnsSQLWithTableName(columns, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    @Test
    public void testScalarNormalLEAllEmptyCorrelateProjectJoin() {
        /**
         * prepare data
         */
        truncateTable(baseOneTableName);
        truncateTable(baseTwoTableName);

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, baseOneTableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

        String sql = "  select pk,varchar_test,integer_test,pk <= all (select integer_test from " + baseTwoTableName
            + " where a.pk = pk)  from " + baseOneTableName + " a  ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    public String buildInsertColumnsSQL(List<ColumnEntity> columns) {
        return buildInsertColumnsSQL(columns, "INSERT INTO");
    }

    public String buildInsertColumnsSQL(List<ColumnEntity> columns, String head) {
        StringBuilder insert = new StringBuilder(head).append(" ").append(baseOneTableName).append(" (");
        StringBuilder values = new StringBuilder(" VALUES (");

        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getName();
            if (i > 0) {
                insert.append(",");
                values.append(",");
            }
            insert.append(columnName);
            values.append("?");
        }

        insert.append(")");
        values.append(")");

        return insert.append(values).toString();
    }
}
