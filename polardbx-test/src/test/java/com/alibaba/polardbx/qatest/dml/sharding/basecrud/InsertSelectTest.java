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
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * 1. select的拆分键有函数，不下推 2. select没有拆分键，不下推 3. select或者insert除非同时为广播表，否则不下推 4.
 * insert列中没有拆分键，不下推 5. 表分布不同，如果可以计算能够落入同一个分库，也可以下推 TODO Comment of
 * InsertSelectTest
 *
 * @author xiaowen.guoxw
 */

public class InsertSelectTest extends CrudBasedLockTestCase {

    List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
    String selectColumn = "integer_test,date_test,timestamp_test,datetime_test,varchar_test,float_test,blob_test";
    String tableNameForCheck1;
    String tableNameForCheck2;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeTwoTable(ExecuteTableName.UPDATE_DELETE_BASE_AUTONIC));
    }

    public InsertSelectTest(String baseOneTableName,
                            String baseTwoTableName) throws SQLException {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Before
    public void initData() throws Exception {
        if (baseOneTableName.endsWith("broadcast")) {
            tableNameForCheck1 = JdbcUtil.getTopology(tddlConnection, baseOneTableName).get(0).getValue();
        } else {
            this.tableNameForCheck1 = baseOneTableName;
        }
        if (baseTwoTableName.endsWith("broadcast")) {
            tableNameForCheck2 = JdbcUtil.getTopology(tddlConnection, baseTwoTableName).get(0).getValue();
        } else {
            this.tableNameForCheck2 = baseTwoTableName;
        }

        String sql = "delete from  " + baseTwoTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        // 从1开始是因为client方式处理自增时，如果batch插入的起始值是0，则一批都会被当做默认自增主键

        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL AUTO_INCREMENT", "getPk()"));
        columns.add(new ColumnEntity("varchar_test", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("integer_test", "int(11) ",
            "DEFAULT NULL", "integer_testDifference()"));
        columns.add(new ColumnEntity("date_test", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        columns.add(new ColumnEntity("timestamp_test", "timestamp ",
            " NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP",
            "timestamp_testRandom()"));
        columns.add(new ColumnEntity("datetime_test", "datetime ",
            "DEFAULT NULL", "datetime_testRandom()"));
        columns.add(new ColumnEntity("float_test", "float ", "DEFAULT NULL",
            "float_testRandom()"));
        columns.add(new ColumnEntity("blob_test", "blob", "DEFAULT NULL",
            "blob_testRandom()"));

        columnDataGenerator.clearTestValues();

        tableDataPrepare(baseOneTableName, 1, 21,
            columns, PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectTest() {

        String sql = String
            .format(
                "insert into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + " select varchar_test,pk+1000,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s where pk > 10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectPkNoFunctionTest() {

        String sql = String
            .format(
                "insert ignore into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s where pk > 10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithSelectNoPkTest() {

        String sql = String
            .format("insert into %s(pk,blob_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select distinct(integer_test)+100,blob_test,float_test,date_test,datetime_test,timestamp_test from %s",

                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithSelectNoPkButWithWhereTest() {

        String sql = String
            .format("insert into %s(pk,varchar_test,blob_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select distinct(integer_test)+100,varchar_test,blob_test,float_test,date_test,datetime_test,timestamp_test from %s where pk > 10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithInsertNoPkTest() {

        String sql = String
            .format(
                "insert into %s(varchar_test,integer_test,blob_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,float_test,date_test,datetime_test,timestamp_test from %s where pk > 10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String cnSql = "select " + selectColumn + "  from " + tableNameForCheck2;
        String dnSql = "select " + selectColumn + "  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithInsertSelectNoPkTest() {

        tableDataPrepare(baseOneTableName, 1, 50,
            columns, PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = String
            .format("insert into %s(integer_test,date_test,datetime_test,timestamp_test,float_test,blob_test) "
                    + "select integer_test,date_test,datetime_test,timestamp_test,float_test,blob_test from %s order by pk limit 200",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        String cnSql = "select " + selectColumn + "  from " + tableNameForCheck2;
        String dnSql = "select " + selectColumn + "  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectPartlyColumnTest() {

        String sql = String
            .format("insert into %s(pk,integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select pk+100,integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test from %s ",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectIgnoreTest() {

        // 准备完全相同的数据
        String sql = "";
        if (!baseOneTableName.equals(baseTwoTableName)) {
            sql = String
                .format(
                    "insert into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                        + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s ",
                    baseTwoTableName, baseOneTableName);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                sql, null);
        }

        sql = String
            .format(
                "insert ignore into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s ",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithDuplicateTest() {

        // 准备完全相同的数据
        String sql = "";

        sql = String
            .format(
                "insert into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s on duplicate key update float_test=1.0",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String cnSql = "select " + selectColumn + "  from " + tableNameForCheck2;
        String dnSql = "select " + selectColumn + "  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);

        cnSql += " where pk in( 18, 17)";
        dnSql += " where pk in( 18, 17)";
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithDuplicate2Test() {

        // 准备完全相同的数据
        String sql = "";

        sql = String
            .format(
                "insert into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s on duplicate key update float_test=values(float_test) + values(integer_test)",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String cnSql = "select " + selectColumn + "  from " + tableNameForCheck2;
        String dnSql = "select " + selectColumn + "  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);

        cnSql += " where pk in( 18, 17)";
        dnSql += " where pk in( 18, 17)";
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithJoinTest() {
        if (baseOneTableName.equals(baseTwoTableName)) {
            return;
        }
        // 准备完全相同的数据
        String sql = "";
        sql = String
            .format("insert into %s(integer_test,pk,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select integer_test,pk,datetime_test,timestamp_test,varchar_test,float_test,blob_test from %s ",
                baseTwoTableName,
                baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        sql = String
            .format("insert  into %s(integer_test,pk,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select a.integer_test, a.pk + 100, a.datetime_test, a.timestamp_test,a.varchar_test,b.float_test,b.blob_test from %s as a join %s as b where a.pk=b.pk+1 ",
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithGroupByTest() {

        String sql = String.format("insert ignore into %s(integer_test,varchar_test) "
                + "select count(pk),varchar_test from %s group by varchar_test order by integer_test",
            baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String cnSql = "select integer_test,varchar_test  from " + tableNameForCheck2;
        String dnSql = "select integer_test,varchar_test  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithAliasTest() {

        String sql = String
            .format("insert into %s(integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select integer_test idd,datetime_test gmtc,timestamp_test gmtt,varchar_test namen,float_test,blob_test from %s where pk > 10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String cnSql = "select " + selectColumn + "  from " + tableNameForCheck2;
        String dnSql = "select " + selectColumn + "  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithAliasPKTest() {

        String sql = String
            .format(
                "insert ignore into %s(pk, integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select pk pk1, integer_test,datetime_test gmtc,timestamp_test gmtt ,varchar_test namen,float_test,blob_test from %s where pk > 10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithSubQueryTest() {

        String sql = String
            .format(
                "insert ignore into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s where pk in (select pk from %s where integer_test>100)",
                baseTwoTableName, baseOneTableName,
                baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithNullTest() {

        String sql = String
            .format(
                "insert ignore into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s where varchar_test is NULL",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select *  from " + tableNameForCheck2;
        String dnSql = "select *  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql, true);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithLimitTest() {

        String sql = String
            .format(
                "insert ignore into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s where pk > 10 order by integer_test limit 2",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithoutColumnTest() {

        String sql = String.format("insert ignore into %s select pk, integer_test, varchar_test, char_test, "
                + "blob_test, tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test, bigint_test, "
                + "float_test, double_test, decimal_test, date_test, time_test, datetime_test, timestamp_test, "
                + "year_test, mediumtext_test from %s where pk > 10",
            baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void insertSelectWithoutColumn2Test() {

        String sql = String.format("insert ignore into %s "
                + "select * from %s where pk > 10", baseTwoTableName,
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithoutAutoIncrementColumnTest() {

        tableDataPrepare(baseOneTableName, 1, 21,
            columns, PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
//		normaltblPrepare(1, 20,baseOneTableName, mysqlConnection, tddlConnection);

        String sql = String
            .format(
                "insert ignore into %s(integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test,pk) "
                    + "select integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test,null from %s where pk > 10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select " + selectColumn + "  from " + tableNameForCheck2;
        String dnSql = "select " + selectColumn + "  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWithNullAutoIncrementColumnTest() {

        String sql = String.format(
            "insert ignore into %s(pk, varchar_test) select null, 'abc'",
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select " + selectColumn + "  from " + tableNameForCheck2;
        String dnSql = "select " + selectColumn + "  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectWith0AutoIncrementColumnTest() {

        String sql = String.format(
            "insert ignore into %s(pk, varchar_test) select 0, \"gxw\"",
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select INTEGER_TEST, DATE_TEST, VARCHAR_TEST, FLOAT_TEST, TIMESTAMP_TEST, DATETIME_TEST from "
            + tableNameForCheck2;
        String dnSql = "select INTEGER_TEST, DATE_TEST, VARCHAR_TEST, FLOAT_TEST, TIMESTAMP_TEST, DATETIME_TEST from "
            + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void batchInsertSelectTest() {
        String sql = String
            .format("insert into %s(integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test from %s where pk > ?",
                baseTwoTableName, baseOneTableName);
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 1; i < 3; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        String cnSql = "select INTEGER_TEST, DATE_TEST, VARCHAR_TEST, FLOAT_TEST, TIMESTAMP_TEST, DATETIME_TEST from "
            + tableNameForCheck2;
        String dnSql = "select INTEGER_TEST, DATE_TEST, VARCHAR_TEST, FLOAT_TEST, TIMESTAMP_TEST, DATETIME_TEST from "
            + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void batchInsertSelectColumnParamTest() {
        String sql = String
            .format(
                "insert ignore into %s(pk, integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select pk, integer_test,datetime_test,timestamp_test,varchar_test,?,? from %s where pk > ?",
                baseTwoTableName, baseOneTableName);
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 1; i < 3; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add("float_test");
            param.add("blob_test");
            param.add(i);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertSelectColumnParamTest() {

        String sql = String
            .format(
                "insert ignore into %s(pk,integer_test,datetime_test,timestamp_test,varchar_test,float_test,blob_test) "
                    + "select pk,integer_test,datetime_test,timestamp_test,varchar_test,?,? from %s where pk > ?",
                baseTwoTableName, baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add("float_test");
        param.add("blob_test");
        param.add(10);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        String cnSql = "select * from " + tableNameForCheck2;
        String dnSql = "select * from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Ignore
    @Test
    public void testInsertSelectWithGroupConcat() {

        String sql = String
            .format("insert into %s(integer_test,varchar_test) "
                    + "select max(integer_test), group_concat(distinct varchar_test separator '|') from %s where pk > 10 group by varchar_test",
                baseTwoTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select integer_test,varchar_test  from " + tableNameForCheck2;
        String dnSql = "select integer_test,varchar_test  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Ignore
    @Test
    public void testInsertSelectWithGroupConcatRunError() {

        String sql = String
            .format("insert into %s(integer_test,varchar_test) "
                    + "select max(integer_test), group_concat(distinct varchar_test order by integer_test separator '|') from %s where pk > 100 group by integer_test",
                baseTwoTableName, baseOneTableName);
        if (baseOneTableName.contains("multi")) {
            executeErrorAssert(tddlConnection, sql, null,
                "Group by column differ from columns in distinct group_concat");
        } else {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        }
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertWhenSelectResultAlwaysEmptyTest() {

        String sql = String
            .format(
                "insert into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + "select varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s where pk > 10 and pk <10",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String cnSql = "select *  from " + tableNameForCheck2;
        String dnSql = "select *  from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql, true);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void insertWhenUnionAllTest() {

        String sql = String.format("insert into %s(pk,integer_test,varchar_test) "
                + "select 30, 2, 'gxw' union all select 40,5,'gxw1'",
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        String cnSql = "select pk,integer_test,varchar_test from " + tableNameForCheck2;
        String dnSql = "select pk,integer_test,varchar_test from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void unionDistinctLimitInsertSelectLimitTest() {
        tableDataPrepare(baseTwoTableName, 1, 20,
            columns, PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = String.format(
            "insert into %s(pk, varchar_test) select distinct(integer_test) + 1000 as idd ,varchar_test from %s where integer_test>15 or integer_test<5 union distinct "
                + "(select pk + 300,varchar_test from %s where 8<pk  order by pk limit 10) order by varchar_test, idd limit 100",
            baseTwoTableName,
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String cnSql = "select pk, varchar_test from " + tableNameForCheck2;
        String dnSql = "select pk, varchar_test from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    //从SelectWithUnionTest迁移过来
    @Test
    public void unionAllLimitInsertSelectLimitTest() {
        tableDataPrepare(baseTwoTableName, 1, 20,
            columns, PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
//		normaltblPrepare(1, 20,baseTwoTableName, mysqlConnection, tddlConnection);

        String sql = String.format(
            "insert into %s (pk, varchar_test) select distinct(integer_test) + 1000 as idd ,varchar_test from %s where integer_test>15 or integer_test<5 union all "
                + "(select pk + 300,varchar_test from %s where 8<pk and pk<12 order by integer_test limit 10) order by varchar_test, idd limit 100",
            baseTwoTableName,
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String cnSql = "select pk, varchar_test from " + tableNameForCheck2;
        String dnSql = "select pk, varchar_test from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void selectWithStringWithQuotes() throws Exception {
        String sql = "insert into " + baseTwoTableName
            + " (pk, varchar_test) values (9999, \"\\\"jim\\\'s is workering\\\"\")";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = "select * from " + baseTwoTableName + " WHERE varchar_test = \"\\\"jim\\'s is workering\\\"\"";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Insert tb1 select tb2, where tb1=tb2.
     * To test if inserted data in current sql will be read again.
     */
    @Test
    public void insertSelectSelfTest() throws SQLException {

        // initialize data
        String sql = String
            .format(
                "insert into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + " select varchar_test,pk+1000,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        // insert select from self. Using MERGE_UNION_SIZE hint to make sure there's only one connection to each group.
        sql = String.format(
            "insert into %s(varchar_test,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                + " select /*+TDDL: cmd_extra(MERGE_UNION_SIZE=0)*/"
                + "varchar_test,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s",
            baseTwoTableName, baseTwoTableName);

        int beforeCount = getCountOfTable(baseTwoTableName);

        int expectedCount = beforeCount;
        for (int i = 0; i < 6; i++) {
            JdbcUtil.executeSuccess(tddlConnection, sql);
            expectedCount *= 2;
        }

        int afterCount = getCountOfTable(baseTwoTableName);
        Assert.assertEquals(expectedCount, afterCount);
    }

    /**
     * INSERT SELECT 包含列子查询
     *
     * @since 5.1.25-SNAPSHOT
     */
    @Ignore
    @Test
    public void unionAllScalarColumnSubqueryTest() {

        tableDataPrepare(baseTwoTableName, 1, 20,
            columns, PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = String.format(
            "INSERT INTO %s (integer_test, pk, varchar_test) \n"
                + "SELECT 30, (select pk + 10000 from %s where pk = 3) pk, 'aa' FROM dual WHERE EXISTS (select * from %s where integer_test>15 OR integer_test<5) \n"
                + "UNION ALL ( SELECT 31, (select pk + 20000 from %s where pk = 3) pk, 'bb' FROM dual WHERE EXISTS (select * from %s where 8<pk AND pk<12) )\n",
            baseTwoTableName,
            baseTwoTableName,
            baseTwoTableName,
            baseOneTableName,
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String cnSql = "select pk, varchar_test from " + tableNameForCheck2;
        String dnSql = "select pk, varchar_test from " + baseTwoTableName;
        assertBroadcastTableSame(dnSql, cnSql);
        selectContentSameAssert(dnSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void insertSelectOnduplicatedUpdateTest() throws SQLException {

        // initialize data
        String sql = String
            .format(
                "/*+TDDL:cmd_extra(INSERT_SELECT_BATCH_SIZE=10)*/ insert into %s(varchar_test,pk,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                    + " select varchar_test,pk+1000,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test from %s ",
                baseTwoTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        // insert select from self. Using MERGE_UNION_SIZE hint to make sure there's only one connection to each group.
        sql = String.format(
            "insert into %s(varchar_test,blob_test,integer_test,float_test,date_test,datetime_test,timestamp_test) "
                + " select varchar_test,blob_test,integer_test,float_test,date_test,datetime_test,"
                + "timestamp_test from %s on duplicate key update datetime_test=now()",
            baseTwoTableName, baseTwoTableName);

        int beforeCount = getCountOfTable(baseTwoTableName);

        int expectedCount = beforeCount;
        for (int i = 0; i < 6; i++) {
            JdbcUtil.executeSuccess(tddlConnection, sql);
            expectedCount *= 2;
        }

        int afterCount = getCountOfTable(baseTwoTableName);
        Assert.assertEquals(expectedCount, afterCount);
    }

    /**
     * INSERT SELECT 包含列子查询, 开事务
     *
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void trxTest() throws Exception {

        Connection connection = null;
        try {
            connection = getPolardbxConnection();
            connection.setAutoCommit(false);

            tableDataPrepare(baseTwoTableName, 1, 20, columns, PK_COLUMN_NAME, mysqlConnection, connection,
                columnDataGenerator);

            String sql = String.format(
                "insert into %s(pk, varchar_test) select distinct(integer_test) + 1000 as idd ,varchar_test from %s where integer_test>15 or integer_test<5 union distinct "
                    + "(select pk + 300,varchar_test from %s where 8<pk  order by pk limit 10) order by varchar_test, idd limit 100",
                baseTwoTableName, baseOneTableName, baseTwoTableName);
            executeOnMysqlAndTddl(mysqlConnection, connection, sql, null);

            String cnSql = "select pk, varchar_test from " + tableNameForCheck2;
            String dnSql = "select pk, varchar_test from " + baseTwoTableName;
            assertBroadcastTableSame(dnSql, cnSql, mysqlConnection, connection, false);
            selectContentSameAssert(dnSql, null, mysqlConnection, connection);
        } finally {
            if (null != connection) {
                connection.setAutoCommit(true);
                connection.close();
            }
        }
    }

    public void assertBroadcastTableSame(String dnSql, String cnSql) {
        assertBroadcastTableSame(dnSql, cnSql, mysqlConnection, tddlConnection, false);
    }

    public void assertBroadcastTableSame(String dnSql, String cnSql, boolean allowEmptyResultSet) {
        assertBroadcastTableSame(dnSql, cnSql, mysqlConnection, tddlConnection, allowEmptyResultSet);
    }

    /**
     * 临时方法
     */
    public void assertBroadcastTableSame(String dnSql, String cnSql, Connection dnConnection, Connection cnConnection,
                                         boolean allowEmptyResultSet) {
        // 判断下广播表是否能够广播插入到每张表
        if (baseTwoTableName.contains("broadcast")) {
            for (int i = 0; i < 2; i++) {
                String hint = String.format("/*TDDL:node=%s*/", i);
                selectContentSameAssert(dnSql, hint + cnSql, null,
                    dnConnection, cnConnection, allowEmptyResultSet);
            }
        }
    }

    /**
     * Get the row count of the table
     */
    private int getCountOfTable(String tableName) throws SQLException {
        String sql = "select count(1) from " + tableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        return rs.getInt(1);
    }
}
