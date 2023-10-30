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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepareWithBatch;

/**
 * @author chenmo.cm
 */


public class UpdateShardingKeyTest extends CrudBasedLockTestCase {

    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();
    private static final String HINT1 = "/*+TDDL:cmd_extra(ENABLE_MODIFY_SHARDING_COLUMN=true)*/";
    //多线程执行
    private static final String HINT2 = "/*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=true,UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ ";

    private final String HINT;

    @Parameters(name = "{index}:hint={0},table1={1}")
    public static List<String[]> prepareData() {
        final List<String[]> result = new ArrayList<>();
        result.addAll(
            Arrays.asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable2(ExecuteTableName.UPDATE_DELETE_BASE)));
        result.addAll(Arrays
            .asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE_AUTONIC)));

        final List<String[]> allTests = new ArrayList<>();
        result.forEach(strings -> allTests.add(new String[] {HINT1, strings[0]}));

        //单表，广播表不走多线程模式，过滤掉
        result.stream().filter(s -> !(s[0].contains("one_db_one_tb") || s[0].contains("broadcast"))).forEach(strings -> {
            allTests.add(new String[] {HINT2, strings[0]});
        });

        return allTests;
    }

    public UpdateShardingKeyTest(String tHint, String baseOneTableName) {
        HINT = tHint;
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepareWithBatch(baseOneTableName,
            1,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection,
            columnDataGenerator);

        String sql = "SELECT count(*) FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOne() throws Exception {

        String sql = HINT + "UPDATE " + baseOneTableName + " SET pk = 24 , varchar_test='sk modified' where pk = 4";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOne2() throws Exception {

        String sql = HINT + "UPDATE " + baseOneTableName + " SET pk = 4 where pk = 4";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateAll() throws Exception {

        String sql = HINT + "UPDATE " + baseOneTableName + " SET pk = pk + 100 , varchar_test='sk modified'";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateAll2() throws Exception {

        String sql = HINT + "UPDATE " + baseOneTableName + " SET date_test='2010-02-22'";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome() throws Exception {
        String sql = HINT + "UPDATE " + baseOneTableName
            + " SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE pk "
            + "BETWEEN 3 AND 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk BETWEEN 103 AND 107";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk BETWEEN 103 AND 107";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome1() throws Exception {
        // String sql = "SELECT * FROM " + baseOneTableName + " WHERE pk > 7";
        // selectContentSameAssert(sql, null, mysqlConnection,
        // polarDbXConnection);

        String sql = HINT + "UPDATE " + baseOneTableName
            + " SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE pk > 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk > 107";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk > 107";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome2() throws Exception {
        String sql = HINT + "UPDATE " + baseOneTableName
            + " SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE pk < 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk > 100 and pk < 107 order by pk";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql =
            "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk > 100 and pk < 107 order by pk";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome3() throws Exception {

        String sql = HINT + "UPDATE " + baseOneTableName
            + " SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE pk >= 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk >= 107";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk >= 107";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome4() throws Exception {
        String sql = HINT + "UPDATE " + baseOneTableName
            + " SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test= ? , double_test= ? WHERE  pk"
            + " <=7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk <= 107 and pk > 100";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk <= 107 and pk > 100";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome5() throws Exception {
        String sql = HINT + "UPDATE " + baseOneTableName
            + " SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE pk = 7 "
            + "order by pk limit 1";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk = 107 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk = 107 order by pk limit 1";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void nowTest() throws Exception {
        String sql = HINT + "update " + baseOneTableName
            + " SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), date_test= now(),datetime_test=now()  "
            + ",timestamp_test=now()  where pk=1";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName + " where pk = 101";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName) + " where pk = 101";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void whereWithComplexTest() throws Exception {
        String sql = HINT + "update " + baseOneTableName
            + " set pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test= ? where varchar_test= ? or  "
            + "((?-integer_test>100)||(?<integer_test && ?-integer_test >200))";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(400);
        param.add(200);
        param.add(800);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void setWithIncrementTest() throws Exception {
        String sql = HINT + "update " + baseOneTableName
            + " set pk = pk + 100, varchar_test = concat(varchar_test, 'b'), float_test= float_test+ ? where "
            + "integer_test =? and varchar_test =?";
        List<Object> param = new ArrayList<Object>();
        param.add(2);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistDataTest() throws Exception {
        long pk = -1l;
        String sql = HINT + "UPDATE " + baseOneTableName
            + "  SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), integer_test=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(pk);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistFielddTest() throws Exception {
        String sql = HINT + "UPDATE " + baseOneTableName
            + "  SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), nothisfield =1";
        executeErrorAssert(tddlConnection, sql, null, "Unknown target column 'nothisfield'");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistTableTest() throws Exception {
        String sql = HINT + "UPDATE nor SET pk = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(1);
        executeErrorAssert(tddlConnection, sql, param, "doesn't exist");
    }

    /**
     * @since 5.0.1
     */
    public void updateNotMatchTypeTest() throws Exception {
        String sql = HINT + "UPDATE " + baseOneTableName
            + "  SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), integer_test=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add("NIHAO");
        param.add(1l);
        executeErrorAssert(tddlConnection, sql, param, "NOTHISFIELD is not existed");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateShardingKey() {
        String sql = HINT + "UPDATE " + baseOneTableName + "  SET pk = pk + 100";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateMultiColumnIncludeShardingKey() {
        String sql = HINT + "UPDATE " + baseOneTableName + "  SET integer_test = integer_test + 1, pk = pk + 100";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * update 永假式处理
     */
    @Test
    public void updateWihtFalseCond() throws Exception {
        String sql = String.format(
            HINT + "update  %s  SET pk = pk + 100, varchar_test = concat(varchar_test, 'b'), integer_test =1 where 1=2",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);

    }

    @Test
    public void updateWithSetTwice() throws Exception {
        String sql = String.format(HINT + "update  %s  SET pk=50, pk=pk+100 where integer_test=?", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(18);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String
            .format(HINT + "update  %s  SET pk=1, pk=pk+200, float_test=?, " + "float_test=?+1 where integer_test=?",
                baseOneTableName);
        param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void updateWithSetTwice2() throws Exception {
        String sql = String.format(HINT
                + "update  %s  SET pk=50, integer_test = pk, integer_test = integer_test + 100, pk=pk+100 where "
                + "integer_test=?",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(18);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format(HINT
                + "update  %s  SET pk=1, integer_test = pk, integer_test = integer_test + 100, pk=pk+200, "
                + "float_test=?, "
                + "float_test=?+1 where integer_test=?",
            baseOneTableName);
        param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysFalse1() throws Exception {
        String sql = String.format(HINT + "UPDATE  %s  SET pk=? WHERE pk < 0 AND pk > 0 ", baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysFalse2() throws Exception {
        String sql = String.format(HINT + "UPDATE  %s  SET pk=? WHERE (pk IN (100, 200, 300)) IS TRUE",
            baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysTrue1() throws Exception {
        String sql = String.format(HINT + "UPDATE  %s  SET pk=? + pk + 100 WHERE NOT (pk < 0 AND pk > 0) ",
            baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysTRUE2() throws Exception {
        String sql = String.format(HINT + "UPDATE  %s  SET pk=? + 100 + pk WHERE (pk IN (1,2,3)) IS FALSE",
            baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     *
     */
    @Test
    public void updateWithTableName() throws Exception {

        String sql = HINT
            + "UPDATE %s SET %s.pk = 100 + %s.pk, %s.float_test=?, %s.double_test=? WHERE  pk BETWEEN 3 AND 7";
        sql = String
            .format(sql, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk BETWEEN 103 AND 107";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk BETWEEN 103 AND 107";
        assertBroadcastTableSame(sql, tddlSql);
    }

    /**
     *
     */
    @Test
    public void updateWithWrongTableName() throws Exception {

        String sql = HINT
            + "UPDATE %s SET %s.pk = 100 + %s.pk, %s.float_test=0, %s.double_test=0 WHERE  pk BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName, "wrongName");

        executeErrorAssert(tddlConnection, sql, null, "not found");
    }

    /**
     *
     */
    @Test
    public void updateWithSubquery() throws Exception {
        if (HINT.equals(HINT2)) {
            //分割多次执行会Duplicate key
            return;
        }

        String sql = HINT
            + "UPDATE (select * from %s) t SET %s.float_test=0, %s.double_test=0 WHERE  pk BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName);

        executeErrorAssert(tddlConnection, sql, null, "The target table 't' of the UPDATE is not updatable");

        sql = HINT
            + "UPDATE %s a,(select * from %s) t SET a.pk = a.pk + 100, a.varchar_test = concat(a.varchar_test, 'b'), "
            + "a.float_test=0, a.double_test=0 WHERE  a.pk BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void updateWithSubquery2() throws Exception {
        if (HINT.equals(HINT2)) {
            //分割多次执行会Duplicate key
            return;
        }

        String sql = HINT
            + "UPDATE (select * from %s) t, %s a,(select * from %s) t2 SET a.pk = a.pk + 100, a.varchar_test = concat"
            + "(a.varchar_test, 'b'), a.float_test=0, a.double_test=0 WHERE  a.pk BETWEEN 3 AND 7 and t2.pk BETWEEN "
            + "13 AND 17";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void updateWithReferCondition() {
        String sql = String.format(
            HINT + "UPDATE %s SET %s.pk = %s.pk + 100, %s.integer_test = 1 WHERE %s.pk = 9 AND %s.varchar_test = '1'",
            baseOneTableName,
            baseOneTableName,
            baseOneTableName,
            baseOneTableName,
            baseOneTableName,
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName + " WHERE pk = 109";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void updateAliasWithReferCondition() {
        String sql = String.format(
            HINT + "UPDATE %s tb SET tb.pk = 111, tb.integer_test = 1 WHERE tb.pk = 11 AND tb.varchar_test = '1'",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName + " WHERE pk = 111";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    public void assertBroadcastTableSame(String sql, String tddlsql) {
        // not support route hint now.
        // 判断下广播表是否能够广播更新到每张表
        if (baseOneTableName.contains("broadcast")) {
            for (int i = 0; i < 2; i++) {
                String hint = String.format("/*TDDL:node=%s*/", i);
                selectContentSameAssertWithDiffSql(
                    hint + tddlsql,
                    sql,
                    null,
                    mysqlConnection,
                    tddlConnection,
                    true,
                    false,
                    true
                );
            }
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void updateShardingKeyEqualNullTest() throws Exception {
        String sql = String
            .format("explain sharding update " + HINT
                + " update_delete_base_two_multi_db_multi_tb set pk = 1, integer_test = 1 where pk = null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void updateShardingKeyIsNullTest() throws Exception {
        String sql = String
            .format("explain sharding update " + HINT
                + " update_delete_base_two_multi_db_multi_tb set pk = 1, integer_test = 1 where pk is null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void updateTwoShardingKeyIsNullTest() throws Exception {
        String sql = String
            .format("explain sharding update " + HINT
                + " update_delete_base_two_multi_db_multi_tb t2, update_delete_base_three_multi_db_multi_tb t3 "
                + " set t2.pk = 1, t2.integer_test = 1 where t2.pk = t3.pk and t2.pk is null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void updateTwoShardingKeyEqualNullTest() throws Exception {
        String sql = String
            .format("explain sharding update " + HINT
                + " update_delete_base_two_multi_db_multi_tb t2, update_delete_base_three_multi_db_multi_tb t3 "
                + " set t2.pk = 1, t2.integer_test = 1 where t2.pk = t3.pk and t2.pk = null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.next();
            Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void updateWithSameValueTest() throws Exception {

        String sql = HINT + "UPDATE " + baseOneTableName
            + " SET varchar_test='sk modified',  pk = 4, integer_test = 10, timestamp_test = '2020-11-24 10:41:54' "
            + "where pk = 4";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);

        assertBroadcastTableSame(sql, tddlSql);

    }

    private String toPhyTableName(String logicalTableName) {
        ResultSet resultSet =
            JdbcUtil.executeQuerySuccess(tddlConnection, "show topology from " + logicalTableName);
        String physicalTableName = logicalTableName;
        try {
            resultSet.next();
            physicalTableName = (String) JdbcUtil.getObject(resultSet, 3);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return physicalTableName;
    }
}
