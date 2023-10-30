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

package com.alibaba.polardbx.qatest.dml.sharding.gsi;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * @author chenmo.cm
 */



public class UpdateGsiShardingKeyTest extends GsiDMLTest {

    private static Map<String, String> tddlTables = new HashMap<>();
    private static Map<String, String> shadowTables = new HashMap<>();
    private static Map<String, String> mysqlTables = new HashMap<>();

    //多线程执行
    private static final String HINT1 = " /*+TDDL:CMD_EXTRA(UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ ";

    @BeforeClass
    public static void beforeCreateTables() {
        try {
            concurrentCreateNewTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void afterDropTables() {

        try {
            concurrentDropTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Parameterized.Parameters(name = "{index}:hint={0} table1={1} table2={2}")
    public static List<String[]> prepareData() {
        List<String[]> rets = doPrepareData();
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    public UpdateGsiShardingKeyTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    @Before
    public void initData() throws Exception {
        super.initData();

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,smallint_test)"
            + " values (?,?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 20; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);
            param.add(i);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOne() throws Exception {

        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = 24 , varchar_test = concat(varchar_test, 'sk modified') where integer_test = 4";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = hint + "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOne2() throws Exception {

        String sql = hint + "UPDATE " + baseOneTableName + " SET bigint_test = 4 where bigint_test = 4";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = hint + "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateAll() throws Exception {

        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = bigint_test + 100 , varchar_test = concat(varchar_test, 'sk modified')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = hint + "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateAll2() throws Exception {

        String sql = hint + "UPDATE " + baseOneTableName + " SET datetime_test='2010-02-22'";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = hint + "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome() throws Exception {
        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE integer_test BETWEEN 3 AND 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test BETWEEN 3 AND 7";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome1() throws Exception {
        // String sql = hint + "SELECT * FROM " + baseOneTableName + " WHERE pk > 7";
        // selectContentSameAssert(sql, null, mysqlConnection,
        // polarDbXConnection);

        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE integer_test > 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test > 7";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome2() throws Exception {
        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE integer_test < 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test < 7 order by pk";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome3() throws Exception {

        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = BIGINT_TEST + 2000, VARCHAR_TEST = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE integer_test >= 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test >= 7";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome4() throws Exception {
        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), float_test= ? , double_test= ? WHERE  integer_test <=7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test <= 7";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome5() throws Exception {
        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE integer_test = 7 order by pk limit 1";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test = 7 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void nowTest() throws Exception {
        String sql = hint + "update  /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), date_test= now(),datetime_test=now()  "
            + ",timestamp_test=now()  where integer_test=1";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName + " where integer_test = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void whereWithComplexTest() throws Exception {
        String sql = hint + "update /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " set bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), float_test= ? where varchar_test= ? or  ((?-integer_test>100)||(?<integer_test && ?-integer_test >200))";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(400);
        param.add(200);
        param.add(800);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void setWithIncrementTest() throws Exception {
        String sql = hint + "update /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  " + baseOneTableName
            + " set bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), float_test= float_test+ ? where integer_test =? and varchar_test =?";
        List<Object> param = new ArrayList<Object>();
        param.add(2);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistDataTest() throws Exception {
        long pk = -1l;
        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/" + baseOneTableName
            + "  SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), integer_test=? WHERE integer_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(pk);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistFielddTest() throws Exception {
        String sql = hint + "UPDATE " + baseOneTableName
            + "  SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), nothisfield =1";
        executeErrorAssert(tddlConnection, sql, null, "Unknown target column 'nothisfield'");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistTableTest() throws Exception {
        String sql = hint + "UPDATE nor SET bigint_test = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(1);

        executeErrorAssert(tddlConnection, sql, param, "doesn't exist");
    }

    /**
     * @since 5.0.1
     */
    public void updateNotMatchTypeTest() throws Exception {
        String sql = hint + "UPDATE " + baseOneTableName
            + "  SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), integer_test=? WHERE integer_test=?";
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
        String sql = hint + "UPDATE " + baseOneTableName + "  SET bigint_test = bigint_test + 2000";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateMultiColumnIncludeShardingKey() {
        String sql = hint + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ " + baseOneTableName
            + "  SET integer_test = integer_test + 100, bigint_test = bigint_test + 2000";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * update 永假式处理
     */
    @Test
    public void updateWihtFalseCond() throws Exception {
        String sql = String.format(
            hint
                + "update  /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ %s  SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), integer_test =1 where 1=2",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);

    }

    @Test
    public void updateWithSetTwice() throws Exception {
        String sql = String.format(
            hint
                + "update  /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  %s  SET bigint_test=50, integer_test = bigint_test, integer_test = integer_test + 2000, bigint_test=bigint_test+2000, pk = pk + 100 where integer_test=?",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(18);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format(
            hint
                + "update  /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  %s  SET bigint_test=1, integer_test = bigint_test, integer_test = integer_test + 2000, bigint_test=bigint_test+2000, float_test=?, "
                + "float_test=?+1, pk = pk + 100  where integer_test=?",
            baseOneTableName);
        param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysFalse1() throws Exception {
        String sql = String.format(hint + "UPDATE  %s  SET bigint_test=? WHERE integer_test < 0 AND integer_test > 0 ",
            baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysFalse2() throws Exception {
        String sql = String.format(
            hint + "UPDATE  %s  SET bigint_test=? WHERE (integer_test IN (1000, 2000, 3000)) IS TRUE",
            baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysTrue1() throws Exception {
        String sql = String.format(
            hint + "UPDATE  %s  SET bigint_test=? + bigint_test + 100 WHERE NOT (pk < 0 AND pk > 0) ",
            baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysTRUE2() throws Exception {
        String sql = String.format(
            hint
                + "UPDATE  %s  SET bigint_test=? + 100 + bigint_test WHERE (integer_test IN (1000,2000,3000)) IS FALSE",
            baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     *
     */
    @Test
    public void updateWithTableName() throws Exception {

        String sql = hint
            + "UPDATE %s SET %s.bigint_test = 2000 + %s.bigint_test, %s.float_test=?, %s.double_test=? WHERE  integer_test BETWEEN 3 AND 7";
        sql = String
            .format(sql, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test BETWEEN 3 AND 7";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     *
     */
    @Test
    public void updateWithWrongTableName() throws Exception {

        String sql = hint
            + "UPDATE %s SET %s.bigint_test = 100 + %s.bigint_test, %s.float_test=0, %s.double_test=0 WHERE  integer_test BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName, "wrongName");

        executeErrorAssert(tddlConnection, sql, null, "not found");
    }

    /**
     *
     */
    @Test
    public void updateWithSubquery() throws Exception {

        String sql = hint
            + "UPDATE (select * from %s) t SET %s.float_test=0, %s.double_test=0 WHERE  integer_test BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName);

        executeErrorAssert(tddlConnection, sql, null, "The target table 't' of the UPDATE is not updatable");

        sql = hint
            + "UPDATE  /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  %s a,(select * from %s) t SET a.bigint_test = a.bigint_test + 2000, a.varchar_test = concat(a.varchar_test, 'b'), a.float_test=0, a.double_test=0 WHERE  a.integer_test BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void updateWithSubquery2() throws Exception {
        String sql = hint
            + "UPDATE  /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/  (select * from %s) t, %s a,(select * from %s) t2 SET a.bigint_test = a.bigint_test + 2000, a.varchar_test = concat(a.varchar_test, 'b'), a.float_test=0, a.double_test=0 WHERE  a.integer_test BETWEEN 3 AND 7 and t2.integer_test BETWEEN 13 AND 17";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void updateWithReferCondition() {
        String sql = String.format(
            hint
                + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ %s SET %s.bigint_test = %s.bigint_test + 100, %s.integer_test = 1 WHERE %s.integer_test = 9 AND %s.varchar_test = '1'",
            baseOneTableName,
            baseOneTableName,
            baseOneTableName,
            baseOneTableName,
            baseOneTableName,
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = hint + "SELECT * FROM " + baseOneTableName + " WHERE integer_test = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void updateAliasWithReferCondition() {
        String sql = String.format(
            hint
                + "UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ %s tb SET tb.bigint_test = 111, tb.integer_test = 1 WHERE tb.integer_test = 11 AND tb.varchar_test = '1'",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = hint + "SELECT * FROM " + baseOneTableName + " WHERE integer_test = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void updatePhySqlId() throws Exception {
        if (TStringUtil.isNotBlank(hint)) {
            return;
        }

        String sql =
            "UPDATE %s SET integer_test = 2000 + integer_test, float_test=?, double_test=? WHERE  integer_test BETWEEN 3 AND 7";
        sql = String
            .format(sql, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        JdbcUtil.executeUpdate(tddlConnection, "set polardbx_server_id = 4927");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, "trace " + sql, param, true);

        final List<List<String>> trace = getTrace(tddlConnection);

        checkPhySqlId(trace);

        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test BETWEEN 2003 AND 2007";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSomeByMulti() throws Exception {

        String sql = hint + HINT1 + " /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ " + "UPDATE " + baseOneTableName
            + " SET bigint_test = BIGINT_TEST + 2000, VARCHAR_TEST = concat(varchar_test, 'b'), float_test=?,double_test=? WHERE integer_test >= 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "SELECT * FROM " + baseOneTableName + "  WHERE integer_test >= 7";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateAllByMulti() throws Exception {

        String sql = hint + HINT1 + " /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ " + " UPDATE " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000 , varchar_test = concat(varchar_test, 'sk modified')";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of(), true);

        sql = hint + "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void nowUpdateByMultiTest() throws Exception {
        String sql = hint + HINT1 + " /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ " + " UPDATE " + baseOneTableName
            + " SET bigint_test = bigint_test + 2000, varchar_test = concat(varchar_test, 'b'), date_test= now(),datetime_test=now()  "
            + ",timestamp_test=now()";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = hint + "SELECT * FROM  " + baseOneTableName + " where integer_test = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

}
