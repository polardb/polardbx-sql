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
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.math.Exp;
import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Delete from gsi table.
 *
 * @author minggong
 */

@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public class DeleteGsiTest extends GsiDMLTest {

    private static Map<String, String> tddlTables = new HashMap<>();
    private static Map<String, String> shadowTables = new HashMap<>();
    private static Map<String, String> mysqlTables = new HashMap<>();

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

    @Rule
    public final TestName name = new TestName();

    public DeleteGsiTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    @Before
    public void initData() throws Exception {
        this.clearDataWithDelete = !TStringUtil.startsWith(name.getMethodName(), "deleteTestConcurrency");
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

    private void prepareDataForTable(String tableName) {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + tableName
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
     * delete one row
     */
    @Test
    public void deleteOneRowTest() throws Exception {
        String sql = hint + "delete from " + baseOneTableName + " where pk=?";

        List<Object> param = new ArrayList<Object>();
        param.add(10);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * delete some data
     */
    @Test
    public void deleteSomeTest() throws Exception {
        String sql = hint + "delete from " + baseOneTableName + " where pk>?";

        List<Object> param = new ArrayList<Object>();
        param.add(10);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * delete no data
     */
    @Test
    public void deleteNoDataTest() throws Exception {
        String sql = hint + "delete from " + baseOneTableName + " where pk>?";

        List<Object> param = new ArrayList<Object>();
        param.add(1000);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * smallint_test between ? and ?
     */
    @Test
    public void deleteByOtherConditionTest() throws Exception {
        String sql = hint + "delete from " + baseOneTableName + " where smallint_test between ? and ?";

        List<Object> param = new ArrayList<Object>();
        param.add(10);
        param.add(15);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * base sharding key = null
     */
    @Test
    public void deleteBaseShardingKeyNullTest() throws Exception {
        if (!baseOneTableName.endsWith("no_unique_one_index_base")) {
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(null);
        param.add(null);
        param.add("test100");
        param.add(columnDataGenerator.datetime_testValue);
        param.add(2000);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "delete from " + baseOneTableName + " where integer_test=null";
        param = new ArrayList<>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * index sharding key = null
     */
    @Test
    public void deleteIndexShardingKeyNullTest() throws Exception {
        if (!baseOneTableName.endsWith("no_unique_one_index_base")) {
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(null);
        param.add(null);
        param.add("test100");
        param.add(columnDataGenerator.datetime_testValue);
        param.add(2000);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "delete from " + baseOneTableName + " where bigint_test=null";
        param = new ArrayList<>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * delete join
     */
    @Test
    public void deleteMultipleTablesTest() throws Exception {
        prepareDataForTable(baseTwoTableName);
        String sql = String.format(hint + "delete %s from %s inner join %s where %s.pk=%s.pk",
            baseOneTableName,
            baseOneTableName,
            baseTwoTableName,
            baseOneTableName,
            baseTwoTableName);

        List<Object> param = new ArrayList<Object>();

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * delete join
     */
    @Test
    public void deleteMultipleTablesByMultiTest() throws Exception {
        prepareDataForTable(baseTwoTableName);
        String tHint = " /*+TDDL:CMD_EXTRA(UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ ";
        String sql = String.format(hint + tHint + "delete %s from %s inner join %s where %s.pk=%s.pk",
            baseOneTableName,
            baseOneTableName,
            baseTwoTableName,
            baseOneTableName,
            baseTwoTableName);

        List<Object> param = new ArrayList<Object>();

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * multi threads
     */
    @Test
    public void deleteTestConcurrency() throws Exception {

        final String updateSql = hint + "delete from " + baseOneTableName + " where pk=?";

        final List<AssertionError> errors = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Runnable() {

                public void run() {
                    Connection connection = null;
                    try {
                        connection = getPolardbxDirectConnection();

                        for (int j = 0; j < 50; j++) {
                            List<Object> param = new ArrayList<Object>();
                            param.add(j);
                            JdbcUtil.updateData(connection, updateSql, param);
                        }
                    } catch (AssertionError ae) {
                        errors.add(ae);
                    } finally {
                        JdbcUtil.close(connection);
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }

        if (!errors.isEmpty()) {
            throw errors.get(0);
        }

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    // test force gsi , use table gsi_dml_global_unique_one_index_base
    @Test
    public void deleteTestWithForceGSI() throws Exception {
        if (!baseOneTableName.endsWith("gsi_dml_global_unique_one_index_base")) {
            return;
        }
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(null);
        param.add(null);
        param.add("test100");
        param.add(columnDataGenerator.datetime_testValue);
        param.add(2000);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        String tddlSql = hint + "delete from " + baseOneTableName + " force index(" + baseOneTableName
            + ".gsi_dml_global_unique_one_index_index1) where integer_test=null";
        String mysqlSql = hint + "delete from " + baseOneTableName + " where integer_test=null";
        param = new ArrayList<>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlSql, tddlSql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }
}
