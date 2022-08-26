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
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * Insert into gsi table with sequence.
 *
 * @author minggong
 */

public class GsiWithSequenceTest extends GsiDMLTest {

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
        List<String[]> rets = Arrays.asList(ExecuteTableName.gsiDMLSequenceTable());
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    public GsiWithSequenceTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    @Before
    public void initData() throws Exception {
        super.initData();
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseTwoTableName
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
     * insert all fields except pk
     */
    @Test
    public void insertSequenceAllField() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        // remove pk
        columns = columns.subList(1, columns.size());
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName + " (";
        String values = " values ( ";

        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName();
            sql = sql + columnName + ",";
            values = values + " ?,";
        }

        sql = sql.substring(0, sql.length() - 1) + ") ";
        values = values.substring(0, values.length() - 1) + ")";

        sql = sql + values;
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, 1);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test from "
            + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        assertLastInsertId();

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert batch
     */
    @Test
    public void insertSequenceBatch() throws Exception {
        if (TStringUtil.isNotEmpty(hint) && TStringUtil.contains(PropertiesUtil.getConnectionProperties(),
            "useServerPrepStmts=true")) {
            // JDBC will rewrite batch insert to multi statement, if hint exists
            // skip sequence test for multi statement;
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertLastInsertId();

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * pk=null
     */
    @Test
    public void insertSequenceNull() throws Exception {
        if (TStringUtil.isNotEmpty(hint) && TStringUtil.contains(PropertiesUtil.getConnectionProperties(),
            "useServerPrepStmts=true")) {
            // JDBC will rewrite batch insert to multi statement, if hint exists
            // skip sequence test for multi statement;
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(null);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertLastInsertId();

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * pk=0
     */
    @Test
    public void insertSequenceZero() throws Exception {
        if (TStringUtil.isNotEmpty(hint) && TStringUtil.contains(PropertiesUtil.getConnectionProperties(),
            "useServerPrepStmts=true")) {
            // JDBC will rewrite batch insert to multi statement, if hint exists
            // skip sequence test for multi statement;
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(0);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertLastInsertId();

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * pk=1,2,3,...
     */
    @Test
    public void insertSequenceExplicitValue() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 1; i <= 10; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * integer_test=nextval
     */
    @Test
    public void insertNextValue() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (AUTO_SEQ_" + baseOneTableName + ".NEXTVAL,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);

            params.add(param);
        }

        JdbcUtil.updateDataBatch(tddlConnection, sql, params);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert select without pk
     */
    @Test
    public void insertSelectSequenceTest() throws Exception {
        String sql = String.format(
            (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into %s"
                + "(integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test)"
                + " select integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test"
                + " from %s", baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertLastInsertId();

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert select with pk
     */
    @Test
    public void insertSelectNoSequenceTest() throws Exception {
        String sql = String.format(
            (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into %s"
                + "(pk,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test)"
                + " select pk+1000,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test"
                + " from %s",
            baseOneTableName,
            baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    private void assertLastInsertId() {
        String sql = hint + "select min(pk) from " + baseOneTableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Object expectedLastInsertId = JdbcUtil.getAllResult(rs).get(0).get(0);

        sql = hint + "select last_insert_id()";
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Object actualLastInsertId = JdbcUtil.getAllResult(rs).get(0).get(0);

        Assert.assertEquals("select last_insert_id() result not as expected", expectedLastInsertId, actualLastInsertId);
    }
}
