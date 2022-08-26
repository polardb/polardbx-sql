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
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * Insert into gsi table.
 *
 * @author minggong
 */

public class InsertGsiTest extends GsiDMLTest {

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

    public InsertGsiTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    /**
     * all fields
     */
    @Test
    public void insertAllFieldTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
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

        sql = hint + "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * (?,?,?,?),(?,?,?,?)
     */
    @Test
    public void insertMultiValuesTest() throws Exception {
        List<Object> param = new ArrayList<Object>();

        StringBuilder sql = new StringBuilder(
            (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into ").append(
                baseOneTableName)
            .append(" (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)")
            .append(" values ");

        for (int i = 0; i < 100; i++) {
            sql.append("(?,?,?,?,?,?,?)");
            if (i != 99) {
                sql.append(",");
            }

            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);
        }
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql.toString(), param, true);

        String selectSql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection, false, true);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert ...; insert ...;
     */
    @Test
    public void insertMultiStatementsTest() throws Exception {
        List<Object> param = new ArrayList<Object>();

        String sqlTemplate = hint + "insert into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test) values(?,?,?,?,?,?,?);";

        StringBuilder sql = new StringBuilder();

        for (int i = 0; i < 100; i++) {
            sql.append(sqlTemplate);

            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);
        }
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql.toString(), param, true);

        String selectSql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection, false, true);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert ignore, twice
     */
    @Test
    public void insertIgnoreTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint : "") + "insert ignore into "
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
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * date_test,timestamp_test,datetime_test = "yyyy-MM-dd HH:mm:ss"
     */
    @Test
    public void insertGmtStringTest() throws Exception {
        Date gmt = new Date(1350304585000l);
        Date gmtDay = new Date(1350230400000l);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String gmtString = df.format(gmt);
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,date_test,timestamp_test,datetime_test,integer_test,bigint_test,varchar_test,year_test,char_test)"
            + " values(?,?,?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(df.format(gmtDay));
        param.add(gmtString);
        param.add(gmtString);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * set pk=?,integer_test=?,bigint_test=?,varchar_test=?,datetime_test=?, year_test=?,char_test=?
     */
    @Test
    public void insertWithSetTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " set pk=?,integer_test=?,bigint_test=?,varchar_test=?,datetime_test=?,year_test=?,char_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * PK,INTEGER_TEST,BIGINT_TEST,VARCHAR_TEST,DATETIME_TEST,YEAR_TEST, CHAR_TEST
     */
    @Test
    public void insertPramUppercaseTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (PK,INTEGER_TEST,BIGINT_TEST,VARCHAR_TEST,DATETIME_TEST,YEAR_TEST,CHAR_TEST)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * time_test=null,float_test=null
     */
    @Test
    public void insertWithNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,time_test,float_test)"
            + " values (?,?,?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        param.add(null);
        param.add(null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * sharding key = null
     */
    @Test
    public void insertShardingKeyNullTest() throws Exception {
        if (!baseOneTableName.endsWith("no_unique_one_index_base")) {
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * sharding key = null
     */
    @Test
    public void insertIgnoreShardingKeyNullTest() throws Exception {
        if (!baseOneTableName.endsWith("no_unique_one_index_base")) {
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint : "") + " insert ignore into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * sharding key = null
     */
    @Test
    public void upsertShardingKeyNullTest() throws Exception {
        if (!baseOneTableName.endsWith("no_unique_one_index_base")) {
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?) on duplicate key update float_test=1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * sharding key = null
     */
    @Test
    public void replaceShardingKeyNullTest() throws Exception {
        if (!baseOneTableName.endsWith("no_unique_one_index_base")) {
            return;
        }

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "replace " : "replace " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,float_test)"
            + " values (?,?,?,?,?,?,?,?)";
        param.add(1);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * pk = Long.MAX_VALUE, id = Integer.MAX_VALUE
     */
    @Test
    public void insertWithMaxMinTest() throws Exception {
        long pk = Long.MAX_VALUE;
        int id = Integer.MAX_VALUE;
        BigInteger bi = new BigInteger(RandomStringUtils.randomNumeric(18), 10);
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        param.add(bi);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        String selectSql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);

        String deleteSql = hint + "delete from " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteSql, new ArrayList<>(), true);

        pk = Long.MIN_VALUE;
        id = Integer.MIN_VALUE;
        param.set(0, pk);
        param.set(1, id);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);
        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * Long batch, splitting batch is expected
     */
    @Test
    public void insertBatchSplitTest() throws Exception {
        if (TStringUtil.isNotEmpty(hint) && TStringUtil.contains(PropertiesUtil.getConnectionProperties(),
            "useServerPrepStmts=true")) {
            // JDBC will rewrite batch insert to multi statement, if hint exists, and this will cause mysql crash
            // skip sequence test for multi statement;
            return;
        }
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint : "") + "insert into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 20000; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000);
            param.add("test" + i);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        assertIndexCountSame(baseOneTableName);
    }

    /**
     * executeBatch
     */
    @Test
    public void insertBatchTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
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
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false, true);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * ignore + executeBatch
     */
    @Test
    public void insertBatchIgnoreTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint : "") + "insert ignore into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add("test" + i);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * values(?,now(),?,concat('test',?),?+1,?,?)
     */
    @Test
    public void insertBatchSomeFieldTestWithFunction() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,datetime_test,integer_test,varchar_test,bigint_test,year_test,char_test)"
            + " values(?,now(),?,concat('test',?),?+1,?,?)";
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            String pk = RandomStringUtils.randomNumeric(8);
            param.add(i);
            param.add(Long.valueOf(pk));
            param.add(String.valueOf(i));
            // param.add(RandomStringUtils.randomNumeric(10));
            param.add(100 * i);
            param.add(columnDataGenerator.year_testValue);
            param.add(columnDataGenerator.char_testValue);
            params.add(param);
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select pk,datetime_test,integer_test,varchar_test,bigint_test,year_test,char_test from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * on duplicate key update float_test=1, double_test=1
     */
    @Test
    public void insertOnDuplicateKeyUpdateTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?) on duplicate key update float_test=1, double_test=1";

        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * on duplicate key update float_test=1, double_test=1, datetime_test=values(datetime_test)
     */
    @Test
    public void insertOnDuplicateKeyUpdateTimeFunctionTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,timestamp_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,now(),?,?,?) on duplicate key update float_test=1, double_test=1,"
            + " timestamp_test=values(timestamp_test)";

        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * float_test=values(float_test)+1
     */
    @Test
    public void insertOnDuplicateKeyUpdateValuesTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,float_test)"
            + " values (?,?,?,?,?,?,?,?) on duplicate key update float_test=values(float_test)+1";

        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);
        param.add(columnDataGenerator.float_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * ignore ... on duplicate key update float_test=1, double_test=1. Result should be the same with that without
     * IGNORE.
     */
    @Ignore
    public void insertIgnoreOnDuplicateKeyUpdateTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint : "") + "insert ignore into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?) on duplicate key update float_test=1, double_test=1";

        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert (pk1), (pk1) on duplicate key update
     */
    @Test
    public void UpsertBatchSameKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?), (?,?,?,?,?,?,?) on duplicate key update float_test=1";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        List<Object> params = new ArrayList<>();
        params.addAll(param);
        params.addAll(param); // add one more time

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * replace tb(guk) value(null)
     */
    @Test
    public void replaceGlobalUniqueKeyNullTest() throws Exception {
        String sql = hint + "replace into " + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(100);
        param.add(100);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert ignore (pk1), (pk1)
     */
    @Test
    public void insertBatchIgnoreSameKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " ignore into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?), (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        List<Object> params = new ArrayList<>();
        params.addAll(param);
        params.addAll(param); // add one more time

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert tb(guk) value(null) on duplicate key update
     */
    @Test
    public void UpsertGlobalUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?) on duplicate key update float_test=1";

        List<Object> param = new ArrayList<>();
        param.add(100);
        param.add(100);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert tb(uk) value(null)
     */
    @Test
    public void insertUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * replace (pk1), (pk1)
     */
    @Test
    public void ReplaceBatchSameKeyTest() throws Exception {
        String sql = hint + "replace into " + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?), (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        List<Object> params = new ArrayList<>();
        params.addAll(param);
        params.addAll(param); // add one more time

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert ignore tb(uk) value(null)
     */
    @Test
    public void insertIgnoreGlobalUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " ignore into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(100);
        param.add(100);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert ignore without unique key
     */
    @Test
    public void insertIgnoreNoUniqueKeyTest() throws Exception {
        String sql = hint + "insert ignore into " + baseOneTableName
            + " (pk,integer_test,bigint_test,datetime_test,year_test,char_test) values (?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert tb(uk) value(null) on duplicate key update
     */
    @Test
    public void UpsertUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?) on duplicate key update float_test=1";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert ignore tb(uk) value(null)
     */
    @Test
    public void insertIgnoreUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " ignore into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert tb(guk) value(null)
     */
    @Test
    public void insertGlobalUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(100);
        param.add(100);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert on duplicate key update without unique key
     */
    @Test
    public void UpsertNoUniqueKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?) on duplicate key update float_test=1";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * replace without unique key
     */
    @Test
    public void replaceNoUniqueKeyTest() throws Exception {
        String sql = hint + "replace into " + baseOneTableName
            + " (pk,integer_test,bigint_test,datetime_test,year_test,char_test) values (?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert without unique key
     */
    @Test
    public void insertNoUniqueKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,datetime_test,year_test,char_test) values (?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * replace tb(uk) value(null)
     */
    @Test
    public void replaceUniqueKeyNullTest() throws Exception {
        String sql = hint + "replace into " + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(null);
        param.add(null);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(null);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert on duplicate key update uniqueKey=xx
     */
    @Test
    public void upsertUpdateUniqueKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?) on duplicate key update varchar_test='a'";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert on duplicate key update shardingKey=xx
     */
    @Test
    public void upsertUpdateShardingKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?) on duplicate key update bigint_test=1";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * replace, twice
     */
    @Test
    public void replaceTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "replace " : "replace " + hint) + " into "
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
        // Don't assert count: for tables with no unique keys, mysql affected
        // rows is 1, but it doesn't conform to the document.
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, false);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * multi threads
     */
    @Test
    public void insertTestConcurrency() throws Exception {

        // Deadlock happens in "INSERT IGNORE".
        final String updateSql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint)
            + " into "
            + baseOneTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test) values(?,?,?,?,?,?,?)";

        final List<AssertionError> errors = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final int x = i * 100;
            Thread thread = new Thread(new Runnable() {

                public void run() {
                    Connection connection = null;
                    try {
                        connection = getPolardbxDirectConnection();

                        for (int j = 0; j < 50; j++) {
                            int y = x + j;
                            List<Object> param = new ArrayList<Object>();
                            param.add(y);
                            param.add(y);
                            param.add(y * 100);
                            param.add("test" + y);
                            param.add(columnDataGenerator.datetime_testValue);
                            param.add(2000 + j);
                            param.add(columnDataGenerator.char_testValue);

                            JdbcUtil.updateData(connection, updateSql, param);
                        }
                    } catch (AssertionError ae) {
                        errors.add(ae);
                    } finally {
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
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
}
