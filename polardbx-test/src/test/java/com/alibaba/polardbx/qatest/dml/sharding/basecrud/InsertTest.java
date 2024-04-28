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

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Insert测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class InsertTest extends CrudBasedLockTestCase {

    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE));

    }

    public InsertTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {

        if (baseOneTableName.startsWith("broadcast")) {
            JdbcUtil.setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB, tddlConnection);
        }

        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertAllFieldTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert into " + baseOneTableName + " (";
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

        sql = "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertMultiValuesTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        List<Object> param = new ArrayList<Object>();

        StringBuilder sql = new StringBuilder("insert into " + baseOneTableName + " values ");

        for (int i = 0; i < 41; i++) {
            sql.append("(");
            for (int j = 0; j < columns.size(); j++) {
                sql.append("?");
                if (j != columns.size() - 1) {
                    sql.append(",");
                }
            }
            sql.append(")");
            if (i != 40) {
                sql.append(",");
            }
            // sql.append("(?,?,?,?,?,?,?)");
            columnDataGenerator.getAllColumnValue(param, columns, PK_COLUMN_NAME, i);
        }
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql.toString(), param, true);

        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore("跨库事务不支持")
    public void insertLastInsertIdTransactionTest() throws Exception {

        mysqlConnection.setAutoCommit(false);
        tddlConnection.setAutoCommit(false);
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert into " + baseOneTableName + " (";
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

        mysqlConnection.commit();
        tddlConnection.commit();

        mysqlConnection.setAutoCommit(true);
        tddlConnection.setAutoCommit(true);

        sql = "select * from " + baseOneTableName;

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void insertIllegalYearTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,year_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        Connection conn = getPolardbxConnection();
        param.add(columnDataGenerator.pkValue);
        param.add("201911");
        param.add(columnDataGenerator.varchar_testValue);
        PreparedStatement ps1 = conn.prepareStatement(sql);
        ps1.setLong(1, (Long) param.get(0));
        ps1.setString(2, (String) param.get(1));
        ps1.setString(3, (String) param.get(2));
        try {
            int affectedRows = ps1.executeUpdate();

        } catch (SQLException e) {

        }

        sql = "select year_test from " + baseOneTableName + " where pk = ?";

        PreparedStatement ps2 = conn.prepareStatement(sql);
        ps2.setLong(1, (Long) param.get(0));
        try {
            ResultSet rs = ps2.executeQuery();
            while (rs.next()) {
                rs.getObject(1);
            }
        } finally {
            conn.close();
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertIgnoreTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert ignore into " + baseOneTableName + " (";
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
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertLOW_PRIORITYTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert LOW_PRIORITY into " + baseOneTableName + " (";
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

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertHIGH_PRIORITYTest() throws Exception {

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert HIGH_PRIORITY into " + baseOneTableName + " (";
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

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertSomeFieldTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,bigint_test,varchar_test)values(?,?+1,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(new BigDecimal(RandomStringUtils.randomNumeric(10)));
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Column list are not ordered according to table definition.
     */
    @Test
    public void insertFieldUnorderedTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (varchar_test,bigint_test,pk)values(?,?+1,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(new BigDecimal(RandomStringUtils.randomNumeric(10)));
        param.add(columnDataGenerator.pkValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertGmtStringTest() throws Exception {
        Date gmt = new Date(1350304585000l);
        Date gmtDay = new Date(1350230400000l);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String gmtString = df.format(gmt);
        String sql = "insert into " + baseOneTableName
            + " (pk,date_test,timestamp_test,datetime_test,varchar_test)values(?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(df.format(gmtDay));
        param.add(gmtString);
        param.add(gmtString);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithSetTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " set pk=? ,varchar_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithMutilTest() throws Exception {
        String sql = "insert into " + baseOneTableName + "(pk,integer_test,varchar_test) values(?,?,?),(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.pkValue + 1);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValueTwo);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Ignore(value = "目前不支持insert中带select的sql语句")
    @Test
    public void insertWithSelectTest() throws Exception {
        // tddlUpdateData("insert into student(id,name,school) values (?,?,?)",
        // Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        // mysqlUpdateData("insert into student(id,name,school) values (?,?,?)",
        // Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        //
        // String sql = "insert into " + baseOneTableName
        // + "(pk,name) select id,name from student where school=?";
        // List<Object> param = new ArrayList<Object>();
        // param.add(school);
        // executeSuccess(sql, param);
        //
        // sql = "select * from " + baseOneTableName + " where pk=" + RANDOM_ID;
        // String[] columnParam = { "name" };
        // selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        //
        // tddlUpdateData("delete from student where school=?",
        // Arrays.asList(new Object[] { school }));
        // mysqlUpdateData("delete from student where school=?",
        // Arrays.asList(new Object[] { school }));
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertPramLowerCaseTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,integer_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertPramUppercaseTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (PK,INTEGER_TEST,VARCHAR_TEST) values (?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithBdbOutParamTest() throws Exception {
        Date gmt = new Date(1350304585000l);
        Date gmtDay = new Date(1350230400000l);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sql = "insert into " + baseOneTableName
            + "(pk,date_test,timestamp_test,datetime_test,integer_test,varchar_test) values("
            + columnDataGenerator.pkValue + ",'" + df.format(gmtDay) + "','" + df.format(gmt) + "','"
            + df.format(gmt) + "'," + columnDataGenerator.integer_testValue + ",'hello'" + ")";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithNullTest() throws Exception {
        String sql = "insert into " + baseOneTableName + "(pk,integer_test,varchar_test) values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(null);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithOutKeyFieldTest() throws Exception {

        String sqlMode =
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
        if (isMySQL80()) {
            sqlMode = "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
        }
        setSqlMode(sqlMode, tddlConnection);
        if (baseOneTableName.contains(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX)
            || baseOneTableName.contains(ExecuteTableName.BROADCAST_TB_SUFFIX)) {
            // 针对单表不会报错
            return;
        }

        String sql = "insert into " + baseOneTableName + " (integer_test,varchar_test) values (?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeErrorAssert(tddlConnection, sql, param, "PK");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithZoreAndNegativeTest() throws Exception {
        long pk = 0l;
        int id = -1;
        String sql = "insert into " + baseOneTableName + " (pk,integer_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        pk = 0;
        id = 0;
        sql = "insert into " + baseOneTableName + " (pk,integer_test,varchar_test)values(?,?,?)";
        param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithMaxMinTest() throws Exception {
        long pk = Long.MAX_VALUE;
        int id = Integer.MAX_VALUE;
        String sql = "insert into " + baseOneTableName + " (pk,integer_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        pk = Long.MIN_VALUE;
        id = Integer.MIN_VALUE;
        sql = "insert into " + baseOneTableName + " (pk,integer_test,varchar_test)values(?,?,?)";
        param.clear();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithNowTest() throws Exception {
        String sql = "insert into " + baseOneTableName + "(pk,date_test,integer_test,varchar_test) values(" + 1
            + ",now()," + 1 + ",'hello'" + ")";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertErrorTypeFiledTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,datetime_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.varchar_testValue);

        executeErrorAssert(tddlConnection, sql, param, "Incorrect datetime");

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertNotExistFieldTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,gmts,name)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeErrorAssert(tddlConnection, sql, param, "Unknown target column 'gmts'");

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertNotMatchFieldTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,integer_test) values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.char_testValue);
        executeErrorAssert(tddlConnection, sql, param, "COLUMN COUNT DOESN'T MATCH VALUE COUNT AT ROW 1");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertNotMatchParameterTest() throws Exception {
        String sql = "insert into " + baseOneTableName + " (pk,integer_test) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeErrorAssert(tddlConnection, sql, param, "param");

    }

    /**
     * @since 5.1.17
     */
    @Test
    public void insertSubstringTest1() throws Exception {

        String sql = String.format(
            "insert into %s(integer_test,pk,datetime_test,varchar_test,float_test) values(?,?,?,substring(?,5),?)",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.float_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void insertSubstringTest2() throws Exception {

        String sql = String.format(
            "insert into %s(integer_test,pk,datetime_test,varchar_test,float_test) values(?,?,?,substring(?,-5),?)",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.float_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.17
     */
    @Test
    public void insertSubstringTest3() throws Exception {

        String sql = String.format(
            "insert into %s(integer_test,pk,datetime_test,varchar_test,float_test) values(?,?,?,substring(?,5,8),?)",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add("abcdefghijklmnopq");
        param.add(columnDataGenerator.float_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void insertSubstringTest4() throws Exception {

        String sql = String.format(
            "insert into %s(integer_test,pk,datetime_test,varchar_test,float_test) values(?,?,?,substring(?,5),?)",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add("abcdefghijklmnopq");
        param.add(columnDataGenerator.float_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void insertSubstringTest5() throws Exception {

        String sql = String.format(
            "insert into %s(integer_test,pk,datetime_test,varchar_test,float_test) values(?,?,?,substring(?,-5),?)",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add("abcdefghijklmnopq");
        param.add(columnDataGenerator.float_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void insertSubstringTest6() throws Exception {

        String sql = String.format(
            "insert into %s(integer_test,pk,datetime_test,varchar_test,float_test) values(?,?,?,substring(?,5,8),?)",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add("abcdefghijklmnopq");
        param.add(columnDataGenerator.float_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void insertSelectUnionTest() throws Exception {

        tableDataPrepare(baseOneTableName,
            20,
            TableColumnGenerator.getAllTypeColum(),
            PK_COLUMN_NAME,
            mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = String
            .format("insert into %s(integer_test,pk,date_test,timestamp_test,datetime_test,varchar_test,float_test) "
                    + "select integer_test,pk+100,date_test,timestamp_test,datetime_test,varchar_test,float_test from %s "
                    + "union all "
                    + "select integer_test,pk+200,date_test,timestamp_test,datetime_test,varchar_test,float_test from %s "
                    + "union all "
                    + "select integer_test,pk+300,date_test,timestamp_test,datetime_test,varchar_test,float_test from %s ",
                baseOneTableName,
                baseOneTableName,
                baseOneTableName,
                baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertBatchAllFieldTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert into " + baseOneTableName + " (";
        String values = " values ( ";

        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName();
            sql = sql + columnName + ",";
            values = values + " ?,";
        }

        sql = sql.substring(0, sql.length() - 1) + ") ";
        values = values.substring(0, values.length() - 1) + ")";

        sql = sql + values;
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertBatchAllFieldIgnoreTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert ignore into " + baseOneTableName + " (";
        String values = " values ( ";

        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName();
            sql = sql + columnName + ",";
            values = values + " ?,";
        }

        sql = sql.substring(0, sql.length() - 1) + ") ";
        values = values.substring(0, values.length() - 1) + ")";

        sql = sql + values;
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertBatchSomeFieldTestWithFunction() throws Exception {
        String sql = "insert into  " + baseOneTableName
            + "  (pk,date_test,integer_test,varchar_test) values(?,now(),?,?)";
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            String pk = RandomStringUtils.randomNumeric(8);
            param.add(i);
            param.add(Long.valueOf(pk));
            param.add("test" + i);
            params.add(param);
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = "select pk,timestamp_test,integer_test,varchar_test from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertBatchWithSomeFieldCostants() throws Exception {
        String sql = "insert into  " + baseOneTableName
            + " (pk,float_test,integer_test,varchar_test)  values(?,?,?,'hello')";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(1.1);
            param.add(i);
            params.add(param);
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Ignore
    @Test
    public void insertStringWithNormalHint() throws Exception {
        String hint = "/*+TDDL({'extra':{'JOIN_STRATEGY':'NEST_LOOP_JOIN'}})*/";
        String sql = String.format("insert into  %s  (pk,varchar_test)  values(1, \"%s\")", baseOneTableName, hint);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Ignore("insert with hint 的bug, 需要确认时间修复")
    @Test
    public void insertStringWithIllegalHint() throws Exception {
        String hint = "/*+TDDL({'extra':{'JOIN_STRATEGY':**'NEST_LOOP_JOIN'}})*/";
        String sql = String.format("insert into  %s  (pk,varchar_test)  values(1, \"%s\")", baseOneTableName, hint);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * INSERT ... ON DUPLICATE KEY UPDATE SET c1=?, c1=?
     */
    @Test
    public void insertDuplicateSetTwice() throws Exception {
        // insert init data
        String sql =
            String.format("insert into %s(integer_test,pk,varchar_test,float_test,timestamp_test) values(?,?,?,?,?)",
                baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = String.format("insert into %s(integer_test,pk,varchar_test,float_test,timestamp_test) values(?,?,?,?,?)"
                + "on duplicate key update integer_test=0, integer_test=integer_test+1, float_test=?, timestamp_test=?",
            baseOneTableName);
        param.clear();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // integer_test and varchar_test
        sql = String.format("insert into %s(integer_test,pk,varchar_test,float_test,timestamp_test) values(?,?,?,?,?)"
            + "on duplicate key update integer_test=0, integer_test=integer_test+1, "
            + "float_test=?, float_test=values(float_test), timestamp_test=?", baseOneTableName);
        param.clear();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Test insert single row in multi threads.
     */
    @Test
    public void insertSingleTestConcurrency() throws Exception {
        StringBuilder stringBuilder = new StringBuilder("insert ignore into ").append(baseOneTableName)
            .append("(pk, integer_test, varchar_test) values(?, ?, ?)");
        final String insertSql = stringBuilder.toString();
        final int INSERT_NUM = 500;

        final ConcurrentLinkedDeque<AssertionError> errors = new ConcurrentLinkedDeque<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            Thread thread = new Thread(new Runnable() {

                public void run() {
                    Connection connection = null;
                    try {
                        Random random = new Random();
                        connection = getPolardbxDirectConnection();
                        for (int i = 0; i < INSERT_NUM; i++) {
                            int num = random.nextInt();
                            JdbcUtil.updateData(connection, insertSql, Lists.newArrayList(num, num, num + ""));
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

        AssertionError error = errors.peekLast();
        if (error != null) {
            throw error;
        }

        String selectSql = "select pk, integer_test, varchar_test from " + baseOneTableName;
        ResultSet rs = JdbcUtil.executeQuery(selectSql, tddlConnection);
        while (rs.next()) {
            long pk = rs.getLong(1);
            int integer_test = rs.getInt(2);
            String varchar_test = rs.getString(3);
            Assert.assertTrue(pk == integer_test);
            Assert.assertTrue(varchar_test.equals(pk + ""));
        }
        rs.close();
    }

    /**
     * Test CASE WHEN function in VALUES and ON DUPLICATE KEY UPDATE.
     */
    @Test
    public void insertCaseWhen() {
        // insert init data
        String sql = String.format("insert into %s(integer_test,pk,varchar_test,float_test) values(?,?,?,"
            + "(case when 1+1>2 then 1.0 else 2.0 end))", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        // insert with 'case when'
        sql = String.format("insert into %s(integer_test,pk,varchar_test,float_test) values(?,?,?,?)"
                + "on duplicate key update integer_test="
                + "(case when integer_test>? then integer_test+1 else integer_test-1 end)",
            baseOneTableName);
        param.clear();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Test CAST function in VALUES and ON DUPLICATE KEY UPDATE.
     */
    @Test
    public void insertCast() {
        // insert init data
        String sql = String.format(
            "insert into %s(integer_test,pk,varchar_test,date_test) values(cast(? as unsigned),?,?,cast(? as date))",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.date_testValue.toString());
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        // insert with 'cast'
        sql = String.format("insert into %s(integer_test,pk,varchar_test) values(?,?,?)"
            + "on duplicate key update date_test=cast(? as date)", baseOneTableName);
        param.clear();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.date_testEndValue.toString());
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void insertWithDefaultKW() throws Exception {
        String sql = String.format(
            "insert into %s(integer_test, pk, varchar_test, char_test) values(default, ?, ?, default)",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * Explain an insert, and its sharding key is a function.
     * prepare模式暂不支持
     */
    @Test
    public void explainInsertShardingKeyFunc() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        String sql = String.format("explain insert into %s(pk,integer_test,varchar_test) values(?+?,?+?,concat(?,?))",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(10);
        param.add(1);
        param.add(10);
        param.add(2);
        param.add("a");
        param.add("b");

        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
        Object result1 = JdbcUtil.getAllResult(rs).get(0).get(0);
        rs.close();
        tddlPs.close();

        sql = String.format("explain insert into %s(pk,integer_test,varchar_test) values(?,?,?)", baseOneTableName);
        param.clear();
        param.add(11);
        param.add(12);
        param.add("ab");

        tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, tddlPs);
        Object result2 = JdbcUtil.getAllResult(rs).get(0).get(0);
        rs.close();
        tddlPs.close();

        int firstIndex = ((String) result1).indexOf("\"");
        int secondIndex = ((String) result1).indexOf("\"", firstIndex + 1);
        String table1 = ((String) result1).substring(firstIndex + 1, secondIndex).replace("[", "").replace("]", "");
        firstIndex = ((String) result2).indexOf("\"");
        secondIndex = ((String) result2).indexOf("\"", firstIndex + 1);
        String table2 = ((String) result2).substring(firstIndex + 1, secondIndex).replace("[", "").replace("]", "");
        Assert.assertTrue(table1.equals(table2));
    }

    /**
     * Explain Sharding an insert
     */
    @Test
    public void explainShardingInsert() throws Exception {
        String sql = String.format("explain sharding insert into %s(pk,integer_test,varchar_test) values(null, 1, 'c')",
            baseOneTableName);

        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    /*
     * Batchinsert 迁移完成
     */

    @Test
    public void insertMysqlPartitionTest() throws Exception {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert into " + baseOneTableName + " partition (p123) (";
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

        JdbcUtil.updateDataBatchFailed(tddlConnection, sql, ImmutableList.of(param),
            "Do not support table with mysql partition");
    }

    @Test
    public void insertBinaryDataTest() throws Exception {
        byte[] binaryData = new byte[2000];
        Random rand = new Random(0);
        rand.nextBytes(binaryData);

        // setBytes
        String sql = "insert into " + baseOneTableName + " (pk, blob_test) values (?, ?)";
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setInt(1, 1);
            ps.setBytes(2, binaryData);
            int updated = ps.executeUpdate();
            Assert.assertEquals(1, updated);
        }

        // setBinaryStream
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setInt(1, 2);
            ps.setBinaryStream(2, new ByteArrayInputStream(binaryData));
            int updated = ps.executeUpdate();
            Assert.assertEquals(1, updated);
        }

        // setBlob
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setInt(1, 3);
            ps.setBlob(2, new ByteArrayInputStream(binaryData));
            int updated = ps.executeUpdate();
            Assert.assertEquals(1, updated);
        }

        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(binaryData);
        String digest = TStringUtil.bytesToHexString(md.digest()).toLowerCase();

        try (Statement stmt = tddlConnection.createStatement()) {
            ResultSet rs =
                stmt.executeQuery("select pk, md5(blob_test) digest, length(blob_test) from " + baseOneTableName);
            while (rs.next()) {
                Assert.assertEquals(digest, rs.getString(2));
                Assert.assertEquals(binaryData.length, rs.getInt(3));
            }
        }

        // Check parameter in SELECT statements
        try (PreparedStatement ps = tddlConnection
            .prepareStatement("select pk, blob_test from " + baseOneTableName + " where blob_test = ?")) {
            ps.setBinaryStream(1, new ByteArrayInputStream(binaryData));
            ResultSet rs = ps.executeQuery();
            int count = 0;
            while (rs.next()) {
                Assert.assertArrayEquals(binaryData, rs.getBytes(2));
                count++;
            }
            Assert.assertEquals(3, count);
        }
    }

    @Test
    public void insertWithView() {
        final String viewName = "insert_with_view_test_view";

        // Recreate view
        String sql = "drop view " + viewName;
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sql, ImmutableSet.of("Unknown view"));
        JdbcUtil.executeUpdateSuccessIgnoreErr(mysqlConnection, sql, ImmutableSet.of("Unknown table"));

        sql = String.format("create view %s as\n"
            + "(\n"
            + "    select integer_test, varchar_test from %s as a where a.pk < 11 \n"
            + ")\n", viewName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Execute update
        sql = String.format("insert into %s(pk, bigint_test, varchar_test) "
            + "select integer_test + 100, integer_test, varchar_test from %s v", baseOneTableName, viewName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Check update result
        sql = "SELECT bigint_test, varchar_test FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // Check error message
        sql = String.format("insert into %s(integer_test, varchar_test) select integer_test, varchar_test from %s a",
            viewName, baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null,
            MessageFormat.format("{0}'' of the {1} is not updatable", viewName, "INSERT"));
        sql = String.format("insert into %s(integer_test, varchar_test) values(1, 'a')", viewName);
        executeErrorAssert(tddlConnection, sql, null,
            MessageFormat.format("{0}'' of the {1} is not updatable", viewName, "INSERT"));
        sql = String.format("insert ignore into %s(integer_test, varchar_test) values(1, 'a')", viewName);
        executeErrorAssert(tddlConnection, sql, null,
            MessageFormat.format("{0}'' of the {1} is not updatable", viewName, "INSERT"));
        sql = String.format("replace into %s(integer_test, varchar_test) values(1, 'a')", viewName);
        executeErrorAssert(tddlConnection, sql, null,
            MessageFormat.format("{0}'' of the {1} is not updatable", viewName, "REPLACE"));
    }
}
