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
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Replace测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class ReplaceTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();
    private static Log log = LogFactory.getLog(ReplaceTest.class);

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public ReplaceTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    @Before
    public void initData() {

        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        JdbcUtil.getSqlMode(tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceAllFieldTest() {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "replace into " + baseOneTableName + " (";
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

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceMultiValuesTest() {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        List<Object> param = new ArrayList<Object>();
        StringBuilder sql = new StringBuilder("replace into " + baseOneTableName
            + " values ");

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
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql.toString(), param, true);

        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null,
            mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceSomeFieldTest() {
        String sql = "replace into " + baseOneTableName
            + " (pk,integer_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceWithSetTest() {
        String sql = "replace into " + baseOneTableName
            + " set pk=? ,varchar_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceWithBdbOutParamTest() {
        if (!(baseOneTableName.contains("mysql"))) {
            Date gmt = new Date(1350304585000l);
            Date gmtDay = new Date(1350230400000l);

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "replace into "
                + baseOneTableName
                + "(pk,date_test,timestamp_test,datetime_test,integer_test,varchar_test) values("
                + columnDataGenerator.pkValue + ",'" + df.format(gmtDay) + "','"
                + df.format(gmt) + "','" + df.format(gmt) + "',"
                + columnDataGenerator.integer_testValue + ",'hello'" + ")";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                sql, null, true);

            sql = "select * from " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replacePramLowerCaseTest() {
        String sql = "replace into " + baseOneTableName
            + " (pk,integer_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replacePramUppercaseTest() {
        String sql = "REPLACE INTO " + baseOneTableName
            + " (PK,INTEGER_TEST,VARCHAR_TEST) values (?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceWithOutKeyFieldTest() throws Exception {
        String sqlMode =
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
        if (isMySQL80()) {
            sqlMode = "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
        }
        setSqlMode(sqlMode, tddlConnection);
        String sql = "replace into " + baseOneTableName
            + " (integer_test,varchar_test)values(?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeErrorAssert(tddlConnection, sql, param, "");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceWithZoreAndNegativeTest() {
        long pk = 0l;
        int id = -1;
        String sql = "replace into " + baseOneTableName
            + " (pk,integer_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        pk = 0;
        id = 0;
        sql = "replace into " + baseOneTableName + " (pk,integer_test,varchar_test)values(?,?,?)";
        param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceWithMaxMinTest() {
        long pk = Long.MAX_VALUE;
        int id = Integer.MAX_VALUE;
        String sql = "replace into " + baseOneTableName
            + " (pk,integer_test,varchar_test)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        pk = Long.MIN_VALUE;
        id = Integer.MIN_VALUE;
        sql = "replace into " + baseOneTableName
            + " (pk,integer_test,varchar_test)values(?,?,?)";
        param.clear();
        param.add(pk);
        param.add(id);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceErrorTypeFieldTest() throws Exception {
        String sqlMode =
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
        if (isMySQL80()) {
            sqlMode = "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
        }
        setSqlMode(sqlMode, tddlConnection);
        String sql = "replace into " + baseOneTableName
            + " (pk,datetime_test,varchar_test)values(?,?,?)";
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
    public void replaceNotExistFieldTest() {
        String sql = "replace into " + baseOneTableName
            + " (pk,gmts,name)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeErrorAssert(tddlConnection, sql, param, "Unknown target column 'gmts'");

    }

    /**
     * @since 5.0.1
     */
    @Ignore("duplicated with replaceWithOutKeyFieldTest")
    @Test
    public void replaceWithOutKeyValueTest() {

        String sql = "replace into " + baseOneTableName + " (name)values(?)";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void replaceNotMatchFieldTest() {
        String sql = "replace into " + baseOneTableName
            + " (pk,integer_test) values(?,?,?)";
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
    public void replaceQuoteTest() {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = String.format("replace into %s (pk,varchar_test) values (10,quote(?))", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add("'zhuoxue'");
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);

        sql = String.format("select * from %s where varchar_test =quote(?)", baseOneTableName);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void ifnullRoundTest() {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        if (!baseOneTableName
            .contains(ExecuteTableName.MULTI_DB_MUTIL_TB_BY_STRING_SUFFIX)) {
            String sql = "replace into " + baseOneTableName + " (pk,float_test) values (10,null)";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            sql = "select distinct ifnull(round(float_test/4,4),0) as a from " + baseOneTableName + " order by a";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

}
