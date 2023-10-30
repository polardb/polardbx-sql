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
import com.alibaba.polardbx.qatest.data.TableEntityGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * Insert重复数据
 *
 * @author zhuoxue
 * @since 5.0.1
 */


public class InsertDuplicatedTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE));

    }

    public InsertDuplicatedTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {

        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertAllFieldTest() {
        String sql =
            columnDataGenerator.getInsertSqlTemplate(TableEntityGenerator.getNormalTableEntity(baseOneTableName));
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, 1);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = sql + " ON DUPLICATE KEY UPDATE  char_test=?";
        param.add("aaa");
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertFunction() {

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

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = sql + " ON DUPLICATE KEY UPDATE  date_test=now()";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertNull() {

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

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = sql + " ON DUPLICATE KEY UPDATE  char_test=NULL";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertFunctions() {
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

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = sql + "ON DUPLICATE KEY UPDATE  char_test=?,date_test=now()";
        param.add("mm");
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertUpdateValuesClause() {
        String sql = "insert into " + baseOneTableName + " (pk, integer_test, varchar_test, char_test) " +
            "values(1, 1, 'a', 'aa'), (2, 2, 'b', 'bb'), (3, 3, 'c', 'cc')";

        List<Object> param = new ArrayList<>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "insert into " + baseOneTableName + " (pk, integer_test, varchar_test, char_test) " +
            "values(1, 10, 'a', 'dd'), (2, 20, 'b', 'ee'), (3, 30, 'c', 'ff') " +
            "ON DUPLICATE KEY UPDATE char_test=values(char_test), integer_test=values(integer_test)";
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
    public void insertSomeFields() {
        String sql = "insert into " + baseOneTableName + " (pk, integer_test, varchar_test, char_test, float_test) " +
            "values(1, 1, 'a', 'aa', 10.0), (2, 2, 'b', 'bb', 20.0), (3, 3, 'c', 'cc', 30.0)";

        List<Object> param = new ArrayList<>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "insert into " + baseOneTableName + " (pk, varchar_test) " +
            "values(1, 'a'), (2, 'b'), (3, 'c') " +
            "ON DUPLICATE KEY UPDATE char_test='dd', integer_test=10, float_test=100.0";
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
    public void insertBatchDuplicate() {

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
        for (int i = 0; i < 10; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = sql + "ON DUPLICATE KEY UPDATE  char_test=?,date_test=now()";

        params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
            param.add("ii");
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
    public void insertDuplicateMultiValues() {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert into " + baseOneTableName + " ( ";
        String values = " values ";

        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName();
            sql = sql + columnName + ",";
        }
        sql = sql.substring(0, sql.length() - 1) + ") ";

        for (int i = 0; i < 10; i++) {
            values = values + "(";
            for (int j = 0; j < columns.size(); j++) {
                values = values + " ?,";
            }
            values = values.substring(0, values.length() - 1) + "),";
        }
        values = values.substring(0, values.length() - 1);

        sql = sql + values;
        List<Object> params = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i);
            params.addAll(param);
        }

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, params, true);
        sql = sql + "ON DUPLICATE KEY UPDATE  char_test=?,integer_test=?,float_test=?";

        params.add("ii");
        params.add(10);
        params.add(1.0f);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, params, true);

        sql = "select * from " + baseOneTableName + " order by pk";
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertWithUpperCase() {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "insert into " + baseOneTableName + " (";
        String values = " values ( ";

        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName().toUpperCase();
            sql = sql + columnName + ",";
            values = values + " ?,";
        }

        sql = sql.substring(0, sql.length() - 1) + ") ";
        values = values.substring(0, values.length() - 1) + ")";

        sql = sql + values;
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, 1);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = sql + "ON DUPLICATE KEY UPDATE  CHAR_TEST=?, INTEGER_test=?";
        param.add("mm");
        param.add(1);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     *
     */
    @Test
    public void insertDuplicateWithTableName() {
        String sql = "insert into " + baseOneTableName + " (pk, integer_test, varchar_test, char_test, float_test) " +
            "values(1, 1, 'a', 'aa', 10.0), (2, 2, 'b', 'bb', 20.0), (3, 3, 'c', 'cc', 30.0)";

        List<Object> param = new ArrayList<>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "insert into %s (pk, varchar_test) " +
            "values(1, 'a'), (2, 'b'), (3, 'c') " +
            "ON DUPLICATE KEY UPDATE %s.char_test='dd', %s.integer_test=10, float_test=100.0";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     *
     */
    @Test
    public void insertDuplicateWithWrongTableName() {
        String sql = "insert into %s (pk, varchar_test) " +
            "values(1, 'a'), (2, 'b'), (3, 'c') " +
            "ON DUPLICATE KEY UPDATE %s.char_test='dd', %s.integer_test=10, float_test=100.0";
        sql = String.format(sql, baseOneTableName, "wrongName", baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "not found");
    }
}
