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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import org.apache.calcite.util.Pair;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase.DISABLE_DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO;
import static com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase.DML_EXECUTION_STRATEGY_LOGICAL;
import static com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase.ENABLE_DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectStringContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

public class DefaultValueTest extends AutoCrudBasedLockTestCase {
    private static final String TABLE_NAME = "default_value_test_auto";
    private static final String CREAT_TABLE = "CREATE TABLE `" + TABLE_NAME + "` ("
        + " `pk` int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1` int(11) NULL,"
        + " `c2` int(11) NOT NULL DEFAULT '1',"
        + " `c3` int(11) NOT NULL DEFAULT '2',"
        + " `c4` int(11) NOT NULL DEFAULT '3',"
        + " `c5` int(11) NOT NULL DEFAULT '4',"
        + " `c6` varchar(20) NOT NULL DEFAULT \"123\","
        + " `c7` varchar(20) NOT NULL DEFAULT \"abc\","
        + " `c8` varchar(20) NULL,"
        + " PRIMARY KEY (`pk`)"
        + ") {0} ";
    private static final String PARTITIONS_METHOD = "";
    private static final String SELECT_COLUMN = "c1,c2,c3,c4,c5,c6,c7,c8";
    private static final List<String> SUPPORT_INSERT = new ArrayList<>();
    private static final List<String> UN_SUPPORT_INSERT = new ArrayList<>();
    private static final List<String> SUPPORT_UPDATE = new ArrayList<>();
    private static final List<String> UN_SUPPORT_UPDATE = new ArrayList<>();
    private static final List<String> SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE = new ArrayList<>();
    private static final List<String> UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE = new ArrayList<>();
    private static final List<String> SUPPORT_SELECT = new ArrayList<>();
    private static final List<String> UN_SUPPORT_SELECT = new ArrayList<>();

    static {
        String temp = "insert into " + TABLE_NAME + " %s values %s;";
        //Insert语句支持default值的表达式
        //插入同一列
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3, c4)", "(default, c3, default(c4))"));
        //插入不同列
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4, default(c5))"));
        //插入不同类型
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c6, default(c6))"));
        SUPPORT_INSERT.add(String.format(temp, "(c6, c7)", "(c2, default(c3))"));
        //计算式
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4 + 1, default(c4) + 1)"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4 - 1, default(c4) - 1)"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4 * 1, default(c4) * 1)"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4 / 1, default(c4) / 1)"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c6 + 1, default(c6) + 1)"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c6 + '1' + 1, default(c6) + '1' + 1)"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4 + c4, default(c4) + default(c4))"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4 + c5, default(c4) + default(c5))"));
        SUPPORT_INSERT.add(String.format(temp, "(c2, c3)", "(c4 + default(c5), default(c4) + c5)"));

        //插入主键，AUTO_INCREMENT
        SUPPORT_INSERT.add(String.format(temp, "(pk)", "(pk),(default),(default(pk))"));
        //null插入null
        SUPPORT_INSERT.add(String.format(temp, "(c1)", "(c1),(default(c1)),(c8)"));

        //Insert语句不支持default值的表达式
        UN_SUPPORT_INSERT.add(String.format(temp, "(c2)", "(default + 1)"));
        UN_SUPPORT_INSERT.add(String.format(temp, "(c2)", "(default + c3)"));
        UN_SUPPORT_INSERT.add(String.format(temp, "(c2)", "(default + default(c3))"));
        UN_SUPPORT_INSERT.add(String.format(temp, "(c2)", "(c7 + 1)"));
        UN_SUPPORT_INSERT.add(String.format(temp, "(c2)", "(default(c7) + 1)"));
        UN_SUPPORT_INSERT.add(String.format(temp, "(c6)", "(c6 + default(c7))"));
        UN_SUPPORT_INSERT.add(String.format(temp, "(c6)", "(default(c7) + 'cba')"));

        temp = "update " + TABLE_NAME + " set %s";
        //Update语句支持default的SQL:
        SUPPORT_UPDATE.add(String.format(temp, "c2 = default"));
        SUPPORT_UPDATE.add(String.format(temp, "c3 = default(pk)"));
        SUPPORT_UPDATE.add(String.format(temp, "c2 = default(c3)"));
        SUPPORT_UPDATE.add(String.format(temp, "c2 = 2 * default(c4) + default(c3) - 1"));
        SUPPORT_UPDATE.add(String.format(temp, "c2 = default(c6)"));
        SUPPORT_UPDATE.add(String.format(temp, "c2 = default(c6) + 20"));
        SUPPORT_UPDATE.add(String.format(temp, "pk = default(c3) + 20"));
        SUPPORT_UPDATE.add(String.format(temp, "pk = default(c4), c4 = default, c3 = default(c2) + 3"));

        //Update语句不支持default值的SQL:
        UN_SUPPORT_UPDATE.add(String.format(temp, "c2 = default(c1)"));
        UN_SUPPORT_UPDATE.add(String.format(temp, "c2 = 1 + default(c1)"));
        UN_SUPPORT_UPDATE.add(String.format(temp, "c2 = default + 2"));
        UN_SUPPORT_UPDATE.add(String.format(temp, "c2 = default(c7)"));
        //sql_mode 包含 STRICT_ALL_TABLES 或 STRICT_TRANS_TABLES，SqlModeStrict为true时，关闭隐式转换，才会出错
        UN_SUPPORT_UPDATE.add(String.format(temp, "pk = default"));
        UN_SUPPORT_UPDATE.add(String.format(temp, "pk = default(c1)"));
        UN_SUPPORT_UPDATE.add(String.format(temp, "pk = default(c1) + 1"));

        temp = "insert into " + TABLE_NAME + "(pk) values(200) on duplicate key update %s";
        //insert on duplicate key update 语句,支持:
        SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = default"));
        SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = default(c3)"));
        SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = 2 * default(c4) + default(c3) - 1"));
        SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = default(c6)"));
        SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = default(c6) + 20"));
        SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "pk = default(c3) + 20"));
        SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(
            String.format(temp, "pk = default(c4), c4 = default, c3 = default(c2) + 3"));

        //insert on duplicate key update 语句,不支持:
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = default(c1)"));
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = 1 + default(c1)"));
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = default + 2"));
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c2 = default(c7)"));
        //sql_mode 包含 STRICT_ALL_TABLES 或 STRICT_TRANS_TABLES，SqlModeStrict为true时，关闭隐式转换，才会出错
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "c3 = default(pk)"));
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "pk = default"));
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "pk = default(c1)"));
        UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE.add(String.format(temp, "pk = default(c1) + 1"));

        temp = "select %s from " + TABLE_NAME;
        //Select 支持default值的sql
        SUPPORT_SELECT.add(String.format(temp, "default(c1)"));
        SUPPORT_SELECT.add(String.format(temp, "default(c2)"));
        SUPPORT_SELECT.add(String.format(temp, "default(c2) + 1"));
        SUPPORT_SELECT.add(String.format(temp, "default(pk)"));
        SUPPORT_SELECT.add(String.format(temp, "default(pk) + 1"));
        SUPPORT_SELECT.add(String.format(temp, "2, default(c3) * 2 + 3, default(c6), default(c7)"));
        SUPPORT_SELECT.add(
            String.format(temp, "concat(default(c2), 'c'), concat(default(c6), 'c'), concat(default(c7), 'c')"));

        //Select 不支持default值的sql
        UN_SUPPORT_SELECT.add(String.format(temp, "default"));
        UN_SUPPORT_SELECT.add(String.format(temp, "default + 1"));
    }

    private Connection polardbxConnection;

    @Before
    public void initData() throws Exception {

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, MessageFormat.format(CREAT_TABLE, ""));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREAT_TABLE, PARTITIONS_METHOD));

        this.polardbxConnection = getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "set sql_mode=''");
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "set DML_REPLACE_IMPLICIT_DEFAULT=true");
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "set DML_REPLACE_DYNAMIC_IMPLICIT_DEFAULT=true");
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "set DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=true");
    }

    @After
    public void closeConnection() {
        JdbcUtil.closeConnection(polardbxConnection);
    }

    private void truncateData(String tableName) {
        String sql = "Truncate table " + tableName;
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void insertDefaultOkTest() {
        for (String sql : SUPPORT_INSERT) {
            truncateData(TABLE_NAME);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            String select = "select " + SELECT_COLUMN + " from " + TABLE_NAME;
            selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void insertDefaultErrTest() {
        for (String sql : UN_SUPPORT_INSERT) {
            //truncateData(TABLE_NAME);
            String mysqlError = JdbcUtil.executeUpdateFailedReturn(mysqlConnection, sql);
            String tddlError = JdbcUtil.executeUpdateFailedReturn(tddlConnection, sql);
            assertWithMessage("TDDL错误不正确，sql: " + sql + " Error: " + tddlError)
                .that(tddlError).contains("ERR_EXECUTE_ON_MYSQL");
        }
    }

    @Test
    public void insertErrDefaultColumnTest() {
        String sql = "insert into " + TABLE_NAME + "(c2) values(pkkkk + 1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Column 'pkkkk' not found in any table");

        sql = "insert into " + TABLE_NAME + "(c2) values(default(pkkkk) + 1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Column 'pkkkk' not found in any table");

    }

    @Test
    public void insertDefaultWithAutoIncrementTest() {
        Connection connection = null;
        try {
            connection = getPolardbxDirectConnection();
            String sqlModeString = "set session sql_mode=''";
            JdbcUtil.executeSuccess(connection, sqlModeString);
            String sql = "truncate table " + TABLE_NAME;
            JdbcUtil.executeUpdateSuccess(connection, sql);
            sql = "insert into " + TABLE_NAME + "(pk) values(pk + 1),(default(pk) + 2)";
            JdbcUtil.executeUpdateSuccess(connection, sql);
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, "select pk from " + TABLE_NAME + " order by pk");
            List<List<Object>> results = JdbcUtil.getAllResult(rs);
            Assert.assertEquals(1, Integer.parseInt(results.get(0).get(0).toString()));
            Assert.assertEquals(2, Integer.parseInt(results.get(1).get(0).toString()));

            sql = "truncate table " + TABLE_NAME;
            JdbcUtil.executeUpdateSuccess(connection, sql);
            sql = "insert into " + TABLE_NAME + "(c1) values(default(pk) + 2),(pk),(pk + 1);";
            JdbcUtil.executeUpdateSuccess(connection, sql);
            rs = JdbcUtil.executeQuerySuccess(connection, "select c1 from " + TABLE_NAME + " order by c1");
            results = JdbcUtil.getAllResult(rs);
            Assert.assertEquals(0, Integer.parseInt(results.get(0).get(0).toString()));
            Assert.assertEquals(1, Integer.parseInt(results.get(1).get(0).toString()));
            Assert.assertEquals(2, Integer.parseInt(results.get(2).get(0).toString()));

        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
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

    @Test
    public void updateDefaultOkTest() {
        for (String sql : SUPPORT_UPDATE) {
            truncateData(TABLE_NAME);
            String prepareData = "insert into " + TABLE_NAME
                + "(pk, c1, c2, c3, c4, c5, c6, c7, c8) values(200, 200, 200, 200, 200, 200, '200', '200', '200')";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, prepareData, null);

            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            String select = "select * from " + TABLE_NAME;
            selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void updateDefaultErrTest() {
        for (String sql : UN_SUPPORT_UPDATE) {
            truncateData(TABLE_NAME);
            String prepareData = "insert into " + TABLE_NAME
                + "(pk, c1, c2, c3, c4, c5, c6, c7, c8) values(200, 200, 200, 200, 200, 200, '200', '200', '200')";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, prepareData, null);
            //String mysqlError = JdbcUtil.executeUpdateFailedReturn(mysqlConnection, sql);
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, sql);
        }
    }

    @Test
    public void insertOnDuplicateKeyUpdateDefaultOkTest() {
        for (String sql : SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE) {
            truncateData(TABLE_NAME);
            String prepareData = "insert into " + TABLE_NAME
                + "(pk, c1, c2, c3, c4, c5, c6, c7, c8) values(200, 200, 200, 200, 200, 200, '200', '200', '200')";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, prepareData, null);

            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            String select = "select * from " + TABLE_NAME;
            selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void insertOnDuplicateKeyUpdateDefaultErrTest() {
        for (String sql : UN_SUPPORT_INSERT_ON_DUPLICATE_KEY_UPDATE) {
            truncateData(TABLE_NAME);
            String prepareData = "insert into " + TABLE_NAME
                + "(pk, c1, c2, c3, c4, c5, c6, c7, c8) values(200, 200, 200, 200, 200, 200, '200', '200', '200')";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, prepareData, null);
            //String mysqlError = JdbcUtil.executeUpdateFailedReturn(mysqlConnection, sql);
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, sql);
        }
    }

    @Test
    public void selectDefaultOkTest() {
        for (String sql : SUPPORT_SELECT) {

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        }
    }

    @Test
    public void selectDefaultErrTest() {
        for (String sql : UN_SUPPORT_SELECT) {
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, sql);
        }
    }

    @Test
    public void defaultWithImplicitPkTest() throws SQLException, InterruptedException {
        final String tableName = "t1_default_with_implicit_pk";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n"
            + "  ) partition by key(c1) partitions 8;\n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        String sql = "INSERT INTO " + tableName + " VALUES();";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.SECONDS.sleep(1);
        sql = "INSERT INTO " + tableName + " VALUES(DEFAULT);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.SECONDS.sleep(1);
        sql = "INSERT INTO " + tableName + " VALUES(DEFAULT(c1));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        int count = 6;
        do {
            // make sure the timestamp is different from upper statements
            TimeUnit.SECONDS.sleep(1);
            sql = "INSERT INTO " + tableName + " VALUES(null);";
            JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        } while (count-- > 0);

        // c1 is also partition key, check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery("select c1 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness(hint,
            tableName,
            allResult,
            ImmutableList.of("c1"),
            ImmutableList.of("c1"),
            polardbxConnection);
    }

    @Ignore
    @Test
    public void dynamicImplicitDefaultTest() throws SQLException, InterruptedException {
        final String tableName = "t2_dynamic_implicit_default";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
            + "  ) partition by hash(c2) partitions 8;\n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        String sql = "INSERT INTO " + tableName + " VALUES();";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(DEFAULT, DEFAULT);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(DEFAULT(c1), DEFAULT(c2));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(DEFAULT(c1), DEFAULT(c1));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(DEFAULT(c2), DEFAULT(c2));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(null, c1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(c2, null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(null + null, now() + null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        int count = 6;
        do {
            // make sure the timestamp is different from upper statements
            TimeUnit.MILLISECONDS.sleep(100);
            sql = "INSERT INTO " + tableName + " VALUES(null, null);";
            JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        } while (count-- > 0);

        // Replace one, replace all
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(null, null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=FALSE") + sql);
        checkTrace(polardbxConnection,
            Matchers.is(1),
            (t, builder) -> {
                builder.that(t.get(0).get(12)).doesNotContain("null, null");
                builder.that(t.get(0).get(11)).contains("IFNULL(?, ?)");
            });
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=TRUE") + sql);
        checkTrace(polardbxConnection, Matchers.is(1),
            (t, builder) -> builder.that(t.get(0).get(12)).doesNotContain("null"));

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery("select c1, c2 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("c1", "c2"),
            ImmutableList.of("c1", "c2"),
            polardbxConnection);
        checkColumnDataSame(tableName, polardbxConnection, "(round(DATE_FORMAT(c1, '%s.%f')) % 60)",
            "(round(DATE_FORMAT(c2, '%s.%f')) % 60)", ImmutableList.of("c1", "c2"));
    }

    @Test
    public void staticImplicitDefaultInsertIgnoreTest() throws SQLException, InterruptedException {
        final String tableName = "t3_static_implicit_default";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `c1` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "  `c3` bigint NOT NULL ,\n"
            + "  `c4` varchar(32) NOT NULL \n"
            + "  ) partition by hash(c3) partitions 8;\n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        String sql = "INSERT IGNORE INTO " + tableName + " VALUES();";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " VALUES(DEFAULT, DEFAULT, DEFAULT, DEFAULT);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " VALUES(DEFAULT(c1), DEFAULT(c2), DEFAULT(c3), DEFAULT(c4));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " VALUES(DEFAULT(c1), DEFAULT(c1), DEFAULT(c3), DEFAULT(c3));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " VALUES(DEFAULT(c2), DEFAULT(c2), DEFAULT(c4), DEFAULT(c4));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " VALUES(null, null, null, null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Replace necessary column only
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT IGNORE INTO " + tableName + " VALUES(null, null, null, null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.is(1),
            (t, builder) -> builder.that(t.get(0).get(12)).contains("null, null, 0, null"));

        // c1 is timestamp type and not partition key, do not replace, and no exception for insert
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT INTO " + tableName + "(c1) VALUES(null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.is(1),
            (t, builder) -> builder.that(t.get(0).get(12)).contains("null"));

        // c3 is partition key, do replace for insert ignore and insert
        // Check explanation in com.alibaba.polardbx.optimizer.core.rel.LogicalInsert.initImplicitDefaultColumnIndexMap
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT INTO " + tableName + "(c3) VALUES(null);";
//            JdbcUtil.executeUpdateFailed(polardbxConnection, sql, "Column 'c3' cannot be null");
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.is(1),
            (t, builder) -> builder.that(t.get(0).get(12)).doesNotContain("null"));
        sql = "trace INSERT IGNORE INTO " + tableName + "(c3) VALUES(null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.is(1),
            (t, builder) -> builder.that(t.get(0).get(12)).doesNotContain("null"));

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult =
            JdbcUtil.getAllResult(
                JdbcUtil.executeQuery("select c1, c2, c3, c4 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("c1", "c2", "c3", "c4"),
            ImmutableList.of("c2", "c3"),
            polardbxConnection);

        checkColumnDataSame(tableName, polardbxConnection, "c1", "c2", ImmutableList.of("c1", "c2"));
    }

    @Test
    public void dynamicImplicitDefaultWithGsiTest() throws SQLException, InterruptedException {
        final String tableName = "t4_dynamic_implicit_default_with_gsi";
        final String gsiName = "g_c4";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `id` bigint NOT NULL DEFAULT 0,\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "    `c3` bigint NOT NULL ,\n"
            + "    `c4` varchar(32) NOT NULL ,\n"
            + "    `c5` bigint NULL DEFAULT NULL, \n"
            + "    global index `" + gsiName + "`(c4) covering(c2) partition by key(c4) partitions 8\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c3`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        String sql = "INSERT INTO " + tableName + " VALUES(1, null, null, 1, 1, 1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=FALSE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            for (int i = 0; i < t.size(); i++) {
                final List<String> traceRow = t.get(i);
                builder.that(traceRow.get(11)).contains("IFNULL(?, ?)");
                builder.that(traceRow.get(12)).doesNotContain("null, null");
            }
        });
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=TRUE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        sql = "INSERT INTO " + tableName
            + " VALUES(2, null, null, 1, 1, 1), (2, '2024-11-19 17:09:25', '2024-11-19 17:09:25', 1, 1, 1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=FALSE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            for (int i = 0; i < t.size(); i++) {
                final List<String> traceRow = t.get(i);
                builder.that(traceRow.get(11)).contains("IFNULL(?, ?)");
                builder.that(traceRow.get(12)).doesNotContain("null, null");
                builder.that(traceRow.get(12)).contains("1, 1");
                builder.that(traceRow.get(12)).contains("2024-11-19 17:09:25");
            }
        });
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=TRUE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(3, null, c1, 1, 1, 1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=FALSE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            for (int i = 0; i < t.size(); i++) {
                final List<String> traceRow = t.get(i);
                if (TStringUtil.containsIgnoreCase(traceRow.get(12), tableName)) {
                    // Insert for primary table
                    builder.that(traceRow.get(11)).contains("IFNULL(?, ?)");
                    builder.that(traceRow.get(12)).doesNotContain("null, null");
                } else {
                    // Insert for gsi table
                    builder.that(traceRow.get(11)).doesNotContain("IFNULL(?, ?)");
                    builder.that(traceRow.get(12)).doesNotContain("null");
                }
            }
        });
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=TRUE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(4, c2, null, 1, 1, 1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(5, null + null, now() + null, 1, 1, 1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=FALSE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            for (int i = 0; i < t.size(); i++) {
                final List<String> traceRow = t.get(i);
                if (TStringUtil.containsIgnoreCase(traceRow.get(12), tableName)) {
                    // Insert for primary table
                    builder.that(traceRow.get(11)).contains("IFNULL((NULL + NULL), ?)");
                    builder.that(traceRow.get(12)).doesNotContain("null, null");
                } else {
                    // Insert for gsi table
                    builder.that(traceRow.get(11)).contains("IFNULL((CAST(? AS DATETIME) + NULL)");
                    builder.that(traceRow.get(12)).doesNotContain("null");
                }
            }
        });
        JdbcUtil.executeUpdateSuccess(polardbxConnection,
            "trace " + buildCmdExtra("DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM=TRUE") + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        int count = 6;
        do {
            // make sure the timestamp is different from upper statements
            TimeUnit.MILLISECONDS.sleep(100);
            sql = "INSERT INTO " + tableName + " VALUES(6, null, null, 1, 1, 1);";
            JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        } while (count-- > 0);

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4, c5 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("c1", "c2", "c3", "c4", "c5"),
            ImmutableList.of("c3", "c4"),
            polardbxConnection);

        gsiIntegrityCheck(polardbxConnection, tableName, gsiName);
        checkColumnDataSame(tableName, polardbxConnection, "(round(DATE_FORMAT(c1, '%s.%f')) % 60)",
            "(round(DATE_FORMAT(c2, '%s.%f')) % 60)", ImmutableList.of("c1", "c2"));
    }

    @Test
    public void dynamicImplicitDefaultWithGsiInsertIgnoreTest() throws SQLException, InterruptedException {
        final String tableName = "t5_dynamic_implicit_default_with_gsi";
        final String gsiName = "g_c1";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "    `c3` bigint NOT NULL ,\n"
            + "    `c4` varchar(32) NOT NULL ,\n"
            + "    `c5` bigint NULL DEFAULT NULL, \n"
            + "    clustered index `" + gsiName + "`(c1) partition by key(c1) partitions 8\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c2`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        // Replace dynamic implicit value only, do not replace static implicit value
        String sql = "INSERT IGNORE INTO " + tableName + " VALUES(null, null, null, null, null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).contains("null, null, null");
            builder.that(t.get(1).get(12)).contains("null, null, null");
        });

        sql = "INSERT IGNORE INTO " + tableName
            + " VALUES(null, null, null, null, null), ('2024-11-19 17:09:25', '2024-11-19 17:09:25', 1, 1, 1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.lessThan(5), (t, builder) -> {
            int unreplacedCount = 0;
            int replacedCount = 0;
            for (int i = 0; i < t.size(); i++) {
                final String params = t.get(i).get(12);
                builder.that(params).doesNotContain("null, null, null, null, null");
                if (params.contains("1, 1, 1")) {
                    builder.that(params).contains("2024-11-19 17:09:25, 2024-11-19 17:09:25, ");
                    unreplacedCount++;
                }
                if (params.contains("null, null, null")) {
                    if (params.split(",").length < 8) {
                        builder.that(params).doesNotContain("2024-11-19 17:09:25");
                    }
                    replacedCount++;
                }
            }
            builder.that(unreplacedCount).isEqualTo(2);
            builder.that(replacedCount).isEqualTo(2);
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " VALUES(null, c1, c1, c1, c1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " VALUES(c2, null, c2, c2, c2);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName
            + " VALUES(1 + null, null + now(), null + null, 1 + null, null + null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain("null");
            builder.that(t.get(1).get(12)).doesNotContain("null");
        });

        int count = 6;
        do {
            // make sure the timestamp is different from upper statements
            TimeUnit.MILLISECONDS.sleep(100);
            sql = "INSERT IGNORE INTO " + tableName + " VALUES(null, null, null, null, null);";
            JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        } while (count-- > 0);

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4, c5 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("c1", "c2", "c3", "c4", "c5"),
            ImmutableList.of("c3", "c4"),
            polardbxConnection);

        gsiIntegrityCheck(polardbxConnection, tableName, gsiName);
        checkColumnDataSame(tableName, polardbxConnection, "(round(DATE_FORMAT(c1, '%s.%f')) % 60)",
            "(round(DATE_FORMAT(c2, '%s.%f')) % 60)", ImmutableList.of("c1", "c2"));
    }

    @Test
    public void dynamicImplicitDefaultWithUpdateTest1() throws SQLException, InterruptedException {
        final String tableName = "t6_dynamic_implicit_default_update";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "    `c3` bigint NOT NULL ,\n"
            + "    `c4` varchar(32) NOT NULL ,\n"
            + "    `c5` bigint NULL DEFAULT NULL \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c2`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        final String timestampValue = "2024-11-25 17:49:57.84679";
        String sql = "INSERT INTO " + tableName
            + " VALUES('" + timestampValue + "', '" + timestampValue + "', 1, 1, 1),"
            + "('" + timestampValue + "', '" + timestampValue + "', 2, 2, 2),"
            + "('" + timestampValue + "', '" + timestampValue + "', 3, 3, 3);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Modify partition column with type 'timestamp not null' and set column value to null
        // Replace all updated timestamp column value with current_timestamp
        sql = "UPDATE " + tableName + " SET c1 = null, c2 = null WHERE c3 = 1 AND c2 = '" + timestampValue + "' ;";
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(1).get(12)).doesNotContain(", null");
            builder.that(t.get(2).get(12)).doesNotContain(", null");
        });

        // Modify ordinary column with type 'timestamp not null' and set column value to null
        // Partition column with property 'ON UPDATE CURRENT_TIMESTAMP'
        // Replace all updated timestamp column value with current_timestamp
        sql = "UPDATE " + tableName + " SET c1 = null WHERE c3 = 2 AND c2 = '" + timestampValue + "' ;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(1).get(12)).doesNotContain(", null");
            builder.that(t.get(2).get(12)).doesNotContain(", null");
        });

        // Modify ordinary column with type 'timestamp not null' and set column value to null
        // Partition column with property 'ON UPDATE CURRENT_TIMESTAMP'
        // Partition column's new value is identical to old value
        // Replace all updated timestamp column value with current_timestamp
        sql = "UPDATE " + tableName + " SET c1 = null, c2 = '" + timestampValue + "' WHERE c3 = 3 AND c2 = '"
            + timestampValue + "' ;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(1).get(12)).doesNotContain(", null");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult1 = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4, c5 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult1,
            ImmutableList.of("c1", "c2", "c3", "c4", "c5"),
            ImmutableList.of("c2", "c1"),
            polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultWithUpdateTest2() throws SQLException, InterruptedException {
        final String tableName = "t7_dynamic_implicit_default_update";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            + "    `c3` bigint NOT NULL ,\n"
            + "    `c4` varchar(32) NOT NULL ,\n"
            + "    `c5` bigint NULL DEFAULT NULL \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c2`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        final String timestampValue = "2024-11-25 17:49:57.84679";
        String sql = "INSERT INTO " + tableName
            + " VALUES('" + timestampValue + "', '" + timestampValue + "', 1, 1, 1),"
            + "('" + timestampValue + "', '" + timestampValue + "', 2, 2, 2),"
            + "('" + timestampValue + "', '" + timestampValue + "', 3, 3, 3);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Modify ordinary column with type 'timestamp not null' and set column value to null
        // Partition column without property 'ON UPDATE CURRENT_TIMESTAMP'
        // Do not replace timestamp column value with current_timestamp(), let dn do it
        sql = "UPDATE " + tableName + " SET c1 = null WHERE c3 = 2 AND c2 = '" + timestampValue + "' ;";
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(1), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains(" = NULL");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult1 = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4, c5 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult1,
            ImmutableList.of("c1", "c2", "c3", "c4", "c5"),
            ImmutableList.of("c2", "c1"),
            polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultWithUpdateTest3() throws SQLException, InterruptedException {
        final String tableName = "t8_dynamic_implicit_default_update";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` bigint NOT NULL ,\n"
            + "    `c3` varchar(32) NOT NULL ,\n"
            + "    `c4` bigint NULL DEFAULT NULL \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c2`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        final String timestampValue = "2024-11-25 17:49:57.84679";
        String sql = "INSERT INTO " + tableName
            + " VALUES('" + timestampValue + "', 1, 1, 1),"
            + "('" + timestampValue + "', 2, 2, 2);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Modify ordinary column with type 'timestamp not null' and set column value to null
        // Do not replace null value, let DN do the work
        sql = "UPDATE " + tableName + " SET c1 = null WHERE c2 = 1;";
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(1), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains(" = NULL WHERE");
        });

        // Modify ordinary column with type 'timestamp not null' and set column value to null
        // Modify partition column without dynamic implicit value and set column value to non-null
        // Do not replace null value, let DN do the work
        sql = "UPDATE " + tableName + " SET c1 = null, c2 = 3 WHERE c2 = 1;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains(", NULL, ?");
            builder.that(t.get(2).get(12)).contains(", null, 3");
        });

        // Modify ordinary column with type 'timestamp not null' and set column value to null
        // Modify partition column without dynamic implicit value and set column value to null
        // Do not replace null value, let DN throw exception
        sql = "UPDATE " + tableName + " SET c1 = null, c2 = null WHERE c2 = 2;";
        JdbcUtil.executeUpdateFailed(polardbxConnection, "trace " + sql, "Column 'c2' cannot be null");
        checkTrace(polardbxConnection, Matchers.is(2), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains(", NULL, NULL");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult2 = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult2,
            ImmutableList.of("c1", "c2", "c3", "c4"),
            ImmutableList.of("c2", "c1"),
            polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultWithGsiUpdateTest() throws SQLException, InterruptedException {
        final String tableName = "t9_dynamic_implicit_default_with_gsi";
        final String gsiName = "g_c1";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "    `c3` bigint NOT NULL ,\n"
            + "    `c4` varchar(32) NOT NULL ,\n"
            + "    `c5` bigint NULL DEFAULT NULL, \n"
            + "    clustered index `" + gsiName + "`(c1) partition by key(c1) partitions 8\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c2`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        final String timestampValue = "2024-11-25 17:49:57.84679";
        String sql = "INSERT INTO " + tableName
            + " VALUES('" + timestampValue + "', '" + timestampValue + "', 1, 1, 1),"
            + "('" + timestampValue + "', '" + timestampValue + "', 2, 2, 2),"
            + "('" + timestampValue + "', '" + timestampValue + "', 3, 3, 3),"
            + "('" + timestampValue + "', '" + timestampValue + "', 4, 4, 4);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Update gsi partition key (with type timestamp not null) to null
        // Specify condition of primary table partition key so that SELECT part can be pushdown
        sql = "UPDATE " + tableName + " SET c1 = null WHERE c3 = 1 AND c2 = '" + timestampValue + "' ;";
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(5), (t, builder) -> {
            builder.that(t.get(1).get(12)).doesNotContain(", null");
            builder.that(t.get(2).get(12)).doesNotContain(", null");
            builder.that(t.get(3).get(12)).doesNotContain(", null");
            builder.that(t.get(4).get(12)).doesNotContain(", null");
        });

        // Update primary table partition key (with type timestamp not null) to null
        // Specify condition of gsi partition key so that SELECT part first pushdown to gsi then lookup primary
        sql = "UPDATE " + tableName + " SET c2 = null WHERE c3 = 2 AND c1 = '" + timestampValue + "' ;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(6), (t, builder) -> {
            builder.that(t.get(2).get(12)).doesNotContain(", null");
            builder.that(t.get(3).get(12)).doesNotContain(", null");
            builder.that(t.get(4).get(12)).doesNotContain(", null");
            builder.that(t.get(5).get(12)).doesNotContain(", null");
        });

        // Update gsi partition key (with type timestamp not null) to null
        // Update primary partition key to original value
        // Specify condition of primary table partition key so that SELECT part can be pushdown
        sql = "UPDATE " + tableName
            + " SET c1 = '" + timestampValue + "' + null, c2 = '" + timestampValue + "'"
            + " WHERE c3 = 3 AND c2 = '" + timestampValue + "' ;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(4), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains(", IFNULL(?,");
            builder.that(t.get(1).get(12)).doesNotContain(", null");
            builder.that(t.get(2).get(12)).doesNotContain(", null");
            builder.that(t.get(3).get(12)).doesNotContain(", null");
        });

        // Update ordinary column (without type timestamp not null)
        // Specify condition of primary table partition key so that SELECT part can be pushdown
        sql = "UPDATE " + tableName
            + " SET c4 = 10086"
            + " WHERE c3 = 4 AND c2 = '" + timestampValue + "' ;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(5), (t, builder) -> {
            builder.that(t.get(0).get(11)).doesNotContain(", null");
            builder.that(t.get(1).get(12)).doesNotContain(", null");
            builder.that(t.get(2).get(12)).doesNotContain(", null");
            builder.that(t.get(3).get(12)).doesNotContain(", null");
            builder.that(t.get(4).get(12)).doesNotContain(", null");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4, c5 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("c1", "c2", "c3", "c4", "c5"),
            ImmutableList.of("c2", "c1"),
            polardbxConnection);

        gsiIntegrityCheck(polardbxConnection, tableName, gsiName);
    }

    @Test
    public void dynamicImplicitDefaultWithMultiTableUpdateTest() throws SQLException, InterruptedException {
        final String tableName1 = "t10_dynamic_implicit_default_multi_update";
        final String tableName2 = "t11_dynamic_implicit_default_multi_update";
        final String timestampValue = "2024-11-25 17:49:57.84679";

        // Init table1
        JdbcUtil.dropTable(polardbxConnection, tableName1);
        String sqlCreateTable = "CREATE TABLE `" + tableName1 + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` bigint NOT NULL ,\n"
            + "    `c3` varchar(32) NOT NULL ,\n"
            + "    `c4` bigint NULL DEFAULT NULL \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c2`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);
        String sql = "INSERT INTO " + tableName1
            + " VALUES('" + timestampValue + "', 1, 1, 1),"
            + "('" + timestampValue + "', 2, 2, 2),"
            + "('" + timestampValue + "', 3, 3, 3),"
            + "('" + timestampValue + "', 4, 4, 4);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Init table2
        JdbcUtil.dropTable(polardbxConnection, tableName2);
        sqlCreateTable = "CREATE TABLE `" + tableName2 + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` bigint NOT NULL ,\n"
            + "    `c3` varchar(32) NOT NULL ,\n"
            + "    `c4` bigint NULL DEFAULT NULL \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c1`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        sql = "INSERT INTO " + tableName2
            + " VALUES('" + timestampValue + "', 1, 1, 1),"
            + "('" + timestampValue + "', 2, 2, 2),"
            + "('" + timestampValue + "', 3, 3, 3),"
            + "('" + timestampValue + "', 4, 4, 4),"
            + "('" + timestampValue + "', 5, 5, 5),"
            + "('" + timestampValue + "', 6, 6, 6);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Implicitly update partition key (with type timestamp not null on update current_timestamp)
        // Do not support modify partition key with multi-table update
        sql = "UPDATE " + tableName1 + " a join " + tableName2
            + " b on a.c2 = b.c2 SET a.c1 = null, b.c2 = null WHERE a.c2 = 1;";
        JdbcUtil.executeUpdateFailed(polardbxConnection, sql,
            "Column 'c1' is a sharding key of table '" + tableName2 + "', which is forbidden to be modified");

        // Directly update partition key (with type timestamp not null on update current_timestamp)
        // Do not support modify partition key with multi-table update
        sql = "UPDATE " + tableName1 + " a join " + tableName2
            + " b on a.c2 = b.c2 SET a.c1 = null, b.c1 = null WHERE a.c2 = 1;";
        JdbcUtil.executeUpdateFailed(polardbxConnection, sql,
            "Column 'c1' is a sharding key of table '" + tableName2 + "', which is forbidden to be modified");

        // Update ordinary column (with type timestamp not null) to null with multi-table update
        sql = "UPDATE " + tableName1 + " a join " + tableName1 + " b on a.c1 = b.c1 and a.c2 = b.c2"
            + " SET a.c1 = null"
            + " WHERE b.c2 = 1 AND b.c1 = '" + timestampValue + "' ;";
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(1), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains("`c1` = NULL");
        });

        // Update ordinary column (partition column is on update current_timestamp) with multi-table update
        sql = "UPDATE " + tableName2 + " a join " + tableName2 + " b on a.c1 = b.c1 and a.c2 = b.c2"
            + " SET a.c2 = 10086"
            + " WHERE b.c2 = 1 AND b.c1 = '" + timestampValue + "' ;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(0).get(11)).doesNotContain(", NULL");
            builder.that(t.get(2).get(12)).doesNotContain("NULL");
        });

        // Update partition column (with type timestamp not null) to null with multi-table update
        sql = "UPDATE " + tableName2 + " a join " + tableName2 + " b on a.c1 = b.c1 and a.c2 = b.c2"
            + " SET a.c1 = '" + timestampValue + "' + null"
            + " WHERE b.c2 = 2 AND b.c1 = '" + timestampValue + "' ;";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains("IFNULL((? + NULL)");
            builder.that(t.get(2).get(12)).doesNotContain("NULL");
        });

        // Update ordinary column (partition column is on update current_timestamp) with multi-table update
        sql = "UPDATE " + tableName2 + " a join ("
            + "select b.c1, b.c2 from " + tableName2 + " b WHERE b.c2 = 3 AND b.c1 = '" + timestampValue + "' ) c "
            + "on a.c1 = c.c1 and a.c2 = c.c2 SET a.c2 = 10086";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(0).get(11)).doesNotContain("NULL");
            builder.that(t.get(2).get(12)).doesNotContain("NULL");
        });

        // Update partition column (with type timestamp not null) to null with multi-table update
        sql = "UPDATE " + tableName2 + " a join ("
            + "select b.c1, b.c2 from " + tableName2 + " b WHERE b.c2 = 4 AND b.c1 = '" + timestampValue + "' ) c "
            + "on a.c1 = c.c1 and a.c2 = c.c2 "
            + " SET a.c1 = '" + timestampValue + "' + null";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains("IFNULL((? + NULL)");
            builder.that(t.get(2).get(12)).doesNotContain("NULL");
        });

        // Update partition column (with type timestamp not null) to null with multi-table update
        sql = String.format("update %s a "
            + "join (select 5 as c2, null as c3 "
            + "union select 6 as c2, 2 + null as c3 ) c on a.c2 = c.c2 and a.c1 = '%s' "
            + "set a.c1 = c.c3 ;", tableName2, timestampValue);
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(2);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(2).get(12)).doesNotContain("NULL");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult2 = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4 from `" + tableName1 + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName1,
            allResult2,
            ImmutableList.of("c1", "c2", "c3", "c4"),
            ImmutableList.of("c2", "c1"),
            polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultWithUpdateBroadcastTest() throws SQLException, InterruptedException {
        final String tableName = "t12_dynamic_implicit_default_broadcast";
        final String timestampValue = "2024-11-25 17:49:57.84679";

        // Init table1
        JdbcUtil.dropTable(polardbxConnection, tableName);
        String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` bigint NOT NULL ,\n"
            + "    `c3` varchar(32) NOT NULL ,\n"
            + "    `c4` bigint NULL DEFAULT NULL \n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "BROADCAST";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);
        String sql = "INSERT INTO " + tableName
            + " VALUES('" + timestampValue + "', 1, 1, 1),"
            + "('" + timestampValue + "', 2, 2, 2),"
            + "('" + timestampValue + "', 3, 3, 3),"
            + "('" + timestampValue + "', 4, 4, 4);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(polardbxConnection, tableName);

        // Update ordinary column (with type timestamp not null) to null
        sql = String.format("UPDATE %s SET c1 = null WHERE c2 = 1;", tableName);
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 1), (t, builder) -> {
            final int bound = topology.size();
            for (int i = 0; i < bound; i++) {
                builder.that(t.get(i).get(12)).doesNotContain("NULL");
            }
        });

        // Update ordinary column (with type timestamp not null) with non-null function
        sql = String.format("UPDATE %s SET c1 = current_timestamp(6) WHERE c2 = 2;", tableName);
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 1), (t, builder) -> {
            builder.that(t.get(0).get(11)).doesNotContain("IFNULL(");
            final int bound = topology.size();
            for (int i = 1; i < bound; i++) {
                builder.that(t.get(i).get(12)).doesNotContain("NULL");
            }
        });

        // Update ordinary column (with type timestamp not null) to null
        sql = String.format("UPDATE %s SET c1 = now() + null WHERE c2 = 3;", tableName);
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 1), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains("IFNULL(");
            final int bound = topology.size();
            for (int i = 1; i < bound; i++) {
                builder.that(t.get(i).get(12)).doesNotContain("NULL");
            }
        });

        // Update ordinary column
        sql = String.format("UPDATE %s SET c3 = 10086 WHERE c2 = 4;", tableName);
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(1);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 1), (t, builder) -> {
            builder.that(t.get(0).get(11)).contains("CURRENT_TIMESTAMP");
            final int bound = topology.size();
            for (int i = 1; i < bound; i++) {
                builder.that(t.get(i).get(12)).doesNotContain("NULL");
            }
        });

        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
    }

    @Ignore
    @Test
    public void dynamicImplicitDefaultBroadcastTest() throws InterruptedException {
        final String tableName = "t13_dynamic_implicit_default_broadcast";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint NOT NULL DEFAULT 0,\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
            + "  ) broadcast";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        String sql = "INSERT INTO " + tableName + " VALUES();";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(1, DEFAULT, DEFAULT);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(2, DEFAULT(c1), DEFAULT(c2));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(3, DEFAULT(c1), DEFAULT(c1));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(4, DEFAULT(c2), DEFAULT(c2));";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(5, null, c1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(6, c2, null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " VALUES(7, null + null, now() + null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        int count = 6;
        do {
            // make sure the timestamp is different from upper statements
            TimeUnit.MILLISECONDS.sleep(100);
            sql = "INSERT INTO " + tableName + " VALUES(8, null, null);";
            JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        } while (count-- > 0);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(polardbxConnection, tableName);

        // Replace one, replace all
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT INTO " + tableName + " VALUES(9, null, null);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.is(topology.size()),
            (t, builder) -> builder.that(t.get(0).get(12)).doesNotContain("null"));

        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
        checkColumnDataSame(tableName, polardbxConnection, "(round(DATE_FORMAT(c1, '%s.%f')) % 60)",
            "(round(DATE_FORMAT(c2, '%s.%f')) % 60)", ImmutableList.of("id", "c1", "c2"));
    }

    @Test
    public void dynamicImplicitDefaultInsertSelectTest() throws SQLException, InterruptedException {
        final String tableName = "t14_dynamic_implicit_default_insert_select";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` bigint NOT NULL ,\n"
            + "    `c3` varchar(32) NOT NULL ,\n"
            + "    `c4` bigint NULL DEFAULT NULL ,\n"
            + "    `c5` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \n"
            + "  ) PARTITION BY KEY(`c1`) PARTITIONS 8";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        String sql = "INSERT IGNORE INTO " + tableName + " VALUES();";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " SELECT default, c2, c3, c4, null from " + tableName + ";";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " SELECT default, c2, c3, c4, null from " + tableName + ";";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " SELECT NULL, c2, c3, c4, null from " + tableName + ";";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " SELECT 1 + NULL, c2, c3, c4, null from " + tableName + ";";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName + " SELECT now() + NULL, c2, c3, c4, null from " + tableName + ";";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        int count = 6;
        do {
            // make sure the timestamp is different from upper statements
            TimeUnit.MILLISECONDS.sleep(100);
            sql = "INSERT INTO " + tableName + " SELECT null, c2, c3, c4, c5 from " + tableName + " LIMIT 4;";
            JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        } while (count-- > 0);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(polardbxConnection, tableName);

        // Replace one, replace all
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT INTO " + tableName + " select null, 10086, 10086, 10086, null from " + tableName
            + " limit 1";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.greaterThanOrEqualTo(2),
            (t, builder) -> {

                builder.that(t.get(t.size() - 2).get(12)).doesNotContain("null");
                builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null");
            });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult2 = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4, c5 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult2,
            ImmutableList.of("c1", "c2", "c3", "c4", "c5"),
            ImmutableList.of("c2", "c1"),
            polardbxConnection);
    }

    @Test
    public void staticImplicitDefaultInsertIgnoreSelectTest() throws SQLException, InterruptedException {
        final String tableName = "t15_static_implicit_default_insert_ignore_select";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `c1` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "  `c3` bigint NOT NULL ,\n"
            + "  `c4` varchar(32) NOT NULL \n"
            + "  ) partition by hash(c3) partitions 8;\n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        String sql = "INSERT IGNORE INTO " + tableName + " VALUES()";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " SELECT DEFAULT, DEFAULT, DEFAULT, DEFAULT FROM " + tableName;
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT IGNORE INTO " + tableName + " SELECT null, null, null, null FROM " + tableName;
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Replace necessary column only
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT IGNORE INTO " + tableName + " SELECT null, null, null, null FROM " + tableName
            + " LIMIT 1";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.greaterThanOrEqualTo(2),
            (t, builder) -> builder.that(t.get(t.size() - 1).get(12)).contains("null, null, 0, null"));

        // c1 is timestamp type and not partition key, do not replace, and no exception for insert
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT INTO " + tableName + "(c1) SELECT null FROM " + tableName + " LIMIT 1";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.greaterThanOrEqualTo(2),
            (t, builder) -> builder.that(t.get(t.size() - 1).get(12)).contains("null"));

        // c3 is partition key, do replace for insert ignore and insert
        // Check explanation in com.alibaba.polardbx.optimizer.core.rel.LogicalInsert.initImplicitDefaultColumnIndexMap
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace INSERT INTO " + tableName + "(c3) SELECT null FROM " + tableName + " LIMIT 1";
//            JdbcUtil.executeUpdateFailed(polardbxConnection, sql, "Column 'c3' cannot be null");
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.greaterThanOrEqualTo(2),
            (t, builder) -> builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null"));
        sql = "trace INSERT IGNORE INTO " + tableName + "(c3) SELECT null FROM " + tableName + " LIMIT 1";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkTrace(polardbxConnection,
            Matchers.greaterThanOrEqualTo(2),
            (t, builder) -> builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null"));

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult =
            JdbcUtil.getAllResult(
                JdbcUtil.executeQuery("select c1, c2, c3, c4 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("c1", "c2", "c3", "c4"),
            ImmutableList.of("c2", "c3"),
            polardbxConnection);

        checkColumnDataSame(tableName, polardbxConnection, "c1", "c2", ImmutableList.of("c1", "c2"));
    }

    @Test
    public void dynamicImplicitDefaultUpsertTest() throws SQLException, InterruptedException {
        final String tableName = "t16_dynamic_implicit_default_upsert";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
            + "  ) partition by hash(c2) partitions 2;\n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        final String timestamp = "2025-01-24 16:03:49.000000";
        final String initDataSql =
            buildCmdExtra("DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true", "DML_EXECUTION_STRATEGY=LOGICAL",
                "PRIMARY_KEY_CHECK=TRUE") + String.format("INSERT IGNORE INTO %s VALUES(1, NULL, '%s');", tableName,
                timestamp);
        final String baseSql = buildCmdExtra("DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true") +
            String.format("INSERT INTO %s VALUES(1, NULL, '%s')", tableName, timestamp);

        // make sure the timestamp is different from upper statements
        JdbcUtil.executeUpdateSuccess(polardbxConnection, initDataSql);
        TimeUnit.MILLISECONDS.sleep(100);
        String sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = NULL;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        JdbcUtil.executeUpdateSuccess(polardbxConnection, initDataSql);
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = VALUES(c1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        JdbcUtil.executeUpdateSuccess(polardbxConnection, initDataSql);
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = DATE_ADD(VALUES(c1), INTERVAL 3 DAY);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // make sure the timestamp is different from upper statements
        JdbcUtil.executeUpdateSuccess(polardbxConnection, initDataSql);
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = c2 + NULL;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Replace one, replace all
        JdbcUtil.executeUpdateSuccess(polardbxConnection, initDataSql);
        TimeUnit.MILLISECONDS.sleep(100);
        sql = "trace " + baseSql + " ON DUPLICATE KEY UPDATE c2 = c2 + null, c1 = null;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        final List<String> topology = showTopology(polardbxConnection, tableName);
        checkTrace(polardbxConnection,
            Matchers.is(topology.size() + 2),
            (t, builder) -> {
                builder.that(t.get(t.size() - 2).get(12)).doesNotContain("null");
                builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null");
            });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult =
            JdbcUtil.getAllResult(
                JdbcUtil.executeQuery("select id, c1, c2 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("id", "c1", "c2"),
            ImmutableList.of("c1", "c2"),
            polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultUpsertWithGsiTest() throws SQLException, InterruptedException {
        final String tableName = "t17_dynamic_implicit_default_upsert_with_gsi";
        final String gsiName = "g_c4";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "    `id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
            + "    `c1` timestamp(6) NOT NULL,\n"
            + "    `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "    `c3` bigint NOT NULL ,\n"
            + "    `c4` varchar(32) NOT NULL ,\n"
            + "    `c5` bigint NULL DEFAULT NULL, \n"
            + "    global index `" + gsiName + "`(c4) covering(c2) partition by key(c4) partitions 8\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c3`)\n" + "PARTITIONS 8 \n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);
        final List<String> topology = showTopology(polardbxConnection, tableName);

        final String timestamp = "2025-01-24 16:03:49.000000";
        final String initDataSql =
            buildCmdExtra("PRIMARY_KEY_CHECK=TRUE")
                + String.format("INSERT IGNORE INTO %s VALUES(1, NULL, '%s', 1, 1, 1);", tableName, timestamp);
        final String baseSql = String.format("INSERT INTO %s VALUES(1, NULL, '%s', 2, 2, 2)", tableName, timestamp);

        JdbcUtil.executeUpdateSuccess(polardbxConnection, initDataSql);
        TimeUnit.MILLISECONDS.sleep(100);
        String sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = NULL;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 2), (t, builder) -> {
            builder.that(t.get(t.size() - 2).get(12)).doesNotContain("null");
            builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null");
        });

        TimeUnit.MILLISECONDS.sleep(100);
        sql = "INSERT INTO " + tableName
            + " VALUES(1, null, null, 3, 3, 3), (1, '2024-11-19 17:09:25', '2024-11-19 17:09:25', 4, 4, 4)"
            + " ON DUPLICATE KEY UPDATE c1 = NULL, c2 = VALUES(c2), c3 = values(c3), c4 = values(c4);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 2 + 2), (t, builder) -> {
            builder.that(t.get(t.size() - 2).get(12)).doesNotContain("null");
            builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null");
            builder.that(t.get(t.size() - 2).get(12)).contains("4, 4");
            builder.that(t.get(t.size() - 1).get(12)).contains("4, 4");
            builder.that(t.get(t.size() - 2).get(12)).contains("2024-11-19 17:09:25");
            builder.that(t.get(t.size() - 1).get(12)).contains("2024-11-19 17:09:25");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = VALUES(c1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 2), (t, builder) -> {
            builder.that(t.get(t.size() - 2).get(12)).doesNotContain("null");
            builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = DATE_ADD(VALUES(c1), INTERVAL 3 DAY);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 2), (t, builder) -> {
            builder.that(t.get(t.size() - 2).get(12)).doesNotContain("null");
            builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null");
        });

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = c2 + NULL, c1 = null;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        checkTrace(polardbxConnection, Matchers.is(topology.size() + 2), (t, builder) -> {
            builder.that(t.get(t.size() - 2).get(12)).doesNotContain("null");
            builder.that(t.get(t.size() - 1).get(12)).doesNotContain("null");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2, c3, c4, c5 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult,
            ImmutableList.of("c1", "c2", "c3", "c4", "c5"),
            ImmutableList.of("c3", "c4"),
            polardbxConnection);

        gsiIntegrityCheck(polardbxConnection, tableName, gsiName);
    }

    @Test
    public void dynamicImplicitDefaultUpsertBroadcastTest() throws InterruptedException {
        final String tableName = "t18_dynamic_implicit_default_upsert_broadcast";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
            + "  ) broadcast";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        final String timestamp = "2025-01-24 16:03:49.000000";
        final String initDataSql = String.format("INSERT IGNORE INTO %s VALUES(1, NULL, '%s');", tableName, timestamp);
        final String baseSql = String.format("INSERT INTO %s VALUES(1, NULL, '%s')", tableName, timestamp);

        JdbcUtil.executeUpdateSuccess(polardbxConnection, initDataSql);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        String sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = NULL;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = VALUES(c1);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = baseSql + " ON DUPLICATE KEY UPDATE c2 = DATE_ADD(VALUES(c1), INTERVAL 3 DAY);";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);

        // make sure the timestamp is different from upper statements
        TimeUnit.MILLISECONDS.sleep(100);
        sql = buildCmdExtra("DML_CHECK_UPSERT_DYNAMIC_IMPLICIT_WITH_COLUMN_REF=TRUE") + baseSql
            + " ON DUPLICATE KEY UPDATE c2 = c2 + NULL;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);

        // Replace one, replace all
        TimeUnit.MILLISECONDS.sleep(100);
        sql = buildCmdExtra("DML_CHECK_UPSERT_DYNAMIC_IMPLICIT_WITH_COLUMN_REF=TRUE") + baseSql
            + " ON DUPLICATE KEY UPDATE c2 = c2 + null, c1 = null;";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, "trace " + sql);
        final List<String> topology = showTopology(polardbxConnection, tableName);
        checkTrace(polardbxConnection,
            Matchers.is(topology.size() + 1),
            (t, builder) -> {
                for (int i = 1; i < topology.size() + 1; i++) {
                    builder.that(t.get(i).get(12)).doesNotContain("null");
                }
            });
        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultUpsertBroadcastTest1() throws SQLException, InterruptedException {
        final String tableName = "t19_dynamic_implicit_default_upsert_broadcast";
        JdbcUtil.dropTable(mysqlConnection, tableName);
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
            + "  )";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sqlCreateTable);
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable + " broadcast");

        final String baseSql = String.format(
            "INSERT INTO %s VALUES(1, '2025-01-24 16:03:49.000000', '2025-02-24 16:03:49.000000'), (2, '2025-01-25 16:03:49.000000', '2025-02-25 16:03:49.000000')",
            tableName);
        String sql = baseSql + " ON DUPLICATE KEY UPDATE c1 = values(c1), c2 = '2025-03-24 16:03:49.000000';";

        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sql, null);
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sql, null);

        for (int i = 1; i < 3; i++) {
            sql = String.format("select * from %s where id=%d", tableName, i);
            selectStringContentSameAssert(sql, sql, null, mysqlConnection, polardbxConnection, false, true);
        }

        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultUpsertBroadcastTest2() {
        final String tableName = "t20_dynamic_implicit_default_upsert_broadcast";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "  `c3` varchar(32) NULL \n"
            + "  ) broadcast";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        final String baseSql = String.format(
            "INSERT INTO %s VALUES(1, '2025-01-24 16:03:49.000000', '2025-02-24 16:03:49.000000', null), (2, '2025-01-25 16:03:49.000000', '2025-02-25 16:03:49.000000', '2025-01-25 17:03:49.000000')",
            tableName);
        String sql = buildCmdExtra("DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO=true") + baseSql
            + " ON DUPLICATE KEY UPDATE c1 = values(c3), c2 = values(c3);";

        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
        checkColumnDataSame(tableName, polardbxConnection, "(cast(c1 as datetime(0)))",
            "(cast(c1 as datetime(0)))", ImmutableList.of("c1", "c2"));
    }

    @Test
    public void dynamicImplicitDefaultUpsertBroadcastTest3() {
        final String tableName = "t21_dynamic_implicit_default_upsert_broadcast";
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
            + "  `c1` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "  `c3` varchar(32) NULL \n"
            + "  ) broadcast";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable);

        // current_timestamp(6)
        final String baseSql = String.format(
            "INSERT INTO %s VALUES(1, '2025-01-24 16:03:49.000000', '2025-02-24 16:03:49.000000', null)",
            tableName);
        String sql = buildCmdExtra("DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO=true") + baseSql
            + " ON DUPLICATE KEY UPDATE c1 = current_timestamp(6), c2 = values(c3);";

        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
        checkColumnDataSame(tableName, polardbxConnection, "c1", "c2", ImmutableList.of("c1", "c2"));

        // current_timestamp(4)
        sql = buildCmdExtra("DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO=true") + baseSql
            + " ON DUPLICATE KEY UPDATE c1 = current_timestamp(4), c2 = values(c3);";

        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
        checkColumnDataSame(tableName, polardbxConnection, "(cast(c1 as datetime(4)))",
            "(cast(c1 as datetime(4)))", ImmutableList.of("c1", "c2"));
    }

    @Test
    public void dynamicImplicitDefaultMultiValueBatchInsertTest() {
        final String tableName = "t22_dynamic_implicit_default_multi_value_batch_insert";
        JdbcUtil.dropTable(mysqlConnection, tableName);
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
            + "  `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `c2` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "  `c3` varchar(16) DEFAULT NULL\n"
            + "  )";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sqlCreateTable);
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable + " broadcast");

        String sql = String.format(
            "INSERT INTO %s VALUES (1 + 1, now() + null, null, 'a'),(1 + 2, current_timestamp(4), '2025-02-24 16:03:49.000000', 'b')",
            tableName);

        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sql, null);

        for (int i = 2; i < 4; i++) {
            sql = String.format("select * from %s where id=%d", tableName, i);
            selectContentSameAssert(sql, null, mysqlConnection, polardbxConnection, false, true);
        }

        checkBroadcastTableDataIntegrity(tableName, polardbxConnection);
    }

    @Test
    public void dynamicImplicitDefaultUpsertSelectTest() {
        final String tableName = "t23_dynamic_implicit_default_upsert_select";
        final String srcTableName = "t23_dynamic_implicit_default_upsert_select_src";
        JdbcUtil.dropTable(mysqlConnection, tableName);
        JdbcUtil.dropTable(polardbxConnection, tableName);
        JdbcUtil.dropTable(mysqlConnection, srcTableName);
        JdbcUtil.dropTable(polardbxConnection, srcTableName);

        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "`(\n"
            + "\t`pk` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\t`id` int(11) DEFAULT NULL,\n"
            + "\t`a` int(11) DEFAULT '1',\n"
            + "\t`b` int(11) DEFAULT '2',\n"
            + "\t`ts` timestamp(6) NOT NULL DEFAULT '2022-10-10 12:00:00.000000' ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "\tPRIMARY KEY (`pk`),\n"
            + "\tKEY `auto_shard_key_a` USING BTREE (`a`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sqlCreateTable);
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable + " PARTITION BY KEY(`a`) PARTITIONS 3");

        final String sqlCreateSrcTable = "CREATE TABLE IF NOT EXISTS `" + srcTableName + "`(\n"
            + "\t`pk` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\t`id` int(11) DEFAULT NULL,\n"
            + "\t`a` int(11) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sqlCreateSrcTable);
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateSrcTable);

        final String sqlInitData =
            "insert into " + srcTableName + " (pk, id, a) values(1, 100, 101), (2, 101, 102), (3, 102, 103);";
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInitData, null);

        final String upsertSelect =
            buildCmdExtra(DML_EXECUTION_STRATEGY_LOGICAL) + String.format("insert into %s (pk,id,a) "
                    + "select pk, id, a from %s where id >= 100 order by id "
                    + "on duplicate key update id=%s.id+10, ts=null;",
                tableName, srcTableName, tableName);

        String sql = null;
        // Do INSERT
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, upsertSelect, null);

        for (int i = 1; i < 4; i++) {
            sql = String.format("select pk, id, a, b, ts from %s where pk=%d", tableName, i);
            selectContentSameAssert(sql, null, mysqlConnection, polardbxConnection, false, true);
        }

        // check insert result
        for (int i = 2; i < 4; i++) {
            final String sql1 = String.format("select ts from %s where pk=%d", tableName, i - 1);
            final String sql2 = String.format("select ts from %s where pk=%d", tableName, i);
            selectStringContentSameAssert(sql1, sql2, null, polardbxConnection, polardbxConnection, false, true);
        }

        // Do UPSERT with DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO=true
        executeOnMysqlAndTddl(mysqlConnection,
            polardbxConnection,
            upsertSelect,
            "trace " + buildCmdExtra(ENABLE_DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO) + upsertSelect,
            null,
            true);

        checkTrace(polardbxConnection,
            Matchers.greaterThanOrEqualTo(2),
            (t, builder) -> {
                for (int i = 0; i < t.size(); i++) {
                    if (TStringUtil.containsIgnoreCase(t.get(i).get(11), "UPDATE")) {
                        builder.that(t.get(i).get(12)).doesNotContain("null");
                    }
                }
            });

        for (int i = 1; i < 4; i++) {
            sql = String.format("select pk, id, a, b from %s where pk=%d", tableName, i);
            selectContentSameAssert(sql, null, mysqlConnection, polardbxConnection, false, true);
        }

        // check DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO
        for (int i = 2; i < 4; i++) {
            final String sql1 = String.format("select ts from %s where pk=%d", tableName, i - 1);
            final String sql2 = String.format("select ts from %s where pk=%d", tableName, i);
            selectStringContentSameAssert(sql1, sql2, null, polardbxConnection, polardbxConnection, false, true);
        }

        // Do UPSERT with DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO=false
        executeOnMysqlAndTddl(mysqlConnection,
            polardbxConnection,
            upsertSelect,
            "trace " + buildCmdExtra(DISABLE_DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO) + upsertSelect,
            null,
            true);

        checkTrace(polardbxConnection,
            Matchers.greaterThanOrEqualTo(2),
            (t, builder) -> {
                for (int i = 0; i < t.size(); i++) {
                    if (TStringUtil.containsIgnoreCase(t.get(i).get(11), "UPDATE")) {
                        builder.that(t.get(i).get(12)).doesNotContain("null");
                    }
                }
            });

        for (int i = 1; i < 4; i++) {
            sql = String.format("select pk, id, a, b from %s where pk=%d", tableName, i);
            selectContentSameAssert(sql, null, mysqlConnection, polardbxConnection, false, true);
        }
    }

    @Test
    public void dynamicImplicitDefaultBinaryStringTest() {
        final String tableName = "t24_dynamic_implicit_default_binary_string";
        JdbcUtil.dropTable(mysqlConnection, tableName);
        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "\t`a` bigint(20) UNSIGNED NOT NULL,\n"
            + "\t`b` datetime NOT NULL,\n"
            + "\t`c` varchar(128) CHARACTER SET utf8 NOT NULL\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sqlCreateTable);
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable + " PARTITION BY RANGE COLUMNS(`c`)\n"
            + "(PARTITION `p0` VALUES LESS THAN ('世界') ENGINE = InnoDB,\n"
            + " PARTITION `p1` VALUES LESS THAN ('世界人民') ENGINE = InnoDB,\n"
            + " PARTITION `p2` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB);\n");

        final String sqlInsertPlainString =
            "insert into " + tableName
                + "(a,b,c) values (1,'2012-12-11 23:00:00', '世界'),(2,'2012-12-12 01:00:00','世界人民')";
        final String sqlInsertBinaryString =
            "insert into " + tableName
                + "(a,b,c) values (3,'2012-12-11 23:00:00', x'E4B896E7958C'),(4,'2012-12-12 01:00:00', x'E4B896E7958CE4BABAE6B091')";

        final String checkSql1 = String.format("select * from %s where c='世界'", tableName);
        final String checkSql2 = String.format("select * from %s where c='世界人民'", tableName);

        // insert with binary string then insert with plain string
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertBinaryString, null);
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertPlainString, null);

        selectStringContentSameAssert(checkSql1, checkSql1, null, mysqlConnection, polardbxConnection, false, true);
        selectStringContentSameAssert(checkSql2, checkSql2, null, mysqlConnection, polardbxConnection, false, true);

        // clear data and plan cache
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection,
            String.format("delete from %s where 1=1", tableName), null);
        JdbcUtil.executeSuccess(polardbxConnection, "clear plancache");

        // insert with plain string then insert with binary string
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertPlainString, null);
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertBinaryString, null);

        selectStringContentSameAssert(checkSql1, checkSql1, null, mysqlConnection, polardbxConnection, false, true);
        selectStringContentSameAssert(checkSql2, checkSql2, null, mysqlConnection, polardbxConnection, false, true);

        final String sqlInsertWithoutColumnPlainString =
            "insert into " + tableName
                + " values (1,'2012-12-11 23:00:00', '世界'),(2,'2012-12-12 01:00:00','世界人民')";
        final String sqlInsertWithoutColumnBinaryString =
            "insert into " + tableName
                + " values (3,'2012-12-11 23:00:00', x'E4B896E7958C'),(4,'2012-12-12 01:00:00', x'E4B896E7958CE4BABAE6B091')";

        // insert with binary string then insert with plain string
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertWithoutColumnBinaryString, null);
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertWithoutColumnPlainString, null);

        selectStringContentSameAssert(checkSql1, checkSql1, null, mysqlConnection, polardbxConnection, false, true);
        selectStringContentSameAssert(checkSql2, checkSql2, null, mysqlConnection, polardbxConnection, false, true);

        // clear data and plan cache
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection,
            String.format("delete from %s where 1=1", tableName), null);
        JdbcUtil.executeSuccess(polardbxConnection, "clear plancache");

        // insert with plain string then insert with binary string
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertWithoutColumnPlainString, null);
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlInsertWithoutColumnBinaryString, null);

        selectStringContentSameAssert(checkSql1, checkSql1, null, mysqlConnection, polardbxConnection, false, true);
        selectStringContentSameAssert(checkSql2, checkSql2, null, mysqlConnection, polardbxConnection, false, true);

        final String sqlReplacePlainString =
            "replace into " + tableName
                + "(a,b,c) values (1,'2012-12-11 23:00:00', '世界'),(2,'2012-12-12 01:00:00','世界人民')";
        final String sqlReplaceBinaryString =
            "replace into " + tableName
                + "(a,b,c) values (3,'2012-12-11 23:00:00', x'E4B896E7958C'),(4,'2012-12-12 01:00:00', x'E4B896E7958CE4BABAE6B091')";

        // replace with binary string then replace with plain string
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlReplaceBinaryString, null);
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlReplacePlainString, null);

        selectStringContentSameAssert(checkSql1, checkSql1, null, mysqlConnection, polardbxConnection, false, true);
        selectStringContentSameAssert(checkSql2, checkSql2, null, mysqlConnection, polardbxConnection, false, true);

        // clear data and plan cache
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection,
            String.format("delete from %s where 1=1", tableName), null);
        JdbcUtil.executeSuccess(polardbxConnection, "clear plancache");

        // replace with plain string then replace with binary string
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlReplacePlainString, null);
        executeBatchOnMysqlAndTddl(mysqlConnection, polardbxConnection, sqlReplaceBinaryString, null);

        selectStringContentSameAssert(checkSql1, checkSql1, null, mysqlConnection, polardbxConnection, false, true);
        selectStringContentSameAssert(checkSql2, checkSql2, null, mysqlConnection, polardbxConnection, false, true);
    }

    @Test
    public void implicitDefaultWithUpdateTest1() throws SQLException {
        final String tableName = "t25_implicit_default_update";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` varchar(32) DEFAULT NULL,\n"
            + "\t`c2` bigint NOT NULL,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4\n";
        final String partition = "PARTITION BY KEY(`c2`) PARTITIONS 8;\n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable + partition);

        String sql = "INSERT INTO " + tableName + "(c1, c2) values(1, 1),(2, 1),(3, 1),(4,2),(5,2)";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sql);

        // Modify partition column with type 'bigint not null' and set column value to null
        // Replace null with 0 and route correctly
        sql = "UPDATE " + tableName + " SET c2 = null WHERE c2 = 1";
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(3);
        checkTrace(polardbxConnection, Matchers.is(3), (t, builder) -> {
            builder.that(t.get(1).get(12)).doesNotContain(", null");
            builder.that(t.get(2).get(12)).doesNotContain(", null");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult1 = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult1,
            ImmutableList.of("c1", "c2"),
            ImmutableList.of("c2"),
            polardbxConnection);
    }

    @Test
    public void implicitDefaultWithInsertTest1() throws SQLException, InterruptedException {
        final String tableName = "t26_implicit_default_insert";

        JdbcUtil.dropTable(polardbxConnection, tableName);

        final String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n"
            + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` varchar(32) DEFAULT NULL,\n"
            + "\t`c2` bigint NOT NULL,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4\n";
        final String partition = "PARTITION BY KEY(`c2`) PARTITIONS 8;\n";
        JdbcUtil.executeUpdateSuccess(polardbxConnection, sqlCreateTable + partition);

        String sql = "INSERT INTO " + tableName + "(c1, c2) values(1, 0),(2, null),(3, 0)";

        // Insert partition column with type 'bigint not null' and set column value to null
        // Replace null with 0 and route correctly
        int affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(3);
        checkTrace(polardbxConnection, Matchers.is(1), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain(", null");
        });

        // Insert IGNORE partition column with type 'bigint not null' and set column value to null
        // Replace null with 0 and route correctly
        sql = "INSERT IGNORE INTO " + tableName + "(c1, c2) values(1, 0),(2, null),(3, 0)";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(3);
        checkTrace(polardbxConnection, Matchers.is(1), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain(", null");
        });

        // Replace partition column with type 'bigint not null' and set column value to null
        // Replace null with 0 and route correctly
        sql = "REPLACE INTO " + tableName + "(c1, c2) values(1, 0),(2, null),(3, 0)";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(3);
        checkTrace(polardbxConnection, Matchers.is(1), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain(", null");
        });

        // Upsert partition column with type 'bigint not null' and set column value to null
        // Replace null with 0 and route correctly
        sql = "INSERT INTO " + tableName + "(c1, c2) values(1, 0),(2, null),(3, 0)"
            + " ON DUPLICATE KEY UPDATE c2 = null";
        affectedRows = JdbcUtil.executeUpdateAndGetEffectCount(polardbxConnection, "trace " + sql);
        Truth.assertWithMessage(sql).that(affectedRows).isEqualTo(3);
        checkTrace(polardbxConnection, Matchers.is(1), (t, builder) -> {
            builder.that(t.get(0).get(12)).doesNotContain(", null");
        });

        // Check null value is replaced with CURRENT_TIMESTAMP before partitioning
        final List<List<Object>> allResult1 = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("select c1, c2 from `" + tableName + "`", polardbxConnection));

        JdbcUtil.assertRouteCorrectness("",
            tableName,
            allResult1,
            ImmutableList.of("c1", "c2"),
            ImmutableList.of("c2"),
            polardbxConnection);
    }
}
