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

import com.alibaba.polardbx.common.utils.GeneralUtil;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

public class DefaultValueTest extends CrudBasedLockTestCase {
    private static final String TABLE_NAME = "default_value_test";
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
    private static final String PARTITIONS_METHOD = "dbpartition by hash(pk) tbpartition by hash(pk) tbpartitions 2";
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

    @Before
    public void initData() throws Exception {

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, MessageFormat.format(CREAT_TABLE, ""));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREAT_TABLE, PARTITIONS_METHOD));
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

}
