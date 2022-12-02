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

package com.alibaba.polardbx.qatest.dql.sharding.functions;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.List;

public class KeywordsTest extends ReadBaseTestCase {
    private static final String TABLE_NAME = "keywords_test";
    private static final String CREAT_TABLE = "CREATE TABLE `" + TABLE_NAME + "` ("
        + " `pk` int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1` int(11) NULL,"
        + " `user` varchar(40) NOT NULL DEFAULT \"123\","
        + " `current_user` varchar(40) NOT NULL DEFAULT \"abc\","
        + " PRIMARY KEY (`pk`)"
        + ") {0} ";
    private static final String PARTITIONS_METHOD = "dbpartition by hash(pk) tbpartition by hash(pk) tbpartitions 2";

    @Before
    public void initData() throws Exception {

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, MessageFormat.format(CREAT_TABLE, ""));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREAT_TABLE, PARTITIONS_METHOD));
    }

    public void mysqlAndTddlContainsString(String sql, Connection mysqlCon, Connection tddlCon, String contain) {
        List<List<Object>> mysqlRes = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, mysqlCon));
        List<List<Object>> tddlRes = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlCon));

        //Assert.assertTrue(mysqlRes.size() == 1 && mysqlRes.get(0).size() == 1);
        Assert.assertTrue(mysqlRes.get(0).get(0).toString().contains(contain));
        //Assert.assertTrue(tddlRes.size() == 1 && tddlRes.get(0).size() == 1);
        Assert.assertTrue(tddlRes.get(0).get(0).toString().contains(contain));
    }

    @Test
    public void selectNoTableTest() {
        String sql = "select user";
        JdbcUtil.executeUpdateFailed(mysqlConnection, sql, "column");
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "column");

        sql = "select `user`";
        JdbcUtil.executeUpdateFailed(mysqlConnection, sql, "column");
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "column");

        sql = "select `current_user`";
        JdbcUtil.executeUpdateFailed(mysqlConnection, sql, "column");
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "column");

        sql = "select current_user";
        mysqlAndTddlContainsString(sql, mysqlConnection, tddlConnection, "@");

        sql = "select user()";
        mysqlAndTddlContainsString(sql, mysqlConnection, tddlConnection, "@");
    }

    @Test
    public void selectFromTableTest() {
        String sql =
            "insert into " + TABLE_NAME + "(c1, `user`, `current_user`) values(10, 'abc', 'efd'),(12, 'aa', 'bb')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select user from " + TABLE_NAME;
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        //函数current_user()
        sql = "select current_user from " + TABLE_NAME;
        mysqlAndTddlContainsString(sql, mysqlConnection, tddlConnection, "@");

        //列current_user
        sql = "select `current_user` from " + TABLE_NAME;
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void insertTableTest() {
        String sql = "insert into " + TABLE_NAME + "(c1, user) values(10, 'abc'),(12, 'aa')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "insert into " + TABLE_NAME + "(c1, `current_user`) values(10, 'abc'),(12, 'aa')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        //mysql 会报错
        sql = "insert into " + TABLE_NAME + "(c1, current_user) values(10, 'abc'),(12, 'aa')";
        JdbcUtil.executeUpdateFailed(mysqlConnection, sql, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void insertOnDuplicateKeyTest() {
        String sql = "insert into " + TABLE_NAME + "(pk, `user`, `current_user`) values(1, 'abc', 'efd')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "insert into " + TABLE_NAME + "(pk, `user`, `current_user`) values(1, 'aa', 'bb') " +
            "ON DUPLICATE KEY UPDATE user = 'cc', `current_user` = 'dd'";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        String selectSql = "select `user`,`current_user` from " + TABLE_NAME;
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = "insert into " + TABLE_NAME + "(pk, `user`, `current_user`) values(1, 'xx', 'yy') " +
            "ON DUPLICATE KEY UPDATE user = values(user)";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void updateTest() {
        String sql = "insert into " + TABLE_NAME + "(pk, `user`, `current_user`) values(1, 'abc', 'efd')";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "update " + TABLE_NAME + " set user = 'aa', `current_user` = 'bb'";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        String selectSql = "select `user`,`current_user` from " + TABLE_NAME;
        DataValidator.selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

    }
}

