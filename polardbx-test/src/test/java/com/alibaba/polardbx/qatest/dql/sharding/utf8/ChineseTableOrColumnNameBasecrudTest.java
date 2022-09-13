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

package com.alibaba.polardbx.qatest.dql.sharding.utf8;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteCHNTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 表名和列名为utf-8字符串的基本的CRUD测试类
 *
 * @author arnkore 2016-07-18 09:56
 */

public class ChineseTableOrColumnNameBasecrudTest extends ReadBaseTestCase {
    private List<Integer> primaryKeys = Lists.newArrayList();

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareParameterData() {
        return Arrays.asList(ExecuteCHNTableName.allBaseTypeOneTable(ExecuteCHNTableName.UPDATE_DELETE_BASE));
    }

    public ChineseTableOrColumnNameBasecrudTest(String tableName) {
        baseOneTableName = tableName;
    }

    @Before
    public void init() throws Exception {

        if (baseOneTableName.startsWith("broadcast")) {
            JdbcUtil.setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB, tddlConnection);
        }
        String sql = "delete from " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        prepareData();
    }

    private void prepareData() throws Exception {
        for (int i = 0; i < 15; i++) {
            primaryKeys.add(i + 1);
        }

        String sql = String.format(
            "insert into %s(编号, 姓名, 性别, 单号) values(?, ?, ?, ?)", baseOneTableName);
        for (int primaryKey : primaryKeys) {
            List<Object> params = Lists.newArrayList();
            params.add(primaryKey);
            params.add(RandomStringUtils.randomAlphabetic(8));
            params.add(RandomStringUtils.randomAlphabetic(1));
            params.add(primaryKey);

            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                sql, params);
        }
    }

    private int randomPrimaryKey() {
        int randomIndex = RandomUtils.nextInt(primaryKeys.size());
        return primaryKeys.get(randomIndex);
    }

    @Test
    public void insertTest() throws Exception {
        String sql = String.format("select * from %s where 编号 in (%s)",
            baseOneTableName, StringUtils.join(primaryKeys, ","));
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void deleteTest() throws Exception {
        String deleteSql = String.format("delete from %s where 编号 = ?",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            deleteSql, Arrays.asList(new Object[] {randomPrimaryKey()}), true);
    }

    @Test
    public void deleteAllTest() throws Exception {
        String deleteSql = String.format("delete from %s", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            deleteSql, null, true);
    }

    @Test
    public void updateTest() throws Exception {
        String updateSql = String.format(
            "update %s set 姓名 = ?, 性别 = ? where 编号 = ?", baseOneTableName);
        List<Object> params = Lists.newArrayList();
        params.add(RandomStringUtils.randomAlphabetic(8));
        params.add(RandomStringUtils.randomAlphabetic(1));
        params.add(randomPrimaryKey());
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            updateSql, params);
        String selectSql = String.format("select * from %s where 编号 in (%s)",
            baseOneTableName, StringUtils.join(primaryKeys, ","));
        selectContentSameAssert(selectSql, null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void updateAllTest() throws Exception {
        String updateSql = String.format(
            "update %s set 姓名 = ?, 性别 = ? where 编号 = ?", baseOneTableName);
        for (int primaryKey : primaryKeys) {
            List<Object> params = Lists.newArrayList();
            params.add(RandomStringUtils.randomAlphabetic(8));
            params.add(RandomStringUtils.randomAlphabetic(1));
            params.add(primaryKey);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                updateSql, params);
        }

        String selectSql = String.format("select * from %s where 编号 in (%s)",
            baseOneTableName, StringUtils.join(primaryKeys, ","));
        selectContentSameAssert(selectSql, null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void truncateTest() throws Exception {
        String sql = String.format("truncate table %s", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
    }

    @Test
    public void testShardKeyIsNull() {
        /*
         * !! 注意，
         *
         * 这个报错是因为拆分键是中文的原因，它的规则原本应该是简单规则（有判空处理），
         * 但因为中文的原因tddl将它识别为复杂规则(里边没有判空),
         *
         * 目前这个testCase在tddl-client之下忽略， client暂时不存在中文列的场景
         */
        String insertSql = String.format(
            "insert into %s (编号, 单号, 姓名) values(1, null, 'abc')",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertSql, null);

        String selectSql = String.format(
            "select * from %s where 单号 is null", baseOneTableName);
        selectContentSameAssert(selectSql, null,
            mysqlConnection, tddlConnection);

        String updateSql = String.format(
            "update %s set 姓名='中文姓名' where 单号 is null",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            updateSql, null);
        selectContentSameAssert(selectSql, null,
            mysqlConnection, tddlConnection);
    }
}
