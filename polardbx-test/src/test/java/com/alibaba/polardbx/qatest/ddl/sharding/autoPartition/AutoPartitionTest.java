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

package com.alibaba.polardbx.qatest.ddl.sharding.autoPartition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @version 1.0
 */

public class AutoPartitionTest extends AutoPartitionTestBase {

    private static final String TABLE_NAME = "auto_partition_tb_test";
    private String fullTableNameDrds;
    private String fullTableNameMySQL;

    public AutoPartitionTest(boolean croassSchema) {
        this.crossSchema = croassSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    @Before
    public void cleanEnv() {
        fullTableNameDrds =
            StringUtils.isBlank(tddlDatabase2) ? quoteSpecialName(TABLE_NAME) :
                tddlDatabase2 + "." + quoteSpecialName(TABLE_NAME);
        fullTableNameMySQL = StringUtils.isBlank(mysqlDatabase2) ? quoteSpecialName(TABLE_NAME) :
            mysqlDatabase2 + "." + quoteSpecialName(TABLE_NAME);

    }

    @After
    public void clean() {

    }

    private void assertPartitioned() {
        Map<String, List<String>> topology = getTopology(tddlDatabase2, TABLE_NAME);
        Assert.assertEquals(2, topology.size());
    }

    @Test
    public void testCreateAutoPartitionNoPk() {

        dropTableIfExists(fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);

        final String createSqlX = "create partition table " + fullTableNameDrds + "(x int)";
        final String insertSqlX = "insert into " + fullTableNameDrds + "values (1)";
        final String selectSqlX = "select * from " + fullTableNameDrds;

        final String createSql = "create table " + fullTableNameMySQL + "(x int)";
        final String insertSql = "insert into " + fullTableNameMySQL + "values (1)";
        final String selectSql = "select * from " + fullTableNameMySQL;

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        assertPartitioned();
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        selectContentSameAssert(selectSql, selectSqlX, null, mysqlConnection, tddlConnection);

        dropTableIfExists(fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);
    }

    @Test
    public void testCreateAutoPartitionSinglePk() {

        dropTableIfExists(fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);

        final String createSqlX = "create partition table " + fullTableNameDrds + "(x int, primary key(x))";
        final String insertSqlX = "insert into " + fullTableNameDrds + "values (1)";
        final String selectSqlX = "select * from " + fullTableNameDrds;

        final String createSql = "create table " + fullTableNameMySQL + "(x int, primary key(x))";
        final String insertSql = "insert into " + fullTableNameMySQL + "values (1)";
        final String selectSql = "select * from " + fullTableNameMySQL;

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        assertPartitioned();
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        selectContentSameAssert(selectSql, selectSqlX, null, mysqlConnection, tddlConnection);

        dropTableIfExists(fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);
    }

    @Test
    public void testCreateAutoPartitionCompositePk() {

        dropTableIfExists(fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);

        final String createSqlX = "create partition table " + fullTableNameDrds + "(x int, y int, primary key(x, y))";
        final String insertSqlX = "insert into " + fullTableNameDrds + "values (1, 2)";
        final String selectSqlX = "select * from " + fullTableNameDrds;

        final String createSql = "create table " + fullTableNameMySQL + "(x int, y int, primary key(x, y))";
        final String insertSql = "insert into " + fullTableNameMySQL + "values (1, 2)";
        final String selectSql = "select * from " + fullTableNameMySQL;

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        assertPartitioned();
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        selectContentSameAssert(selectSql, selectSqlX, null, mysqlConnection, tddlConnection);

        dropTableIfExists(fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);
    }

}
