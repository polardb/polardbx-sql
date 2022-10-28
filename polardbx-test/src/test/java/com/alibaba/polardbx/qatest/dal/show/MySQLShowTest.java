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

package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;


public class MySQLShowTest extends ReadBaseTestCase {
    private String testTableName;
    
    public MySQLShowTest(String testTableName) {
        this.testTableName = testTableName;
    }

    @Parameterized.Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE_AUTONIC));
    }

    @Test
    public void showColumns() {
        String sql = "show columns from " + testTableName;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showColumnsWithDb() {
        String sql = "show columns from " + polardbxOneDB + "." + testTableName;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "show columns from " + testTableName + " from " + polardbxOneDB;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showColumnsWithDb_information_schema() {
        String sql = "show columns from information_schema.TABLES";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "show columns from TABLES from information_schema";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showIndex() {
        String sql = "show index from " + testTableName;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showIndexWithDb() {
        String sql = "show index from " + polardbxOneDB + "." + testTableName;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "show index from " + testTableName + " from " + polardbxOneDB;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showIndexWithDb_information_schema() {
        String sql = "show index from information_schema.TABLES";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "show index from TABLES from information_schema";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showOpenTables() {
        String sql = "show open tables from " + polardbxOneDB;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showOpenTables_information_schema() {
        String sql = "show open tables from information_schema";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    // @Ignore("cannot handle demo_group._tddl_")
    public void showTableStatus() {
        String sql = "show table status from " + polardbxOneDB;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showTables() {
        String sql = "show tables from " + polardbxOneDB;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showTables_information_schema() {
        String sql = "show tables from information_schema";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showTriggers() {
        String sql = "show triggers from " + polardbxOneDB;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showTriggers_information_schema() {
        String sql = "show triggers from information_schema";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showCreateTable() {
        String sql = "show create table " + polardbxOneDB + "." + testTableName;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showCreateTable_information_schema() {
        String sql = "show create table information_schema.TABLES";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void showAnalyzeTable() {
        String sql = "analyze table " + polardbxOneDB + "." + testTableName;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    @Ignore("access denied")
    public void showAnalyzeTable_information_schema() {
        String sql = "analyze table information_schema.TABLES";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void descTable() {
        String sql = "desc " + polardbxOneDB + "." + testTableName;

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }

    @Test
    public void descTableWithDifferentpolardbxOneDB() throws SQLException {
        String sql = "desc " + polardbxOneDB + "." + testTableName;
        try (Connection conn = getPolardbxConnection(polardbxOneDB2)) {
            executeOnMysqlAndTddl(mysqlConnection, conn, sql, ImmutableList.of());
        }
    }

    @Test
    public void descTable_information_schema() {
        String sql = "desc information_schema.TABLES";

       executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }
}