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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_172_trim extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select 1 as '\\\"f\\\"a';";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT 1 AS '\"f\"a';", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "select 1 as \"\\\"f\\\"\";";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

//        System.out.println(stmt.toString());

        assertEquals("SELECT 1 AS \"\\\"f\\\"\";", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "select 1 as \"\\\"f\\\"\";";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

//        System.out.println(stmt.toString());

        assertEquals("SELECT 1 AS \"\\\"f\\\"\";", stmt.toString());
    }

    public void test_3() throws Exception {
        String sql = "select 1 as '\\'f\\'';";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        System.out.println(stmt.toString());

        assertEquals("SELECT 1 AS '\\'f\\'';", stmt.toString());
        assertEquals("'f'", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
    }

    public void test_3x() throws Exception {
        String sql = "select 1 as '\\'\\'f\\'';";
//        System.out.println(sql);
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT 1 AS '\\'\\'f\\'';", stmt.toString());
        assertEquals("''f'", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
    }

    public void test_4() throws Exception {
        String sql = "select 1 as \"\\'f\\'\";";
//        System.out.println(sql);
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

//        System.out.println(stmt.toString());

        assertEquals("SELECT 1 AS \"'f'\";", stmt.toString());
        assertEquals("'f'", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
    }

    public void test_5() throws Exception {
        String sql = "select 1 as \"\\\"f\\\"\";";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

//        System.out.println(stmt.toString());

        assertEquals("SELECT 1 AS \"\\\"f\\\"\";", stmt.toString());
        assertEquals("\"f\"", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
    }

    public void test_6() throws Exception {
        String sql = "select 1 as '\n'";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

//        System.out.println(stmt.toString());

        assertEquals("SELECT 1 AS \"\n" +
                "\"", stmt.toString());
        assertEquals("\n", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
    }

    public void test_7() throws Exception {
        String sql = "select 1 as '\\\\'";
        System.out.println(sql);
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT 1 AS \"\\\\\"", stmt.toString());
        assertEquals("\\", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
    }

    public void test_8() throws Exception {
        String sql = "select 1 as '\\t'";
        System.out.println(sql);
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT 1 AS \"\t\"", stmt.toString());
        assertEquals("\t", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
    }

    public void test_9() throws Exception {
        String sql = "select 1 as \"\"\"\", 2 as ''''";
        System.out.println(sql);
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT 1 AS \"\\\"\", 2 AS '\\''", stmt.toString());
        assertEquals("\"", stmt.getSelect().getQueryBlock().getSelectItem(0).getAlias2());
        assertEquals("'", stmt.getSelect().getQueryBlock().getSelectItem(1).getAlias2());
    }
}