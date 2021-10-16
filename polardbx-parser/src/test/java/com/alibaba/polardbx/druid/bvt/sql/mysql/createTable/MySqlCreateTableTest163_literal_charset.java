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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * @version 1.0
 */
public class MySqlCreateTableTest163_literal_charset extends MysqlTest {

    public void test0() throws Exception {
        final String sql = "CREATE TABLE `test` (\n"
            + "  `x` varchar(32) CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'\n"
            + ");";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE TABLE `test` (\n"
            + "\t`x` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci\n"
            + ");", statementList.get(0).toString());
    }

    public void test1() throws Exception {
        final String sql = "CREATE TABLE `test` (\n"
            + "  `x` varchar(32)\n"
            + ") CHARACTER SET 'utf8' COLLATE 'utf8_general_ci';";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE TABLE `test` (\n"
            + "\t`x` varchar(32)\n"
            + ") DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE = 'utf8_general_ci';", statementList.get(0).toString());
    }

    public void test2() throws Exception {
        final String sql = "CREATE DATABASE `test` CHARACTER SET 'utf8' COLLATE 'utf8_general_ci';";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE DATABASE `test` CHARACTER SET utf8 COLLATE utf8_general_ci;",
            statementList.get(0).toString());
    }

    public void test3() throws Exception {
        final String sql = "CREATE TABLE `test` (\n"
            + "  `x` varchar(32) CHARACTER SET binary\n"
            + ");";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE TABLE `test` (\n"
            + "\t`x` varchar(32) CHARACTER SET binary\n"
            + ");", statementList.get(0).toString());
    }

    public void test4() throws Exception {
        final String sql = "CREATE TABLE `test` (\n"
            + "  `x` varchar(32)\n"
            + ") CHARACTER SET binary;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE TABLE `test` (\n"
            + "\t`x` varchar(32)\n"
            + ") DEFAULT CHARACTER SET = BINARY;", statementList.get(0).toString());
    }

    public void test5() throws Exception {
        final String sql = "CREATE DATABASE `test` CHARACTER SET binary;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE DATABASE `test` CHARACTER SET binary;", statementList.get(0).toString());
    }

    public void test6() throws Exception {
        final String sql = "CREATE TABLE `test` (\n"
            + "  `x` varchar(32) CHARACTER SET 'binary'\n"
            + ");";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE TABLE `test` (\n"
            + "\t`x` varchar(32) CHARACTER SET binary\n"
            + ");", statementList.get(0).toString());
    }

    public void test7() throws Exception {
        final String sql = "CREATE TABLE `test` (\n"
            + "  `x` varchar(32)\n"
            + ") CHARACTER SET 'binary';";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE TABLE `test` (\n"
            + "\t`x` varchar(32)\n"
            + ") DEFAULT CHARACTER SET = 'binary';", statementList.get(0).toString());
    }

    public void test8() throws Exception {
        final String sql = "CREATE DATABASE `test` CHARACTER SET 'binary';";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        assertEquals("CREATE DATABASE `test` CHARACTER SET binary;", statementList.get(0).toString());
    }

}
