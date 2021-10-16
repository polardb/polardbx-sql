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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

public class MySqlCreateTableTest118 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE sal_emp (\n" +
                "    name            int,\n" +
                "    pay_by_quarter  int[],\n" +
                "    schedule        long[256]\n" +
                ");";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE sal_emp (\n" +
                "\tname int,\n" +
                "\tpay_by_quarter int[],\n" +
                "\tschedule long[256]\n" +
                ");", stmt.toString());

    }

    public void test_1() throws Exception {
        String sql = "CREATE TABLE sal_emp (\n" +
                "    name            int,\n" +
                "    pay_by_quarter  array<int>,\n" +
                "    schedule        array<long>(256)\n" +
                ");";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE sal_emp (\n" +
                "\tname int,\n" +
                "\tpay_by_quarter ARRAY<int>,\n" +
                "\tschedule ARRAY<long>(256)\n" +
                ");", stmt.toString());

    }

}