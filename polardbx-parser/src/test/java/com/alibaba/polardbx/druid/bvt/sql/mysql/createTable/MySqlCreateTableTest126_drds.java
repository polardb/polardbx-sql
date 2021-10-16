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

public class MySqlCreateTableTest126_drds extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE xx\n" +
                "DBPARTITION BY hash(name1) TBPARTITION BY hash(name2) TBPARTITIONS 4\n" +
                "EXTPARTITION (\n" +
                "    DBPARTITION xxx BY KEY('abc') TBPARTITION yyy BY KEY('abc'),\n" +
                "    DBPARTITION yyy BY KEY('def') TBPARTITION yyy BY KEY('def'),\n" +
                "    DBPARTITION yyy BY KEY('gpk')\n" +
                ")";
//        System.out.println(sql);

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE xx\n" +
                "DBPARTITION BY hash(name1)\n" +
                "TBPARTITION BY hash(name2) TBPARTITIONS 4\n" +
                "EXTPARTITION (\n" +
                "\tDBPARTITION xxx BY KEY('abc') TBPARTITION yyy BY KEY('abc'), \n" +
                "\tDBPARTITION yyy BY KEY('def') TBPARTITION yyy BY KEY('def'), \n" +
                "\tDBPARTITION yyy BY KEY('gpk')\n" +
                ")", stmt.toString());

    }



}