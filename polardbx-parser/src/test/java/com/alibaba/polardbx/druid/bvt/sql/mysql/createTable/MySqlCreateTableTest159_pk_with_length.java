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
import com.google.common.collect.ImmutableList;

import java.util.List;

public class MySqlCreateTableTest159_pk_with_length extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "create table gxw_test_87 (id int, name varchar(20),  primary key(id, `name`(10))) dbpartition by hash(id);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals(stmt.getPrimaryKeyNames(), ImmutableList.of("id", "name"));

        assertEquals("CREATE TABLE gxw_test_87 (\n" +
                "\tid int,\n" +
                "\tname varchar(20),\n" +
                "\tPRIMARY KEY (id, `name`(10))\n" +
                ")\n" +
                "DBPARTITION BY hash(id);", stmt.toString());



    }




}