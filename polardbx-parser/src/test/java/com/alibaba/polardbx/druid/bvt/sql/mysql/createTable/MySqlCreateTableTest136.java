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

public class MySqlCreateTableTest136 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `c1` (\n" +
                "        `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "        `d` int DEFAULT 1,\n" +
                "        `b` varchar(100) DEFAULT '123',\n" +
                "        PRIMARY KEY (`id`),\n" +
                "    key idxa2(`d`,`b`) comment '2'\n" +
                ");";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE `c1` (\n" +
                "\t`id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "\t`d` int DEFAULT 1,\n" +
                "\t`b` varchar(100) DEFAULT '123',\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tKEY idxa2 (`d`, `b`) COMMENT '2'\n" +
                ");", stmt.toString());

        assertEquals("create table `c1` (\n" +
                "\t`id` bigint not null auto_increment,\n" +
                "\t`d` int default 1,\n" +
                "\t`b` varchar(100) default '123',\n" +
                "\tprimary key (`id`),\n" +
                "\tkey idxa2 (`d`, `b`) comment '2'\n" +
                ");", stmt.toLowerCaseString());

    }




}