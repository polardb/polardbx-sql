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

public class MySqlCreateTableTest141 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE sbtest1 (\n" +
                "id INTEGER UNSIGNED NOT NULL ,\n" +
                "k INTEGER UNSIGNED DEFAULT '0' NOT NULL,\n" +
                "c CHAR(120) DEFAULT '' NOT NULL,\n" +
                "pad CHAR(60) DEFAULT '' NOT NULL,\n" +
                "KEY xid (id)\n" +
                ") /*! ENGINE = innodb MAX_ROWS = 1000000 */  dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE sbtest1 (\n" +
                "\tid INTEGER UNSIGNED NOT NULL,\n" +
                "\tk INTEGER UNSIGNED NOT NULL DEFAULT '0',\n" +
                "\tc CHAR(120) NOT NULL DEFAULT '',\n" +
                "\tpad CHAR(60) NOT NULL DEFAULT '',\n" +
                "\tKEY xid (id)\n" +
                ")\n" +
                "DBPARTITION BY hash(id)\n" +
                "TBPARTITION BY hash(id) TBPARTITIONS 2 /*! ENGINE = innodb MAX_ROWS = 1000000 */", stmt.toString());

        assertEquals("create table sbtest1 (\n" +
                "\tid INTEGER unsigned not null,\n" +
                "\tk INTEGER unsigned not null default '0',\n" +
                "\tc CHAR(120) not null default '',\n" +
                "\tpad CHAR(60) not null default '',\n" +
                "\tkey xid (id)\n" +
                ")\n" +
                "dbpartition by hash(id)\n" +
                "tbpartition by hash(id) tbpartitions 2 /*! ENGINE = innodb MAX_ROWS = 1000000 */", stmt.toLowerCaseString());

    }





}