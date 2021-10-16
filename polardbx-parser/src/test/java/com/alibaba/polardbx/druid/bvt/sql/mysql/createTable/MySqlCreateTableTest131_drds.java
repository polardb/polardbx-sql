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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

public class MySqlCreateTableTest131_drds extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE shard_hot_test_table (\n" +
                " id int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                " name varchar(30) DEFAULT NULL,\n" +
                " gmt_create datetime(3) DEFAULT NULL, PRIMARY KEY (id), KEY auto_shard_key_name (name) USING BTREE ) ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(name) dbpartitions 8 tbpartition by hash(name) tbpartitions 64 extpartition( dbpartition HOT_TEST_ERROR_TIPS_1544416881653EDSX_Z4MU_0000_HOT by key('aaa') tbpartition shard_hot_test_table_aaa_table by key('aaa'), dbpartition HOT_TEST_ERROR_TIPS_1544416881653EDSX_Z4MU_0001_HOT by key('bbb')\n" +
                " tbpartition shard_hot_test_table_bbb_table by key('bbb') )";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE shard_hot_test_table (\n" +
                "\tid int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "\tname varchar(30) DEFAULT NULL,\n" +
                "\tgmt_create datetime(3) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (id),\n" +
                "\tKEY auto_shard_key_name USING BTREE (name)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
                "DBPARTITION BY hash(name) DBPARTITIONS 8\n" +
                "TBPARTITION BY hash(name) TBPARTITIONS 64\n" +
                "EXTPARTITION (\n" +
                "\tDBPARTITION HOT_TEST_ERROR_TIPS_1544416881653EDSX_Z4MU_0000_HOT BY key('aaa') TBPARTITION shard_hot_test_table_aaa_table BY key('aaa'), \n" +
                "\tDBPARTITION HOT_TEST_ERROR_TIPS_1544416881653EDSX_Z4MU_0001_HOT BY key('bbb') TBPARTITION shard_hot_test_table_bbb_table BY key('bbb')\n" +
                ")", stmt.toString());

        SQLColumnDefinition id = stmt.getColumn("ID");
        assertNotNull(id);
        assertTrue(id.isPrimaryKey());
    }




}