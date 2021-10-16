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

public class MySqlCreateTableTest95 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `f_product` (\n" +
                "  `id` int(200) NOT NULL AUTO_INCREMENT,\n" +
                " `type` enum('market','land','enterprise') CHARACTER SET utf8 DEFAULT NULL COMMENT 'comment',\n" +
                " PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(3, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE `f_product` (\n" +
                "\t`id` int(200) NOT NULL AUTO_INCREMENT,\n" +
                "\t`type` enum('market', 'land', 'enterprise') CHARACTER SET utf8 DEFAULT NULL COMMENT 'comment',\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4", stmt.toString());
    }
}