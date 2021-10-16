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

public class MySqlCreateTableTest100 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS ttable\n" +
                "(\n" +
                "`id` BIGINT(20) NOT NULL AUTO_INCREMENT,\n" +
                "`queue_id` varchar(20) NOT NULL DEFAULT '-1',\n" +
                "`status` TINYINT(4) NOT NULL DEFAULT '1',\n" +
                "`geometry` geometry not null,\n" +
                "CONSTRAINT PRIMARY KEY (`id`),\n" +
                "CONSTRAINT UNIQUE KEY `uk_queue_id` USING BTREE (`queue_id`) KEY_BLOCK_SIZE=10,\n" +
                "FULLTEXT KEY `ft_status` (`queue_id`),\n" +
                "spatial index `spatial` (`geometry`),\n" +
                "CONSTRAINT FOREIGN KEY `fk_test`(`queue_id`) REFERENCES `test`(`id`)\n" +
                ") ENGINE=INNODB";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(9, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS ttable (\n" +
                "\t`id` BIGINT(20) NOT NULL AUTO_INCREMENT,\n" +
                "\t`queue_id` varchar(20) NOT NULL DEFAULT '-1',\n" +
                "\t`status` TINYINT(4) NOT NULL DEFAULT '1',\n" +
                "\t`geometry` geometry NOT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `uk_queue_id` USING BTREE (`queue_id`) KEY_BLOCK_SIZE = 10,\n" +
                "\tFULLTEXT KEY `ft_status` (`queue_id`),\n" +
                "\tSPATIAL INDEX `spatial`(`geometry`),\n" +
                "\tCONSTRAINT FOREIGN KEY `fk_test` (`queue_id`) REFERENCES `test` (`id`)\n" +
                ") ENGINE = INNODB", stmt.toString());
    }
}