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

public class MySqlCreateTableTest99 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS srcTable\n" +
                "(\n" +
                "  `id` BIGINT(20) NOT NULL,\n" +
                "  `queue_id` BIGINT(20) NOT NULL DEFAULT '-1',\n" +
                "  `status` TINYINT(4) NOT NULL DEFAULT '1',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=INNODB AUTO_INCREMENT 10 AVG_ROW_LENGTH 10 DEFAULT CHARACTER SET=utf8 DEFAULT COLLATE = utf8_general_ci\n" +
                "CHECKSUM=0 COMPRESSION='NONE' CONNECTION = 'connect_string'  DELAY_KEY_WRITE = 0 ENCRYPTION 'N' INSERT_METHOD FIRST\n" +
                "MAX_ROWS 1000 MIN_ROWS=10 PACK_KEYS DEFAULT PASSWORD '12345678' STATS_AUTO_RECALC 0 STATS_PERSISTENT 0 \n" +
                "STATS_SAMPLE_PAGES 10";

//        System.out.println(sql);

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(4, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE IF NOT EXISTS srcTable (\n" +
                "\t`id` BIGINT(20) NOT NULL,\n" +
                "\t`queue_id` BIGINT(20) NOT NULL DEFAULT '-1',\n" +
                "\t`status` TINYINT(4) NOT NULL DEFAULT '1',\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = INNODB AUTO_INCREMENT = 10 AVG_ROW_LENGTH = 10 DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_general_ci CHECKSUM = 0 COMPRESSION = 'NONE' CONNECTION = 'connect_string' DELAY_KEY_WRITE = 0 ENCRYPTION = 'N' INSERT_METHOD = FIRST MAX_ROWS = 1000 MIN_ROWS = 10 PACK_KEYS = DEFAULT PASSWORD = '12345678' STATS_AUTO_RECALC = 0 STATS_PERSISTENT = 0 STATS_SAMPLE_PAGES = 10", stmt.toString());
    }
}