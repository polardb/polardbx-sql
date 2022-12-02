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

public class MySqlCreateTableTest128_quote extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE \"linxi_test\".\"linxi_subpart11\" (\n" +
            "  \"id\"       BIGINT  NOT NULL\n" +
            "  COMMENT '',\n" +
            "  \"int_test\" BIGINT  NOT NULL\n" +
            "  COMMENT '',\n" +
            "  v_test     VARCHAR NOT NULL\n" +
            "  COMMENT '',\n" +
            "  PRIMARY KEY (\'id\', int_test, subcol)\n" +
            ") PARTITION BY HASH KEY (\"id\"\n" +
            ") PARTITION NUM 10 SUBPARTITION BY LIST (\"subcol\" BIGINT\n" +
            ") SUBPARTITION OPTIONS (available_Partition_Num=100\n" +
            ") TABLEGROUP group2 OPTIONS (UPDATETYPE='realtime'\n" +
            ") COMMENT ''";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TABLE \"linxi_test\".\"linxi_subpart11\" (\n" +
            "\t\"id\" BIGINT NOT NULL COMMENT '',\n" +
            "\t\"int_test\" BIGINT NOT NULL COMMENT '',\n" +
            "\tv_test VARCHAR NOT NULL COMMENT '',\n" +
            "\tPRIMARY KEY (\'id\', int_test, subcol)\n" +
            ")\n" +
            "OPTIONS (UPDATETYPE = 'realtime') COMMENT ''\n" +
            "PARTITION BY HASH KEY('id') PARTITION NUM 10\n" +
            "SUBPARTITION BY LIST (\"subcol\" BIGINT)\n" +
            "SUBPARTITION OPTIONS (available_Partition_Num = 100)\n" +
            "TABLEGROUP = group2", stmt.toString());

    }

}