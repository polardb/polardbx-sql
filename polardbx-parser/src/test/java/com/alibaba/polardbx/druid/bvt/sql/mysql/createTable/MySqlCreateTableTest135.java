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

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author Dagon0577
 * @date 2020/5/14 10:51
 */
public class MySqlCreateTableTest135 extends TestCase {
    public void test_0() throws Exception {
        String sql =
                "create table MQ_TOPIC_RECORD(\n"
                        + "   TOPIC_ID             bigint(11) not null,\n"
                        + "   BROKENNAME           national VARCHAR(50),\n"
                        + "   BROKENNAME2           national CHAR(50)\n" + ");";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE MQ_TOPIC_RECORD (\n" +
                "\tTOPIC_ID bigint(11) NOT NULL,\n" +
                "\tBROKENNAME national VARCHAR(50),\n" +
                "\tBROKENNAME2 national CHAR(50)\n" +
                ");", stmt.toString());

        assertEquals("create table MQ_TOPIC_RECORD (\n" +
                "\tTOPIC_ID bigint(11) not null,\n" +
                "\tBROKENNAME national VARCHAR(50),\n" +
                "\tBROKENNAME2 national CHAR(50)\n" +
                ");", stmt.toLowerCaseString());

    }
}
