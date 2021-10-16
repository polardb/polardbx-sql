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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

public class MySqlSelectTest_117 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select * \n" +
                "FROM\n" +
                "    rpt_sta_tool_daily a\n" +
                "WHERE\n" +
                "    day_id IN (\n" +
                "        DATE_FORMAT(\n" +
                "            DATE_ADD(NOW(), INTERVAL - 1 DAY),\n" +
                "            '%Y%m%d'\n" +
                "        ),\n" +
                "        DATE_FORMAT(\n" +
                "            DATE_ADD(NOW(), INTERVAL - 2 DAY),\n" +
                "            '%Y%m%d'\n" +
                "        ),\n" +
                "        DATE_FORMAT(\n" +
                "            DATE_ADD(NOW(), INTERVAL - 8 DAY),\n" +
                "            '%Y%m%d'\n" +
                "        )\n" +
                "    ) limit 100";

//        System.out.println(sql);

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT *\n" +
                "FROM rpt_sta_tool_daily a\n" +
                "WHERE day_id IN (DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y%m%d'), DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -2 DAY), '%Y%m%d'), DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -8 DAY), '%Y%m%d'))\n" +
                "LIMIT 100", stmt.toString());
    }
}