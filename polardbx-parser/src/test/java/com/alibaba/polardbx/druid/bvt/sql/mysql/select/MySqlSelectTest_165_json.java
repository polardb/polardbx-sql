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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_165_json extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select concat(l_shipdate,'10') from lineitem join orders on l_orderkey = o_orderkey where l_shipdate between '1997-01-27' and '1997-02-20' and json_extract(l_comment,'$.id') = json '1997-01-2810' limit 3";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT concat(l_shipdate, '10')\n" +
                "FROM lineitem\n" +
                "\tJOIN orders ON l_orderkey = o_orderkey\n" +
                "WHERE l_shipdate BETWEEN '1997-01-27' AND '1997-02-20'\n" +
                "\tAND json_extract(l_comment, '$.id') = JSON '1997-01-2810'\n" +
                "LIMIT 3", stmt.toString());


    }

}