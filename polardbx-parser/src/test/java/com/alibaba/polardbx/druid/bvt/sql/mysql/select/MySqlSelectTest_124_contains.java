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

public class MySqlSelectTest_124_contains extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select shid from alitrip_htl_realtime.hotel_real_time_inventory where rate_sale_status = 1 and rate_tags contains ('520') " +
                "group by shid,start_time_daily,end_time_daily";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT shid\n" +
                "FROM alitrip_htl_realtime.hotel_real_time_inventory\n" +
                "WHERE rate_sale_status = 1\n" +
                "\tAND rate_tags CONTAINS ('520')\n" +
                "GROUP BY shid, start_time_daily, end_time_daily", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "select shid from alitrip_htl_realtime.hotel_real_time_inventory where rate_sale_status = 1 and contains (rate_tags, '520') " +
                "group by shid,start_time_daily,end_time_daily";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT shid\n" +
                "FROM alitrip_htl_realtime.hotel_real_time_inventory\n" +
                "WHERE rate_sale_status = 1\n" +
                "\tAND CONTAINS (rate_tags, '520')\n" +
                "GROUP BY shid, start_time_daily, end_time_daily", stmt.toString());
    }
}