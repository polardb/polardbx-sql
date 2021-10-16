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

public class MySqlSelectTest_127 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "/*+engine=mpp*/SELECT min(pay_byr_rate_90d) FROM (/*+engine=mpp*/SELECT pay_byr_rate_90d FROM caspian.ads_itm_hpcj_all_df WHERE item_pools_tags = '1116' AND pay_byr_rate_90d >= 0 ORDER BY pay_byr_rate_90d DESC LIMIT 49) LIMIT 500";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=mpp*/\n" +
                "SELECT min(pay_byr_rate_90d)\n" +
                "FROM (\n" +
                "\t/*+engine=mpp*/\n" +
                "\tSELECT pay_byr_rate_90d\n" +
                "\tFROM caspian.ads_itm_hpcj_all_df\n" +
                "\tWHERE item_pools_tags = '1116'\n" +
                "\t\tAND pay_byr_rate_90d >= 0\n" +
                "\tORDER BY pay_byr_rate_90d DESC\n" +
                "\tLIMIT 49\n" +
                ")\n" +
                "LIMIT 500", stmt.toString());
    }


}