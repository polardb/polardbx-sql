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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_178 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select rt_dws_csn_sta_lgt_ord_ri.metrics_id as yujiu from rt_dws_csn_sta_lgt_ord_ri CROSS JOIN rt_dws_csn_sta_lgt_ord_mi ;";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.EnableSQLBinaryOpExprGroup,
                SQLParserFeature.OptimizedForParameterized);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT rt_dws_csn_sta_lgt_ord_ri.metrics_id AS yujiu\n" +
                "FROM rt_dws_csn_sta_lgt_ord_ri\n" +
                "\tCROSS JOIN rt_dws_csn_sta_lgt_ord_mi;", stmt.toString());

        assertEquals("select rt_dws_csn_sta_lgt_ord_ri.metrics_id as yujiu\n" +
                "from rt_dws_csn_sta_lgt_ord_ri\n" +
                "\tcross join rt_dws_csn_sta_lgt_ord_mi;", stmt.toLowerCaseString());


        assertEquals("SELECT rt_dws_csn_sta_lgt_ord_ri.metrics_id AS yujiu\n" +
                "FROM rt_dws_csn_sta_lgt_ord_ri\n" +
                "\tCROSS JOIN rt_dws_csn_sta_lgt_ord_mi;", stmt.toParameterizedString());

        SQLJoinTableSource join = (SQLJoinTableSource) stmt.getSelect().getQueryBlock().getFrom();
        assertEquals(SQLJoinTableSource.JoinType.CROSS_JOIN, join.getJoinType());
        assertNull(join.getLeft().getAlias());
    }


}