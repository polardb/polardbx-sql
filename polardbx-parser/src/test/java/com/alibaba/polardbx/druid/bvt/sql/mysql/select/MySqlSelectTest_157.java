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
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_157 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT 1 " +
                "FROM corona_select_one_db_one_tb AS layer_0_left_tb " +
                "RIGHT JOIN corona_select_multi_db_multi_tb AS layer_0_right_tb " +
                "   ON layer_0_right_tb.mediumint_test=layer_0_right_tb.char_test " +
                "WHERE (layer_0_right_tb.timestamp_test BETWEEN 'x-3' AND ROW(3, 4) NOT IN (ROW(1, 2 ),ROW(3, 4)))";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        SQLBetweenExpr where = (SQLBetweenExpr) stmt.getSelect().getQueryBlock().getWhere();
        assertEquals(SQLInListExpr.class, where.getEndExpr().getClass());

        assertEquals("SELECT 1\n" +
                "FROM corona_select_one_db_one_tb layer_0_left_tb\n" +
                "\tRIGHT JOIN corona_select_multi_db_multi_tb layer_0_right_tb ON layer_0_right_tb.mediumint_test = layer_0_right_tb.char_test\n" +
                "WHERE layer_0_right_tb.timestamp_test BETWEEN 'x-3' AND (ROW(3, 4) NOT IN (ROW(1, 2), ROW(3, 4)))", stmt.toString());

        assertEquals("SELECT ?\n" +
                        "FROM corona_select_one_db_one_tb layer_0_left_tb\n" +
                        "\tRIGHT JOIN corona_select_multi_db_multi_tb layer_0_right_tb ON layer_0_right_tb.mediumint_test = layer_0_right_tb.char_test\n" +
                        "WHERE layer_0_right_tb.timestamp_test BETWEEN ? AND (ROW(?, ?) NOT IN (ROW(?, ?), ROW(?, ?)))"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));


    }

}