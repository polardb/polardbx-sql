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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_149 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT length('aaa' collate utf8_general_ci) FROM corona_select_one_db_one_tb";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT length('aaa' COLLATE utf8_general_ci)\n" +
                "FROM corona_select_one_db_one_tb", stmt.toString());

        assertEquals("SELECT length(? COLLATE utf8_general_ci)\n" +
                        "FROM corona_select_one_db_one_tb"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));

        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
        assertEquals(1, queryBlock.getSelectList().size());
        assertEquals("length('aaa' COLLATE utf8_general_ci)"
                , queryBlock.getSelectList().get(0).getExpr().toString()
        );
    }
    public void test_1() throws Exception {
        String sql = "SELECT count(1), length('aaa' collate utf8_general_ci) FROM corona_select_one_db_one_tb";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT count(1), length('aaa' COLLATE utf8_general_ci)\n" + "FROM corona_select_one_db_one_tb", stmt.toString());

        assertEquals("SELECT count(1), length(? COLLATE utf8_general_ci)\n" + "FROM corona_select_one_db_one_tb"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));

        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
        assertEquals(2, queryBlock.getSelectList().size());
        assertEquals("length('aaa' COLLATE utf8_general_ci)"
                , queryBlock.getSelectList().get(1).getExpr().toString()
        );
    }
    public void test_2() throws Exception {
        String sql = "select i `table` from ttt `table`";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint,
                                                                    SQLParserFeature.IgnoreNameQuotes);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        System.out.println(stmt.toString());
    }
}