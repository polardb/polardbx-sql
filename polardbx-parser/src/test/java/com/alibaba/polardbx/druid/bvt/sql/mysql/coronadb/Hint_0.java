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

package com.alibaba.polardbx.druid.bvt.sql.mysql.coronadb;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcUtils;
import junit.framework.TestCase;

import java.util.List;

public class Hint_0 extends TestCase {
    public void test_for_single_hint() throws Exception {
        String sql = "select /*+ TDDL : construct() add_ms(sort=\"pk\", asc=\"true\") add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(sort=\"c\", asc=\"true\") */ pk, count(*) c from normaltbl_c group by pk order by pk ;";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);
        assertEquals(
                "+ TDDL : construct() add_ms(sort=\"pk\", asc=\"true\") add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(sort=\"c\", asc=\"true\") ",
                hint.getText());
    }

    public void test_for_multi_hint() throws Exception {
        String sql = "select /*+TDDL:construct() add_ms(sort=\"pk\", asc=\"true\") add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(sort=\"c\", asc=\"true\") */ pk, count(*) c from normaltbl_c group by pk order by pk ;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);
        assertEquals(
                "+TDDL:construct() add_ms(sort=\"pk\", asc=\"true\") add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(sort=\"c\", asc=\"true\") ",
                hint.getText());
    }

    public void test_for_multi_hint_1() throws Exception {
        String sql = "select /*+ TDDL : construct() add_ms(sort=t.pk, asc=true) add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(t.pk, true) */ pk, count(*) c from normaltbl_c group by pk order by pk ;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);
        assertEquals(
                "+ TDDL : construct() add_ms(sort=t.pk, asc=true) add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(t.pk, true) ",
                hint.getText());
    }

    public void test_for_multi_hint_bang() throws Exception {
        String sql = "select /!TDDL : construct() add_ms(sort=t.pk, asc=true) add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(c, true) */ pk, count(*) c from normaltbl_c group by pk order by pk ;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);
        assertEquals(
                "TDDL : construct() add_ms(sort=t.pk, asc=true) add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(c, true) ",
                hint.getText());
    }

    public void test_for_multi_hint_2() throws Exception {
        String sql = "select /*TDDL : construct() add_ms(sort=t.pk, asc=true) add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(c, true) */ pk, count(*) c from normaltbl_c group by pk order by pk ;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);
        assertEquals("TDDL : construct() add_ms(sort=t.pk, asc=true) add_agg(agg=\"SUM\", group=\"pk\", column=\"c\") add_pj(c=\"c\") add_ts(c, true) ", hint.getText());
    }

    public void test_for_hint_3() throws Exception {
        String sql = "select /*+ TDDL : add_ts(t.pk, true) add_ms(sort=t.pk, asc=true) */ pk, count(*) c from normaltbl_c group by pk order by pk ;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);
        assertEquals("+ TDDL : add_ts(t.pk, true) add_ms(sort=t.pk, asc=true) ", hint.getText());
    }

    public void test_for_hint_4() throws Exception {
        String sql = "select /*TDDL:construct() add_ft(\"name like \\'%hi%\\'\") */ pk, count(*) c from normaltbl_c group by pk order by pk ;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);

        assertEquals("TDDL:construct() add_ft(\"name like \\'%hi%\\'\") ", hint.getText());
    }

    public void test_for_hint_5() throws Exception {
        String sql = " select /* +TDDL: construct() push_pj(\"case pk when 1 then 'hello' when substring('2', 1, 1) then 'CoronaDB' when null then '!' end \") add_un()*/ * from test_table_a;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL, SQLParserFeature.TDDLHint);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQueryBlock();
        SQLCommentHint hint = queryBlock.getHints().get(0);

        assertEquals("+TDDL: construct() push_pj(\"case pk when 1 then 'hello' when substring('2', 1, 1) then 'CoronaDB' when null then '!' end \") add_un()", hint.getText());
    }
}
