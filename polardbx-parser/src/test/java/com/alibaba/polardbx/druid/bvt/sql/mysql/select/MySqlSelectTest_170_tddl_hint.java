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
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_170_tddl_hint extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "/*+TDDL({'type':'direct','dbid':'xxx_group'})*/select * from real_table_0;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+TDDL({'type':'direct','dbid':'xxx_group'})*/\n" +
                "SELECT *\n" +
                "FROM real_table_0;", stmt.toString());

        TDDLHint hint = (TDDLHint) stmt.getHeadHintsDirect().get(0);
        assertSame(TDDLHint.Type.JSON, hint.getType());
        assertEquals("{'type':'direct','dbid':'xxx_group'}", hint.getJson());
    }

    public void test_1() throws Exception {
        String sql = "/*TDDL:DEFER*/select * from real_table_0;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*TDDL:DEFER*/\n" +
                "SELECT *\n" +
                "FROM real_table_0;", stmt.toString());

        TDDLHint hint = (TDDLHint) stmt.getHeadHintsDirect().get(0);
        assertSame(TDDLHint.Type.Function, hint.getType());
        assertEquals("DEFER", hint.getFunctions().get(0).getName());
    }

    public void test_2() throws Exception {
        String sql = "/*TDDL:UNDO_LOG_LIMIT=2000*/select * from real_table_0;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*TDDL:UNDO_LOG_LIMIT=2000*/\n" +
                "SELECT *\n" +
                "FROM real_table_0;", stmt.toString());

        TDDLHint hint = (TDDLHint) stmt.getHeadHintsDirect().get(0);
        assertSame(TDDLHint.Type.Function, hint.getType());
        assertEquals("UNDO_LOG_LIMIT", hint.getFunctions().get(0).getName());
    }
}