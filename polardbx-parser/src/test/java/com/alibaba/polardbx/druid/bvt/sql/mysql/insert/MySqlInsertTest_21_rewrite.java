/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import org.junit.Assert;

import java.util.List;
import java.util.TimeZone;

public class MySqlInsertTest_21_rewrite extends MysqlTest {
    private TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");

    public void test_insert_rollback_on_fail() throws Exception {
        String sql = "insert into x values (1, CURDATE()), (2, CURDATE())";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.KeepInsertValueClauseOriginalString);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        String now = null;
        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;
        doRewriteFunctions(insertStmt, timeZone);
//        System.out.println(SQLUtils.toMySqlString(insertStmt));

        String formatSql = "insert into x values (1, '2018-01-25'), (2, '2018-01-25')";
        Assert.assertEquals(formatSql, SQLUtils.toMySqlString(insertStmt, VisitorFeature.OutputUseInsertValueClauseOriginalString));
    }

    public static void doRewriteFunctions(SQLInsertStatement insertStatement, TimeZone timeZone) {
        String now = null;
        for (SQLInsertStatement.ValuesClause valuesClause : insertStatement.getValuesList()) {
            boolean changed = false;
            List<SQLExpr> values = valuesClause.getValues();
            for (int i = 0; i < values.size(); i++) {
                SQLExpr expr = values.get(i);

                String methodName = null;
                if (expr instanceof SQLMethodInvokeExpr) {
                    methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
                } else {
                    continue;
                }

                if ("CURDATE".equalsIgnoreCase(methodName)
                        || "CUR_DATE".equalsIgnoreCase(methodName))
                {
                    if (now == null) {
                        now = now(timeZone);
                    }

                    values.set(i, new SQLCharExpr(now));
                    changed = true;
                }
            }
            if (changed) {
                valuesClause.setOriginalString(null);
                valuesClause.setOriginalString(SQLUtils.toMySqlString(valuesClause, VisitorFeature.OutputUseInsertValueClauseOriginalString));
            }
        }
    }

    public static String now(TimeZone timeZone) {
        return "2018-01-25";
    }
}
