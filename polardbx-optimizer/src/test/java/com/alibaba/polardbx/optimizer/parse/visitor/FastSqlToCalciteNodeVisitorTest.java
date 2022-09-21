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

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateJavaFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropJavaFunctionStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import junit.framework.TestCase;
import org.apache.calcite.sql.SqlCreateJavaFunction;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDropJavaFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FastSqlToCalciteNodeVisitorTest extends TestCase {

    FastSqlToCalciteNodeVisitor visitor;

    @Test
    public void testVisitCreate() {
        visitor = new FastSqlToCalciteNodeVisitor(new ContextParameters(), new ExecutionContext());
        String name = "CREATE function TESTFOO\n";
        String returnType = "returnType bigint\n";
        String input = "inputType bigint, bigint\n";
        String importString = "import\n"
            + "import java.util.Date;\n"
            + "endimport\n";
        String code = "CODE\n"
            + "    public Object compute(Object[] args) {\n"
            + "        int a = Integer.parseInt(args[0].toString());\n"
            + "        int b = Integer.parseInt(args[1].toString());\n"
            + "\n"
            + "        return a + b;\n"
            + "    }\n"
            + "ENDCODE\n";

        List<String> list = Arrays.asList(returnType, importString, input, code);
        StringBuilder sb = new StringBuilder(name);
        Collections.shuffle(list);
        for (String str : list) {
            sb.append(str);
        }
        String sql = sb.toString();
        SQLStatementParser sqlStatementParser = new SQLStatementParser(sql);
        SQLCreateJavaFunctionStatement sqlCreateJavaFunctionStatement = sqlStatementParser.parseCreateJavaFunction();
        visitor.visit(sqlCreateJavaFunctionStatement);
        SqlCreateJavaFunction node = (SqlCreateJavaFunction) SqlDdlNodes.createJavaFunction(SqlParserPos.ZERO,
            (SqlIdentifier) visitor.convertToSqlNode(new SQLIdentifierExpr("TESTFOO", 0)),
            "bigint",
            Arrays.asList("bigint", "bigint"),
            sql.substring(sql.indexOf("CODE") + 4, sql.indexOf("ENDCODE")).trim(),
            "import java.util.Date;");
        SqlCreateJavaFunction sqlCreateJavaFunction = (SqlCreateJavaFunction) visitor.getSqlNode();

        Assert.assertEquals(sqlCreateJavaFunction.getJavaCode(), node.getJavaCode());
        Assert.assertEquals(sqlCreateJavaFunction.getFuncName().toString(), node.getFuncName().toString());
        Assert.assertEquals(sqlCreateJavaFunction.getImportString(), node.getImportString());
        Assert.assertEquals(sqlCreateJavaFunction.getReturnType(), node.getReturnType());
        Assert.assertEquals(sqlCreateJavaFunction.getInputTypes(), node.getInputTypes());

        SqlDialect dialect = AnsiSqlDialect.DEFAULT;
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        writer.setAlwaysUseParentheses(true);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        sqlCreateJavaFunction.unparse(writer, 0, 0);

        Assert.assertEquals(
            "CREATE JAVA FUNCTION `TESTFOO` RETURNTYPE bigint INPUTTYPE bigint, bigint IMPORT import java.util.Date; ENDIMPORT CODE public Object compute(Object[] args) {\n"
                + "        int a = Integer.parseInt(args[0].toString());\n"
                + "        int b = Integer.parseInt(args[1].toString());\n"
                + "\n"
                + "        return a + b;\n"
                + "    } ENDCODE", writer.toString());

    }

    @Test
    public void testVisitDrop() {
        visitor = new FastSqlToCalciteNodeVisitor(new ContextParameters(), new ExecutionContext());
        String sql = "drop function hundred";
        SQLStatementParser sqlStatementParser = new SQLStatementParser(sql);
        SQLDropJavaFunctionStatement sqlDropJavaFunctionStatement =
            (SQLDropJavaFunctionStatement) sqlStatementParser.parseDrop();
        visitor.visit(sqlDropJavaFunctionStatement);
        SqlDropJavaFunction sqlDropJavaFunction = (SqlDropJavaFunction) visitor.getSqlNode();

        SqlDialect dialect = AnsiSqlDialect.DEFAULT;
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        writer.setAlwaysUseParentheses(true);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        sqlDropJavaFunction.unparse(writer, 0, 0);

        Assert.assertEquals("DROP JAVA FUNCTION `hundred`", writer.toString());
    }
}