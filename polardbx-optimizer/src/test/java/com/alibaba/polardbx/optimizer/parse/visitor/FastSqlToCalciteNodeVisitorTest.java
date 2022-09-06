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
        String name = "CREATE java_function Addfour\n";
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
            (SqlIdentifier) visitor.convertToSqlNode(new SQLIdentifierExpr("Addfour", 0)),
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
            "CREATE JAVA FUNCTION `Addfour` RETURNTYPE bigint INPUTTYPE bigint, bigint IMPORT import java.util.Date; ENDIMPORT CODE public Object compute(Object[] args) {\n"
                + "        int a = Integer.parseInt(args[0].toString());\n"
                + "        int b = Integer.parseInt(args[1].toString());\n"
                + "\n"
                + "        return a + b;\n"
                + "    } ENDCODE", writer.toString());

    }

    @Test
    public void testVisitDrop() {
        visitor = new FastSqlToCalciteNodeVisitor(new ContextParameters(), new ExecutionContext());
        String sql = "drop java_function hundred";
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