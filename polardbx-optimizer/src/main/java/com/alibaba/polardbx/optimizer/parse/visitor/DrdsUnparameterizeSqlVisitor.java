package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.io.BaseEncoding;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.alibaba.polardbx.druid.sql.parser.SQLParserUtils.createSQLStatementParser;

public class DrdsUnparameterizeSqlVisitor extends DrdsParameterizeSqlVisitor {
    private List<ParameterContext> parameterContextList;

    private int parameterIndex = 0;

    // WARNING!!!
    // DON'T CALL THIS VISITOR, BECAUSE IT'S DESIGNED FOR ONLY ONE OCCASION, WHICH IS UNLIKELY YOURS.
    public DrdsUnparameterizeSqlVisitor(Appendable appender, boolean parameterized, ExecutionContext executionContext,
                                        List<ParameterContext> parameterContextList) {
        super(appender, parameterized, executionContext);
        this.parameterContextList = parameterContextList;
        this.parameterIndex = 0;
    }

    @Override
    protected void visitPreparedParam(SQLVariantRefExpr x) {
        ParameterContext parameterContext = parameterContextList.get(parameterIndex++);
        ParameterMethod method = parameterContext.getParameterMethod();
        Object value = parameterContext.getValue();
        switch (method) {
        case setInt:
            print(Integer.valueOf(value.toString()));
            break;
        case setLong:
            print(Long.valueOf(value.toString()));
            break;
        case setDouble:
            print(Double.valueOf(value.toString()));
            break;
        case setBytes:
            print(BaseEncoding.base16().decode(value.toString()).toString());
            break;
        default:
            print(value.toString());
            break;
        }
    }

    public static String unparameterizeSql(String sql, List<ParameterContext> params) {
        SQLStatementParser statementParser = createSQLStatementParser(sql, JdbcConstants.MYSQL);
        SQLStatement sqlStatment = statementParser.parseStatement();
        StringBuilder out = new StringBuilder();
        DrdsUnparameterizeSqlVisitor drdsUnparameterizeSqlVisitor = new DrdsUnparameterizeSqlVisitor(
            out,
            false,
            new ExecutionContext(),
            params
        );
        sqlStatment.accept(drdsUnparameterizeSqlVisitor);
        return out.toString();
    }
}
