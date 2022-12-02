package com.alibaba.polardbx.druid.sql.parser;

import com.alibaba.polardbx.druid.FastsqlException;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnsupportedStatement;

public class SQLUnsupportedException extends FastsqlException {

    private static final long serialVersionUID = 1L;

    public static final String UNSUPPORTED_SQL_WARN = "unsupported sql : ";

    public SQLUnsupportedException(SQLUnsupportedStatement sqlUnsupportedStatement) {
        super(UNSUPPORTED_SQL_WARN + sqlUnsupportedStatement.toString());
    }

    public SQLUnsupportedException(ByteString sql) {
        super(UNSUPPORTED_SQL_WARN + sql.toString());
    }

    public SQLUnsupportedException(String sql) {
        super(UNSUPPORTED_SQL_WARN + sql);
    }

}
