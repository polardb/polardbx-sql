package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;

public class ExceptionHandler {
    final protected MySqlHandlerType type;
    final protected SQLStatement statement;

    public ExceptionHandler(MySqlHandlerType type, SQLStatement statement) {
        this.type = type;
        this.statement = statement;
    }

    public MySqlHandlerType getType() {
        return type;
    }

    public SQLStatement getStatement() {
        return statement;
    }
}
