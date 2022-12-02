package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;

public class ExitBlockException extends RuntimeException{
    final SQLBlockStatement statement;
    public ExitBlockException(SQLBlockStatement statement) {
        this.statement = statement;
    }

    public SQLBlockStatement getStatement() {
        return statement;
    }
}
