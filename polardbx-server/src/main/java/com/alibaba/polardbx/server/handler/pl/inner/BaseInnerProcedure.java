package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * @author yaozhili
 */
public abstract class BaseInnerProcedure {

    /**
     * @param cursor results returned to user
     */
    abstract void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor);
}
