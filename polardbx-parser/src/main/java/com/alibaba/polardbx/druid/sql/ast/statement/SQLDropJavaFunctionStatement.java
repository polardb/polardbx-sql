package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLReplaceable;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;

public class SQLDropJavaFunctionStatement extends SQLStatementImpl implements SQLDropStatement, SQLReplaceable {
  //JAVA_FUNC TODO
  @Override
  public boolean replace(SQLExpr expr, SQLExpr target) {
    return false;
  }
}
