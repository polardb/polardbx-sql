package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectWithDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;

public class SQLCreateJavaFunctionStatement extends SQLStatementImpl implements SQLCreateStatement, SQLObjectWithDataType {
  //JAVA_FUNC TODO
  @Override
  public SQLDataType getDataType() {
    return null;
  }

  @Override
  public void setDataType(SQLDataType dataType) {

  }
}
