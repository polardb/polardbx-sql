package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLReplaceable;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLDropJavaFunctionStatement extends SQLStatementImpl implements SQLDropStatement, SQLReplaceable {

  protected SQLName           name;
  private boolean             ifExists;

  public SQLDropJavaFunctionStatement() {

  }

  @Override
  protected void accept0(SQLASTVisitor visitor) {
    if (visitor.visit(this)) {
      acceptChild(visitor, name);
    }
    visitor.endVisit(this);
  }

  public SQLName getName() {
    return name;
  }

  public void setName(SQLName name) {
    this.name = name;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }

  @Override
  public boolean replace(SQLExpr expr, SQLExpr target) {
    return false;
  }
}
