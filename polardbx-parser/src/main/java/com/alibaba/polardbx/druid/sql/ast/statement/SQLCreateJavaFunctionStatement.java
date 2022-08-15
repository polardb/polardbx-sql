package com.alibaba.polardbx.druid.sql.ast.statement;


import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectWithDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLCreateJavaFunctionStatement extends SQLStatementImpl implements SQLCreateStatement, SQLObjectWithDataType {
  public SQLCreateJavaFunctionStatement() {

  }
  protected SQLName name;
  protected String  javaCode;
  protected String returnType;
  protected String inputType;

  @Override
  public void accept0(SQLASTVisitor visitor) {
    if (visitor.visit(this)) {
      acceptChild(visitor, name);
    }
    visitor.endVisit(this);
  }

  public String getJavaCode() {
    return javaCode;
  }

  public void setJavaCode(String javaCode) {
    this.javaCode = javaCode;
  }

  public SQLName getName() {
    return name;
  }

  public void setName(SQLName name) {
    this.name = name;
  }

  public String getReturnType() {
    return returnType;
  }

  public void setReturnType(String returnType) {
    this.returnType = returnType;
  }

  public String getInputType() {
    return inputType;
  }

  public void setInputType(String inputType) {
    this.inputType = inputType;
  }

  @Override
  public SQLDataType getDataType() {
    return null;
  }

  @Override
  public void setDataType(SQLDataType dataType) {

  }
}
