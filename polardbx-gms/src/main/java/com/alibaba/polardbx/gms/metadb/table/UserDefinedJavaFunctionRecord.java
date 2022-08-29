package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UserDefinedJavaFunctionRecord implements SystemTableRecord {

  public String funcName;
  public String className;
  public String code;
  public String codeLanguage;
  public String inputTypes;
  public String resultType;


  @Override
  public UserDefinedJavaFunctionRecord fill(ResultSet rs) throws SQLException {
    this.funcName = rs.getString("func_name");
    this.className = rs.getString("class_name");
    this.code = rs.getString("code");
    this.codeLanguage = rs.getString("code_language");
    this.inputTypes = rs.getString("input_types");
    this.resultType = rs.getString("result_type");

    return this;
  }
}
