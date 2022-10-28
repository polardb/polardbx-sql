/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.nio.charset.Charset;
import java.util.List;

/**
 * A <code>SqlDynamicParam</code> represents a dynamic parameter marker in an
 * SQL statement. The textul order in which dynamic parameters appear within an
 * SQL statement is the only property which distinguishes them, so this 0-based
 * index is recorded as soon as the parameter is encountered.
 */
public class SqlDynamicParam extends SqlNode {

  //~ Instance fields --------------------------------------------------------

  //~ Enum
  public enum DYNAMIC_TYPE_VALUE{
    DEFAULT,SINGLE_PARALLEL
  }
  
  protected final int index;
  protected final SqlTypeName typeName;
  protected Object value;
  protected int dynamicKey = -1;
  protected Enum dynamicType = RexDynamicParam.DYNAMIC_TYPE_VALUE.DEFAULT;
  protected Charset charset;
  protected SqlCollation collation;

  //~ Constructors -----------------------------------------------------------

  public SqlDynamicParam(
          int index,
          SqlParserPos pos) {
    this(index, SqlTypeName.ANY, pos, null);
  }

  public SqlDynamicParam(
      int index,
      SqlParserPos pos,
      Object val) {
    this(index, SqlTypeName.ANY, pos, val);
  }

  public SqlDynamicParam(
      int index,
      SqlTypeName typeName,
      SqlParserPos pos) {
    this(index, typeName, pos, null);
  }

  public SqlDynamicParam(
          int index,
          SqlTypeName typeName,
          SqlParserPos pos,
          Object value) {
    super(pos);
    this.index = index;
    this.typeName = typeName;
    this.value = value;
    this.charset = null;
    this.collation = null;
  }

  public SqlDynamicParam(
      int index,
      SqlTypeName typeName,
      SqlParserPos pos,
      Object value,
      Charset charset,
      SqlCollation collation) {
    super(pos);
    this.index = index;
    this.typeName = typeName;
    this.value = value;
    this.charset = charset;
    this.collation = collation;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode clone(SqlParserPos pos) {
    SqlDynamicParam sqlDynamicParam = new SqlDynamicParam(index, typeName, pos, value);
    sqlDynamicParam.setDynamicKey(dynamicKey);
    return sqlDynamicParam;
  }

  public SqlKind getKind() {
    return SqlKind.DYNAMIC_PARAM;
  }

  public int getIndex() {
    return index;
  }

  public SqlTypeName getTypeName() {
    return typeName;
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    if(value!=null||index==-4){
      if(value == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY){
        writer.print("NULL");
        writer.setNeedWhitespace(true);
      }else{
        Object realValue = value;
        if(getTypeName()==SqlTypeName.BOOLEAN&&value instanceof Number){
          realValue = isTrue((Integer)value);
        } else if (realValue instanceof RexFieldAccess) {
          writer.print("?");
          writer.setNeedWhitespace(true);
          return;
        }else if(realValue instanceof List){
          int count=0;
          for(Object tmpValue : (List)realValue){
            if(count++ != 0){
              writer.sep(",");
            }
            if(tmpValue==null){
              writer.print("NULL");
              continue;
            }
            getTypeName().createLiteral(tmpValue, pos).unparse(writer, leftPrec, rightPrec);
          }
          return;
        }
        getTypeName().createLiteral(realValue, pos).unparse(writer, leftPrec, rightPrec);
      }
    } else {
      writer.dynamicParam();
      writer.setNeedWhitespace(true);
    }

  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDynamicParam(this);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlDynamicParam)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlDynamicParam that = (SqlDynamicParam) node;
    if (this.index != that.index) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public boolean isTrue(Integer i) {
    if (i == null) {
      return false;
    }

    return i != 0;
  }

  public Enum getDynamicType() {
    return dynamicType;
  }

  public void setDynamicType(Enum dynamicType) {
    this.dynamicType = dynamicType;
  }

  public Charset getCharset() {
    return charset;
  }

  public SqlCollation getCollation() {
    return collation;
  }

  public int getDynamicKey() {
    return dynamicKey;
  }

  public void setDynamicKey(int dynamicKey) {
    this.dynamicKey = dynamicKey;
  }
}

// End SqlDynamicParam.java
