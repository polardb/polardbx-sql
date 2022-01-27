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
package org.apache.calcite.rex;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.charset.CharsetName;
import io.airlift.slice.Slice;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * Dynamic parameter reference in a row-expression.
 */
public class RexDynamicParam extends RexVariable {
  //~ Instance fields --------------------------------------------------------

  /**
   * When
   *  index >= 0, it means the content of RexDynamicParam is the params value can be fetched from ExecutionContext
   *  index = -1, it means the content of RexDynamicParam is phy table name;
   *  index = -2, it means the content of RexDynamicParam is scalar subquery;
   *  index = -3, it means the content of RexDynamicParam is apply subquery.
   */
  private final int     index;
  private       RelNode rel;
  private       Object  value;
  private       boolean maxOnerow = true;
  private ImmutableList<RexNode> subqueryOperands;
  private SqlOperator subqueryOp;
  private SqlKind subqueryKind;
  private SemiJoinType semiType;
  private Enum dynamicType = DYNAMIC_TYPE_VALUE.DEFAULT;
  private List<RexNode> leftCondition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a dynamic parameter.
   *
   * @param type  inferred type of parameter
   * @param index 0-based index of dynamic parameter in statement
   */
  public RexDynamicParam(
      RelDataType type,
      int index) {
    super("?" + index, type);
    this.index = index;
  }

  public RexDynamicParam(
          RelDataType type,
          int index,Enum dynamicType) {
    super("?" + index, type);
    this.index = index;
    assert dynamicType != null;
    this.dynamicType = dynamicType;
  }

  public RexDynamicParam(
          RelDataType type,
          int index,
          RelNode rel) {
    super("subquery", type);
    this.index = index;
    this.rel = rel;
  }

  public RexDynamicParam(RelDataType type, int index, RelNode rel, ImmutableList<RexNode> operands, SqlOperator op,
                         SqlKind kind) {
    super("subquery", type);
    this.index = index;
    this.rel = rel;
    this.subqueryOperands = operands;
    this.subqueryOp = op;
    this.subqueryKind = kind;
  }

    //~ Methods ----------------------------------------------------------------

  public SqlKind getKind() {
    return SqlKind.DYNAMIC_PARAM;
  }

  public int getIndex() {
    return index;
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitDynamicParam(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitDynamicParam(this, arg);
  }

  public RelNode getRel() {
    return rel;
  }

  public void setRel(RelNode rel) {
    this.rel = rel;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    if(value==null){
      this.value = DYNAMIC_SPECIAL_VALUE.EMPTY;
    }else if (value instanceof Slice){
      this.value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
    }else if (value instanceof UInt64){
      this.value = ((UInt64) value).toBigInteger();
    }else{
      this.value = value;
    }
  }

  public SqlKind getSubqueryKind() {
    return subqueryKind;
  }

  public SqlOperator getSubqueryOp() {
    return subqueryOp;
  }

  public ImmutableList<RexNode> getSubqueryOperands(){
    return subqueryOperands;
  }

  public Enum getDynamicType() {
    return dynamicType;
  }

  public SemiJoinType getSemiType() {
    return semiType;
  }

  public void setSemiType(SemiJoinType semiType) {
    this.semiType = semiType;
  }

  public List<RexNode> getLeftCondition() {
    return leftCondition;
  }

  public void setLeftCondition(List<RexNode> leftCondition) {
    this.leftCondition = leftCondition;
  }

  public void setSubqueryKind(SqlKind subqueryKind) {
    this.subqueryKind = subqueryKind;
  }

  public boolean isMaxOnerow() {
    return maxOnerow;
  }

  public void setMaxOnerow(boolean maxOnerow) {
    this.maxOnerow = maxOnerow;
  }

  //~ Enum
  public enum DYNAMIC_SPECIAL_VALUE{
    EMPTY,
  }

  //~ Enum
  public enum DYNAMIC_TYPE_VALUE{
    DEFAULT,
    /**
     * represents the dynamic type is a segmented parallel query type
     */
    SINGLE_PARALLEL,
    SEQUENCE,
    REX_CALL
  }

}

// End RexDynamicParam.java
