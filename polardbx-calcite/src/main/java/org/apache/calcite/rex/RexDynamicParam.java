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

  // index for IN expr
  private int subIndex = -1;

  // the col index of ROW expr
  private int skIndex = -1;
  private       RelNode rel;
  private       Object  value;
  private       boolean maxOnerow = true;
  private ImmutableList<RexNode> subqueryOperands;
  private SqlOperator subqueryOp;
  private SqlKind subqueryKind;
  private SemiJoinType semiType;
  private Enum dynamicType = DYNAMIC_TYPE_VALUE.DEFAULT;
  private List<RexNode> leftCondition;
  private String dynamicRexName;

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
    this.dynamicRexName = this.name;
  }

  public RexDynamicParam(
          RelDataType type,
          int index,Enum dynamicType) {
    super("?" + index, type);
    this.index = index;
    assert dynamicType != null;
    this.dynamicType = dynamicType;
    this.dynamicRexName = this.name;
  }

  public RexDynamicParam(
          RelDataType type,
          int index,
          RelNode rel) {
    super(buildDigiestWithSubQuery(index, rel, null, null, null, DYNAMIC_TYPE_VALUE.DEFAULT), type);
    this.index = index;
    this.rel = rel;
    this.dynamicType = DYNAMIC_TYPE_VALUE.DEFAULT;
    this.dynamicRexName = this.name;
  }

  public RexDynamicParam(
      RelDataType type,
      int index,
      Enum dynamicType,
      RelNode rel) {
    super(buildDigiestWithSubQuery(index, rel, null, null, null, dynamicType), type);
    this.index = index;
    this.rel = rel;
    this.dynamicType = dynamicType;
    this.dynamicRexName = this.name;
  }

  public RexDynamicParam(RelDataType type, int index, RelNode rel, ImmutableList<RexNode> operands, SqlOperator op,
                         SqlKind kind) {
    super(buildDigiestWithSubQuery(index, rel, operands, op, kind, DYNAMIC_TYPE_VALUE.DEFAULT), type);
    this.index = index;
    this.rel = rel;
    this.subqueryOperands = operands;
    this.subqueryOp = op;
    this.subqueryKind = kind;
    this.dynamicType = DYNAMIC_TYPE_VALUE.DEFAULT;
    this.dynamicRexName = this.name;
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
    this.digest = buildDigiestWithSubQuery(index, this.rel, null, null, null, this.dynamicType);
    this.dynamicRexName = this.digest;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    if (getSubIndex() == -1) {
      return getName();
    } else {
      return getName() + ":" + getSubIndex() + ( getSkIndex() >= 0 ? ":" + getSkIndex() : "") ;
    }
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

  public int getSubIndex() {
    return subIndex;
  }

  public void setSubIndex(int subIndex) {
    this.subIndex = subIndex;
  }

  public int getSkIndex() {
    return skIndex;
  }

  public void setSkIndex(int skIndex) {
    this.skIndex = skIndex;
  }

  @Override
  public String getName() {
    return this.dynamicRexName;
  }

  /**
   * @return true if actual value of this parameter is a literal value of user input
   */
  public boolean literal() {
    return true;
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
    REX_CALL,

    /**
     * Only used as a temporary variable by performing SubQuery-In Pruning of new Partitioned table,
     * the value type of SUBQUERY_TEMP_VAR means the value of this RexDynamicParam
     * is from the ec.ScalarSubQueryExecContext.nextValue() instead of fetching from ec.getParams()
     * <pre>
     *     e.g The shard predicate "shardKey in (select a from tbl order by limit 5)" are as followed steps:
     *     =>1. build: shardKey in (scalarSubQuery), scalarSubQuery="select a from tbl order by limit 5"
     *     =>2. eval subQuery value, shardKey in (scalarSubQuery),
     *            scalarSubQuery="1,2,3,4,5" which values are saved in ec.ScalarSubQueryExecContext
     *     =>3. pruning: shardKey in (scalarSubQuery),scalarSubQuery="1,2,3,4,5"
     *            for each val in scalarSubQuery
     *              SUBQUERY_TEMP_VAR=val
     *              partBitSet |= pruning(shardKey=(SUBQUERY_TEMP_VAR)),
     *                  which SUBQUERY_TEMP_VAR is from  ec.ScalarSubQueryExecContext
     *           return partBitSet
     * </pre>
     */
    SUBQUERY_TEMP_VAR
  }

  private static String buildDigiestWithSubQuery(int index,
                                                 RelNode rel,
                                                 ImmutableList<RexNode> operands,
                                                 SqlOperator op,
                                                 SqlKind kind,
                                                 Enum dynamicType) {

    StringBuilder sb = new StringBuilder();
    if ( dynamicType != null && dynamicType == DYNAMIC_TYPE_VALUE.SUBQUERY_TEMP_VAR) {
      sb.append("#tmpVar@(relId=").append(rel.getId()).append(")");
      return sb.toString();
    }

    if (index == -2) {
      sb.append("scalarSubQueryParam");
    } else if (index == -3) {
      sb.append("applySubQueryParam");
    }
    sb.append("[");
    if (rel != null) {
      sb.append("relId=").append(rel.getId());
      sb.append(",relatedId=").append(rel.getRelatedId());
    }

    if (op != null) {
      sb.append(",");
      sb.append("op=").append(op);
    }

    if (kind != null) {
      sb.append(",");
      sb.append("kind=").append(kind);
    }

    if (operands != null) {
      sb.append(",");
      sb.append("rexOp=[");
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append(operands.get(i).digest);
      }
      sb.append("]");
    }
    sb.append("]");
    return sb.toString();
  }
}

// End RexDynamicParam.java
