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
package org.apache.calcite.rel.logical;

import com.google.common.collect.Sets;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Set;

/**
 * A relational operator that performs nested-loop joins.
 *
 * <p>It behaves like a kind of {@link org.apache.calcite.rel.core.Join},
 * but works by setting variables in its environment and restarting its
 * right-hand input.
 *
 * <p>A LogicalCorrelate is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 *
 * @see org.apache.calcite.rel.core.CorrelationId
 */
public final class LogicalCorrelate extends Correlate {
  //~ Instance fields --------------------------------------------------------
  /**
   * LeftCondition and opKind need to be separeted when transforming correlate to subquery .
   */
  private List<RexNode> leftConditions;
  private SqlKind opKind;
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalCorrelate.
   * @param cluster      cluster this relational expression belongs to
   * @param left         left input relational expression
   * @param right        right input relational expression
   * @param correlationId variable name for the row of left input
   * @param requiredColumns Required columns
   * @param joinType     join type
   */
  public LogicalCorrelate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      List<RexNode> leftConditions,
      SqlKind opKind,
      SemiJoinType joinType) {
    super(
        cluster,
        traitSet,
        left,
        right,
        correlationId,
        requiredColumns,
        joinType);
    this.opKind = opKind;
    this.leftConditions = leftConditions;
  }

  @Deprecated // to be removed before 2.0
  public LogicalCorrelate(RelOptCluster cluster, RelNode left, RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns, List<RexNode> leftConditions, SqlKind opKind, SemiJoinType joinType) {
    this(cluster, cluster.traitSetOf(Convention.NONE), left, right,
        correlationId, requiredColumns, leftConditions, opKind, joinType);
  }

  /**
   * Creates a LogicalCorrelate by parsing serialized output.
   */
  public LogicalCorrelate(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
        input.getInputs().get(1),
        new CorrelationId((Integer) input.get("correlation")),
        input.getBitSet("requiredColumns"),
        input.getExpressionList("leftConditions"),
        input.getString("opKind")!=null?input.getEnum("opKind", SqlKind.class):null,
        input.getEnum("joinType", SemiJoinType.class));
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("leftConditions", leftConditions)
        .item("opKind", opKind);
  }

  /** Creates a LogicalCorrelate. */
  public static LogicalCorrelate create(RelNode left, RelNode right,
      CorrelationId correlationId, ImmutableBitSet requiredColumns, List<RexNode> leftConditions, SqlKind opKind,
      SemiJoinType joinType) {
    final RelOptCluster cluster = left.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalCorrelate(cluster, traitSet, left, right, correlationId,
        requiredColumns, leftConditions, opKind, joinType);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalCorrelate copy(RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, SemiJoinType joinType) {
    return new LogicalCorrelate(getCluster(), traitSet, left, right,
        correlationId, requiredColumns, leftConditions, opKind, joinType);
  }

  @Override
  public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    if (leftConditions == null) {
      return;
    }
    Set<RexFieldAccess> rexFieldAccesses = Sets.newHashSet();
    leftConditions.stream().forEach(condition->rexFieldAccesses.addAll(RexUtil.findFieldAccessesDeep(condition)));
    rexFieldAccesses.stream().map(RexFieldAccess::getReferenceExpr).map(rex -> ((RexCorrelVariable) rex).getId())
        .forEach(id -> variableSet.add(id));

    if(leftConditions!=null&&leftConditions.size()>0){
      variableSet.add(correlationId);
    }
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelWriter explainTermsForDisplay(RelWriter pw) {
    pw.item(RelDrdsWriter.REL_NAME, "CorrelateApply");
    pw.item("cor", correlationId);
    pw.item("leftConditions", leftConditions);
    pw.item("opKind", opKind);
    pw.item("type", joinType);
    return pw;
   }

    public List<RexNode> getLeftConditions() {
        return leftConditions;
    }

    public SqlKind getOpKind() {
        return opKind;
    }
}

// End LogicalCorrelate.java
