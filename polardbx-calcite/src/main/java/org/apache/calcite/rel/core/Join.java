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
package org.apache.calcite.rel.core;

import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public abstract class Join extends BiRel {
  //~ Instance fields --------------------------------------------------------
  protected final RexNode condition;
  protected final ImmutableSet<CorrelationId> variablesSet;

  /**
   * Values must be of enumeration {@link JoinRelType}, except that
   * {@link JoinRelType#RIGHT} is disallowed.
   */
  protected final JoinRelType joinType;

  //~ Constructors -----------------------------------------------------------

  // Next time we need to change the constructor of Join, let's change the
  // "Set<String> variablesStopped" parameter to
  // "Set<CorrelationId> variablesSet".

  /**
   * Creates a Join.
   *
   * This constructor is not deprecated, for now, because maintaining overloaded
   * constructors in multiple sub-classes would be onerous.
   *
   * @param cluster          Cluster
   * @param traitSet         Trait set
   * @param left             Left input
   * @param right            Right input
   * @param condition        Join condition
   * @param joinType         Join type
   * @param variablesSet     Set variables that are set by the
   *                         LHS and used by the RHS and are not available to
   *                         nodes above this Join in the tree
   */
  protected Join(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType,
      SqlNodeList hints) {
    super(cluster, traitSet, left, right);
    this.condition = Preconditions.checkNotNull(condition);
    this.variablesSet = ImmutableSet.copyOf(variablesSet);
    this.joinType = Preconditions.checkNotNull(joinType);
    this.hints = hints;
  }

  protected Join(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
      this(cluster, traitSet, left, right, condition, variablesSet, joinType, new SqlNodeList(SqlParserPos.ZERO));
  }

  @Deprecated // to be removed before 2.0
  protected Join(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType,
      Set<String> variablesStopped) {
    this(cluster, traitSet, left, right, condition,
        CorrelationId.setOf(variablesStopped), joinType, new SqlNodeList(SqlParserPos.ZERO));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(condition);
  }

  @Override public RelNode accept(RexShuttle shuttle) {
    RexNode condition = shuttle.apply(this.condition);
    if (this.condition == condition) {
      return this;
    }
    return copy(traitSet, condition, left, right, joinType, isSemiJoinDone());
  }

  public RexNode getCondition() {
    return condition;
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  @Override public boolean isValid(Litmus litmus, Context context) {
    if (!super.isValid(litmus, context)) {
      return false;
    }
    if (getRowType().getFieldCount() != getSystemFieldList().size() + left.getRowType().getFieldCount() + ((
            this.getJoinType() == JoinRelType.SEMI || this.getJoinType() == JoinRelType.ANTI) ? 0 :
            (this instanceof SemiJoin) && this.getJoinType() == JoinRelType.LEFT ? 1 : right.getRowType()
                    .getFieldCount())) {
      return litmus.fail("field count mismatch");
    }
    if (condition != null) {
      if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN && condition.getType().getSqlTypeName() != SqlTypeName.BIGINT) {
        return litmus.fail("condition must be boolean: {}",
            condition.getType());
      }
      // The input to the condition is a row type consisting of system
      // fields, left fields, and right fields. Very similar to the
      // output row type, except that fields have not yet been made due
      // due to outer joins.
      RexChecker checker =
          new RexChecker(
              getCluster().getTypeFactory().builder()
                  .addAll(getSystemFieldList())
                  .addAll(getLeft().getRowType().getFieldList())
                  .addAll(getRight().getRowType().getFieldList())
                  .build(),
              context, litmus);
      condition.accept(checker);
      if (checker.getFailureCount() > 0) {
        return litmus.fail(checker.getFailureCount()
            + " failures in condition " + condition);
      }
    }
    return litmus.succeed();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // REVIEW jvs 9-Apr-2006:  Just for now...
    double rowCount = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, 0, 0,0,0);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return Util.first(RelMdUtil.getJoinRowCount(mq, this, condition), 1D);
  }

  @Override public Set<CorrelationId> getVariablesSet() {
    return variablesSet;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("condition", condition)
        .item("joinType", joinType.name())
        .itemIf(
            "systemFields",
            getSystemFieldList(),
            !getSystemFieldList().isEmpty())
        .itemIf("variablesSet",
            variablesSet,
            variablesSet != null && variablesSet.size() > 0);
  }

//  @API(since = "1.24", status = API.Status.INTERNAL)
  @EnsuresNonNullIf(expression = "#1", result = true)
  protected boolean deepEquals0(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Join o = (Join) obj;
    return traitSet.equals(o.traitSet)
        && left.deepEquals(o.left)
        && right.deepEquals(o.right)
        && condition.equals(o.condition)
        && joinType == o.joinType
        && getRowType().equalsSansFieldNames(o.getRowType());
  }

//  @API(since = "1.24", status = API.Status.INTERNAL)
  protected int deepHashCode0() {
    return Objects.hash(traitSet,
        left.deepHashCode(), right.deepHashCode(),
        condition, joinType, hints);
  }

  @Override protected RelDataType deriveRowType() {
    return SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
        right.getRowType(), joinType, getCluster().getTypeFactory(), null,
        getSystemFieldList());
  }

  /**
   * Returns whether this LogicalJoin has already spawned a
   * {@link SemiJoin} via
   * {@link org.apache.calcite.rel.rules.JoinAddRedundantSemiJoinRule}.
   *
   * <p>The base implementation returns false.</p>
   *
   * @return whether this join has already spawned a semi join
   */
  public boolean isSemiJoinDone() {
    return false;
  }

  /**
   * Returns a list of system fields that will be prefixed to
   * output row type.
   *
   * @return list of system fields
   */
  public List<RelDataTypeField> getSystemFieldList() {
    return Collections.emptyList();
  }

  @Override public final Join copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return copy(traitSet, getCondition(), inputs.get(0), inputs.get(1),
        joinType, isSemiJoinDone());
  }

  @Override
  public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    if(condition==null){
      return;
    }
    Set<RexFieldAccess> rexFieldAccesses = Sets.newHashSet();
    rexFieldAccesses.addAll(RexUtil.findFieldAccessesDeep(condition));
    rexFieldAccesses.stream().map(RexFieldAccess::getReferenceExpr).map(rex -> ((RexCorrelVariable) rex).getId())
        .forEach(id -> variableSet.add(id));
  }

  /**
   * Creates a copy of this join, overriding condition, system fields and
   * inputs.
   *
   * <p>General contract as {@link RelNode#copy}.
   *
   * @param traitSet      Traits
   * @param conditionExpr Condition
   * @param left          Left input
   * @param right         Right input
   * @param joinType      Join type
   * @param semiJoinDone  Whether this join has been translated to a
   *                      semi-join
   * @return Copy of this join
   */
  public abstract Join copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone);

  /**
   * Analyzes the join condition.
   *
   * @return Analyzed join condition
   */
  public JoinInfo analyzeCondition() {
    return JoinInfo.of(left, right, condition);
  }

  public boolean emptyHints() {
    return (null == hints) || (hints.size() <= 0);
  }

  public RelNode getOuter() {
    return joinType != JoinRelType.RIGHT ? left : right;
  }

  public RelNode getInner() {
    return joinType != JoinRelType.RIGHT ? right : left;
  }
}

// End Join.java
