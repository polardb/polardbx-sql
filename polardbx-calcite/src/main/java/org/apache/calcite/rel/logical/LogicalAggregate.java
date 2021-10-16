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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>LogicalAggregate</code> is a relational operator which eliminates
 * duplicates and computes totals.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}.
 * </ul>
 */
public final class LogicalAggregate extends Aggregate {
  /** use for agg optimization */
  private final AggOptimizationContext aggOptimizationContext = new AggOptimizationContext();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalAggregate.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster  Cluster that this relational expression belongs to
   * @param child    input relational expression
   * @param groupSet Bit set of grouping fields
   * @param groupSets Grouping sets, or null to use just {@code groupSet}
   * @param aggCalls Array of aggregates to compute, not null
   */
  public LogicalAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
  }

  @Deprecated // to be removed before 2.0
  public LogicalAggregate(
      RelOptCluster cluster,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    this(cluster, cluster.traitSetOf(Convention.NONE), child, indicator,
        groupSet, groupSets, aggCalls);
  }

  /**
   * Creates a LogicalAggregate by parsing serialized output.
   */
  public LogicalAggregate(RelInput input) {
    super(input);
  }

  /** Creates a LogicalAggregate. */
  public static LogicalAggregate create(final RelNode input,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return create_(input, false, groupSet, groupSets, aggCalls);
  }

  @Deprecated // to be removed before 2.0
  public static LogicalAggregate create(final RelNode input,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return create_(input, indicator, groupSet, groupSets, aggCalls);
  }

  private static LogicalAggregate create_(final RelNode input,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalAggregate(cluster, traitSet, input, indicator, groupSet,
        groupSets, aggCalls);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public LogicalAggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
                               List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    LogicalAggregate logicalAggregate = new LogicalAggregate(getCluster(),
            traitSet,
            input,
            indicator,
            groupSet,
            groupSets,
            aggCalls);
    logicalAggregate.getAggOptimizationContext().copyFrom(this.getAggOptimizationContext());
    return logicalAggregate;
  }

  public LogicalAggregate copy(RelNode input, ImmutableBitSet groupSet, List<AggregateCall> aggCalls) {
    return copy(traitSet, input, indicator, groupSet, null, aggCalls);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  public AggOptimizationContext getAggOptimizationContext() {
    return aggOptimizationContext;
  }

  public RelWriter explainTermsForDisplay(RelWriter pw) {
    pw.item(RelDrdsWriter.REL_NAME, "LogicalAgg");

    List<String> groupList = new ArrayList<String>(groupSet.length());

    for (int groupIndex : groupSet.asList()) {
      RexExplainVisitor visitor = new RexExplainVisitor(this);
      groupList.add(visitor.getField(groupIndex).getKey());
    }
    pw.itemIf("group", StringUtils.join(groupList, ","), !groupList.isEmpty());
    int groupCount = groupSet.cardinality();
    for (int i = groupCount; i < getRowType().getFieldCount(); i++) {
      RexExplainVisitor visitor = new RexExplainVisitor(this);
      aggCalls.get(i - groupCount).accept(visitor);
      pw.item(rowType.getFieldList().get(i).getKey(), visitor.toSqlString());
    }

    return pw;
  }

}

// End LogicalAggregate.java
