/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Planner rule that pushes a {@link Filter}
 * past a {@link Window}.
 *
 * @see AggregateFilterTransposeRule
 */
public class FilterWindowTransposeRule extends RelOptRule {

  /** The default instance of
   * {@link FilterWindowTransposeRule}.
   *
   * <p>It matches any kind of agg. or filter */
  public static final FilterWindowTransposeRule INSTANCE =
      new FilterWindowTransposeRule(Filter.class,
          RelFactories.LOGICAL_BUILDER, Window.class);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterWindowTransposeRule.
   *
   * <p>If {@code filterFactory} is null, creates the same kind of filter as
   * matched in the rule. Similarly {@code aggregateFactory}.</p>
   */
  public FilterWindowTransposeRule(
      Class<? extends Filter> filterClass,
      RelBuilderFactory builderFactory,
      Class<? extends Window> windowClass) {
    this(
        operand(filterClass,
            operand(windowClass, any())),
        builderFactory);
  }

  protected FilterWindowTransposeRule(RelOptRuleOperand operand,
                                      RelBuilderFactory builderFactory) {
    super(operand, builderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public FilterWindowTransposeRule(
      Class<? extends Filter> filterClass,
      RelFactories.FilterFactory filterFactory,
      Class<? extends Window> windowClass) {
    this(filterClass, RelBuilder.proto(Contexts.of(filterFactory)),
            windowClass);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final Window window = call.rel(1);

    final List<RexNode> conditions =
        RelOptUtil.conjunctions(filterRel.getCondition());
    final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    final List<RelDataTypeField> origFields =
            window.getRowType().getFieldList();
    final int[] adjustments = new int[origFields.size()];
    final List<RexNode> pushedConditions = Lists.newArrayList();
    final List<RexNode> remainingConditions = Lists.newArrayList();

    for (RexNode condition : conditions) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
      if (canPush(window, rCols)) {
        pushedConditions.add(condition.accept(new RelOptUtil.RexInputConverter(rexBuilder,
                origFields,
                window.getInput(0).getRowType().getFieldList(),
                adjustments)));
      } else {
        remainingConditions.add(condition);
      }
    }

    final RelBuilder builder = call.builder();
    RelNode rel =
        builder.push(window.getInput()).filter(pushedConditions).build();
    if (rel == window.getInput(0)) {
      return;
    }
    rel = window.copy(window.getTraitSet(), ImmutableList.of(rel));
    rel = builder.push(rel).filter(remainingConditions).build();
    call.transformTo(rel);
  }

  private boolean canPush(Window window, ImmutableBitSet rCols) {
    if(rCols.get(window.getRowType().getFieldCount()-1)){
      return false;
    }
    return true;
  }
}

// End FilterAggregateTransposeRule.java
