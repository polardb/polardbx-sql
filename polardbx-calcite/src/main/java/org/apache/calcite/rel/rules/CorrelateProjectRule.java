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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.SemiJoinType.LEFT;

/**
 * Planner rule that pushes a {@link Filter} above a {@link Correlate} into the
 * inputs of the Correlate.
 */
public class CorrelateProjectRule extends RelOptRule {

  public static final CorrelateProjectRule INSTANCE =
      new CorrelateProjectRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterCorrelateRule.
   */
  public CorrelateProjectRule(RelBuilderFactory builderFactory) {
    super(
        operand(Correlate.class,
            operand(Project.class, RelOptRule.any())),
        builderFactory, "FilterCorrelateRule");
  }

  /**
   * Creates a FilterCorrelateRule with an explicit root operand and
   * factories.
   */
  @Deprecated // to be removed before 2.0
  public CorrelateProjectRule(RelFactories.FilterFactory filterFactory,
                              RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(filterFactory, projectFactory));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalCorrelate corr = call.rel(0);
    final Project project = call.rel(1);

    if(corr.getLeftConditions()!=null||corr.getJoinType()!=LEFT){
      return;
    }
    Correlate newCorr = LogicalCorrelate.create(project.getInput(), corr.getRight(), corr.getCorrelationId(), corr.getRequiredColumns(), null, null, LEFT);

    List<RexNode> newPros = Lists.newArrayList();
    for(int i=0;i<project.getProjects().size();i++){
      RexNode r = project.getProjects().get(i);
      if(r instanceof RexInputRef&&((RexInputRef) r).getIndex()==i){
        newPros.add(r);
      }else{
        return;
      }
    }
    RelBuilder relBuilder = call.builder().push(newCorr);
    newPros.add(relBuilder.field(newCorr.getRowType().getFieldCount()-1));
    relBuilder.project(newPros);
    call.transformTo(relBuilder.build());
  }
}

// End FilterCorrelateRule.java
