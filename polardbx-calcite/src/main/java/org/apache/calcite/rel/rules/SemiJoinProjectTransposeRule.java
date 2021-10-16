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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.SemiJoin} down in a tree past
 * a {@link org.apache.calcite.rel.core.Project}.
 *
 * <p>The intention is to trigger other rules that will convert
 * {@code SemiJoin}s.
 *
 * <p>SemiJoin(LogicalProject(X), Y) &rarr; LogicalProject(SemiJoin(X, Y))
 *
 * @see org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule
 */
public class SemiJoinProjectTransposeRule extends RelOptRule {
  public static final SemiJoinProjectTransposeRule INSTANCE =
      new SemiJoinProjectTransposeRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SemiJoinProjectTransposeRule.
   */
  private SemiJoinProjectTransposeRule(RelBuilderFactory relBuilderFactory) {
    super(
        operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            some(operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalSemiJoin logicalSemiJoin = call.rel(0);
    return logicalSemiJoin.getJoinType() == JoinRelType.SEMI || logicalSemiJoin.getJoinType() == JoinRelType.ANTI;
  }

  public void onMatch(RelOptRuleCall call) {
    LogicalSemiJoin logicalSemiJoin = call.rel(0);
    LogicalProject project = call.rel(1);

    // Convert the LHS semi-join keys to reference the child projection
    // expression; all projection expressions must be RexInputRefs,
    // otherwise, we wouldn't have created this semi-join.
    final List<Integer> newLeftKeys = new ArrayList<>();
    final List<Integer> leftKeys = logicalSemiJoin.getLeftKeys();
    final List<RexNode> projExprs = project.getProjects();
    for (int leftKey : leftKeys) {
      RexInputRef inputRef = (RexInputRef) projExprs.get(leftKey);
      newLeftKeys.add(inputRef.getIndex());
    }

    // convert the semijoin condition to reflect the LHS with the project
    // pulled up
    RexNode newCondition = adjustCondition(project, logicalSemiJoin, logicalSemiJoin.getCondition());
    List<RexNode> newOperands = adjustOperands(project, logicalSemiJoin, logicalSemiJoin.getOperands());
    SemiJoin newLogicalSemiJoin = logicalSemiJoin.copy(logicalSemiJoin.getTraitSet(), newCondition, project.getInput(),
            logicalSemiJoin.getRight(), logicalSemiJoin.getJoinType(), logicalSemiJoin.isSemiJoinDone(), newOperands);

    // Create the new projection.  Note that the projection expressions
    // are the same as the original because they only reference the LHS
    // of the semijoin and the semijoin only projects out the LHS
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(newLogicalSemiJoin);
    relBuilder.project(projExprs, project.getRowType().getFieldNames());

    call.transformTo(relBuilder.build());
  }

  private List<RexNode> adjustOperands(LogicalProject project, LogicalSemiJoin logicalSemiJoin, List<RexNode> operands) {
    if (operands == null) {
      return null;
    }
    List<RexNode> newOperands = Lists.newArrayList();
    for (RexNode rexNode : operands) {
      // FIXME hack adjustCondition by mocking as a equal rexCall
      RexNode hackCondition = logicalSemiJoin.getCluster().getRexBuilder()
              .makeCall(SqlStdOperatorTable.AND, Arrays.asList(rexNode, rexNode));
      RexNode newHackCondition = adjustCondition(project, logicalSemiJoin, hackCondition);
      assert newHackCondition instanceof RexCall;
      RexNode newRexNode = ((RexCall)newHackCondition).getOperands().get(0);
      newOperands.add(newRexNode);
    }
    return newOperands;
  }

  /**
   * Pulls the project above the semijoin and returns the resulting semijoin
   * condition. As a result, the semijoin condition should be modified such
   * that references to the LHS of a semijoin should now reference the
   * children of the project that's on the LHS.
   *
   * @param project  LogicalProject on the LHS of the semijoin
   * @param semiJoin the semijoin
   * @param semiJoin the condition
   * @return the modified condition
   */
  private RexNode adjustCondition(LogicalProject project, LogicalSemiJoin semiJoin, RexNode condition) {
    // create two RexPrograms -- the bottom one representing a
    // concatenation of the project and the RHS of the semijoin and the
    // top one representing the semijoin condition

    RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelNode rightChild = semiJoin.getRight();

    // for the bottom RexProgram, the input is a concatenation of the
    // child of the project and the RHS of the semijoin
    RelDataType bottomInputRowType =
        SqlValidatorUtil.deriveJoinRowType(
            project.getInput().getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            semiJoin.getSystemFieldList());
    RexProgramBuilder bottomProgramBuilder =
        new RexProgramBuilder(bottomInputRowType, rexBuilder);

    // add the project expressions, then add input references for the RHS
    // of the semijoin
    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      bottomProgramBuilder.addProject(pair.left, pair.right);
    }
    int nLeftFields = project.getInput().getRowType().getFieldCount();
    List<RelDataTypeField> rightFields =
        rightChild.getRowType().getFieldList();
    int nRightFields = rightFields.size();
    for (int i = 0; i < nRightFields; i++) {
      final RelDataTypeField field = rightFields.get(i);
      RexNode inputRef =
          rexBuilder.makeInputRef(
              field.getType(), i + nLeftFields);
      bottomProgramBuilder.addProject(inputRef, field.getName());
    }
    RexProgram bottomProgram = bottomProgramBuilder.getProgram();

    // input rowtype into the top program is the concatenation of the
    // project and the RHS of the semijoin
    RelDataType topInputRowType =
        SqlValidatorUtil.deriveJoinRowType(
            project.getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            semiJoin.getSystemFieldList());
    RexProgramBuilder topProgramBuilder =
        new RexProgramBuilder(
            topInputRowType,
            rexBuilder);
    topProgramBuilder.addIdentity();
    topProgramBuilder.addCondition(condition);
    RexProgram topProgram = topProgramBuilder.getProgram();

    // merge the programs and expand out the local references to form
    // the new semijoin condition; it now references a concatenation of
    // the project's child and the RHS of the semijoin
    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    return mergedProgram.expandLocalRef(
        mergedProgram.getCondition());
  }
}

// End SemiJoinProjectTransposeRule.java
