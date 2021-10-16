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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class RFilterProjectTransposeRule extends RelOptRule {

    public static final RFilterProjectTransposeRule INSTANCE =
        new RFilterProjectTransposeRule(Filter.class, Project.class,
            RelFactories.LOGICAL_BUILDER);

    public RFilterProjectTransposeRule(
        Class<? extends Filter> filterClass,
        Class<? extends Project> projectClass,
        RelBuilderFactory relBuilderFactory) {
        super(
            operand(filterClass,
                operand(projectClass, any())), relBuilderFactory, null);
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final Project project = call.rel(1);

        if (RexOver.containsOver(project.getProjects(), null)) {
            return;
        }

        final List<RexNode> pushedConditions = Lists.newArrayList();
        final List<RexNode> remainingConditions = Lists.newArrayList();
        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());

        for (RexNode condition : conditions) {
            if (isBloomFilterCondition(condition)) {
                RexNode newCondition =
                    RelOptUtil.pushPastProject(condition, project);
                pushedConditions.add(newCondition);
            } else {
                remainingConditions.add(condition);
            }
        }

        if (pushedConditions.size() > 0) {
            RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            RelNode outerOfProject = project.getInput();
            LogicalFilter newLogicalFilter = LogicalFilter.create(
                outerOfProject, RexUtil.composeConjunction(rexBuilder, pushedConditions, true));

            final RelBuilder relBuilder = call.builder();
            RelNode ret = relBuilder.push(newLogicalFilter)
                .project(project.getProjects(), project.getRowType().getFieldNames())
                .build();
            if (remainingConditions.size() > 0) {
                ret = LogicalFilter.create(
                    ret, RexUtil.composeConjunction(rexBuilder, remainingConditions, true));
            }
            call.transformTo(ret);
        }
    }

    private boolean isBloomFilterCondition(RexNode condition) {
        if (condition instanceof RexCall && (((RexCall) condition).getOperator() instanceof SqlRuntimeFilterFunction)) {
            return true;
        } else {
            return false;
        }
    }
}