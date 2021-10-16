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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.mapping.Mappings;

public class DrdsSortProjectTransposeRule extends RelOptRule {
    public static final DrdsSortProjectTransposeRule INSTANCE =
        new DrdsSortProjectTransposeRule(Sort.class, Project.class,
            RelFactories.LOGICAL_BUILDER, "DrdsSortProjectTransposeRule");

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a DrdsSortProjectTransposeRule.
     */
    public DrdsSortProjectTransposeRule(
        Class<? extends Sort> sortClass,
        Class<? extends Project> projectClass,
        RelBuilderFactory relBuilderFactory, String description) {
        this(operand(sortClass, operand(projectClass, any())), relBuilderFactory, description);
    }

    /**
     * Creates a DrdsSortProjectTransposeRule with an operand.
     */
    protected DrdsSortProjectTransposeRule(RelOptRuleOperand operand,
                                           RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        return (sort instanceof LogicalSort && sort.withLimit());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final Project project = call.rel(1);
        final RelOptCluster cluster = project.getCluster();

        if (sort.getConvention() != project.getConvention()) {
            return;
        }

        if (project.getCluster().getMetadataQuery().collations(project).size() != 0) {
            return;
        }

        // Determine mapping between project input and output fields. If sort
        // relies on non-trivial expressions, we can't push.
        final Mappings.TargetMapping map =
            RelOptUtil.permutationIgnoreCast(
                project.getProjects(), project.getInput().getRowType());
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                return;
            }
            final RexNode node = project.getProjects().get(fc.getFieldIndex());
            if (node.isA(SqlKind.CAST)) {
                // Check whether it is a monotonic preserving cast, otherwise we cannot push
                final RexCall cast = (RexCall) node;
                final RexCallBinding binding =
                    RexCallBinding.create(cluster.getTypeFactory(), cast,
                        ImmutableList.of(RelCollations.of(RexUtil.apply(map, fc))));
                if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
                    return;
                }
            }
        }
        final RelCollation newCollation =
            cluster.traitSet().canonize(
                RexUtil.apply(map, sort.getCollation()));
        final Sort newSort =
            sort.copy(
                sort.getTraitSet().replace(newCollation),
                project.getInput(),
                newCollation,
                sort.offset,
                sort.fetch);
        RelNode newProject =
            project.copy(
                sort.getTraitSet(),
                ImmutableList.<RelNode>of(newSort));

        call.transformTo(newProject);
    }
}

// End SortProjectTransposeRule.java
