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
package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Planner rule that pushes a {@link Project}
 * past a {@link Filter}.
 */
public class ProjectCorrelateTransposeRule extends RelOptRule {
    public static final ProjectCorrelateTransposeRule INSTANCE = new ProjectCorrelateTransposeRule(
        LogicalProject.class, Correlate.class, RelFactories.LOGICAL_BUILDER);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectFilterTransposeRule.
     * <p>
     * preserved in the projection
     */
    public ProjectCorrelateTransposeRule(
        Class<? extends Project> projectClass,
        Class<? extends Correlate> correlateClass,
        RelBuilderFactory relBuilderFactory) {
        this(
            operand(
                projectClass,
                operand(correlateClass, any())),
            relBuilderFactory);
    }

    protected ProjectCorrelateTransposeRule(RelOptRuleOperand operand,
                                            RelBuilderFactory relBuilderFactory) {
        super(operand, relBuilderFactory, null);
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    public void onMatch(RelOptRuleCall call) {
        Project origProj = call.rel(0);
        LogicalCorrelate correlate = call.rel(1);

        RelBuilder builder = call.builder();
        RexBuilder rexBuilder = builder.getRexBuilder();

        List<Integer> indexList =
            RexUtil.findAllIndex(origProj.getProjects()).stream().map(rexInputRef -> rexInputRef.getIndex())
                .collect(Collectors.toList());
        indexList.addAll(correlate.getRequiredColumns().asList());
        indexList.addAll(
            RexUtil.findAllIndex(correlate.getLeftConditions()).stream().map(rexInputRef -> rexInputRef.getIndex())
                .collect(Collectors.toList()));

        TreeSet<Integer> indexSorted = new TreeSet(indexList);

        indexSorted.remove(correlate.getLeft().getRowType().getFieldCount());

        List<RexNode> pushedPros =
            indexSorted.stream().map(index -> rexBuilder.makeInputRef(correlate.getLeft(), index))
                .collect(Collectors.toList());
        if (pushedPros.size() == 0 || RexUtil.isIdentity(pushedPros, correlate.getLeft().getRowType())) {
            return;
        }

        LogicalProject logicalProject = (LogicalProject) builder.push(correlate.getLeft()).project(pushedPros).build();

        Mappings.TargetMapping
            mapping =
            Project.getPartialMapping(correlate.getLeft().getRowType().getFieldCount(), logicalProject.getProjects());
        Map<Integer, Integer> map = Maps.newHashMap();
        mapping.forEach(intPair -> map.put(intPair.source, intPair.target));

        RelNode right = RelUtils.replaceAccessField(RelUtils.removeHepRelVertex(correlate.getRight()),
            (RexCorrelVariable) rexBuilder.makeCorrel(logicalProject.getRowType(), correlate.getCorrelationId()),
            map);

        List<Integer> newRequiredCollumns =
            correlate.getRequiredColumns().asList().stream().map(integer -> map.get(integer))
                .collect(Collectors.toList());

        LogicalCorrelate newCorrelate = LogicalCorrelate
            .create(logicalProject, right, correlate.getCorrelationId(), ImmutableBitSet.of(newRequiredCollumns),
                correlate.getLeftConditions().stream().map(rexNode -> RexUtil.shift(rexNode, map))
                    .collect(Collectors.toList()), correlate.getOpKind(), correlate.getJoinType());

        map.put(correlate.getLeft().getRowType().getFieldCount(), newCorrelate.getLeft().getRowType().getFieldCount());
        List<RexNode> topPros =
            origProj.getProjects().stream().map(rexNode -> RexUtil.shift(rexNode, map)).collect(Collectors.toList());
        RelNode topProject = builder.push(newCorrelate)
            .project(topPros)
            .build();

        call.transformTo(topProject);
    }
}

