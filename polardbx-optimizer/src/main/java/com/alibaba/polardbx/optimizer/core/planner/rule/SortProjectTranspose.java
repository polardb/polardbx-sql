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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.Mappings;

/**
 * @author chenmo.cm
 */
class SortProjectTranspose {
    private boolean skipped;
    private Sort sort;
    private Project project;
    private Sort newSort;
    private Project newProject;

    public SortProjectTranspose(Sort sort, Project project) {
        this.sort = sort;
        this.project = project;
    }

    boolean isSkipped() {
        return skipped;
    }

    public Sort getNewSort() {
        return newSort;
    }

    public Project getNewProject() {
        return newProject;
    }

    public SortProjectTranspose invoke() {
        final RelOptCluster cluster = project.getCluster();
        // Determine mapping between project input and output fields. If sort
        // relies on non-trivial expressions, we can't push.
        final Mappings.TargetMapping map = RelOptUtil.permutationIgnoreCast(project.getProjects(),
            project.getInput().getRowType());
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                skipped = true;
                return this;
            }
            final RexNode node = project.getProjects().get(fc.getFieldIndex());
            if (node.isA(SqlKind.CAST)) {
                // Check whether it is a monotonic preserving cast, otherwise we cannot push
                final RexCall cast = (RexCall) node;
                final RexCallBinding binding = RexCallBinding
                    .create(cluster.getTypeFactory(), cast,
                        ImmutableList.of(RelCollations.of(RexUtil.apply(map, fc))));
                if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
                    skipped = true;
                    return this;
                }
            }
        }

        // Build
        final RelCollation newCollation = cluster.traitSet().canonize(RexUtil.apply(map, sort.getCollation()));
        newSort = sort
            .copy(sort.getTraitSet().replace(newCollation), project.getInput(), newCollation, sort.offset,
                sort.fetch);
        newProject = (Project) project.copy(sort.getTraitSet(), ImmutableList.of(newSort));
        skipped = false;
        return this;
    }
}
