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

import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author chenzilin
 */
public class OrcTableScanRule extends RelOptRule {

    public static final OrcTableScanRule PROJECT_FILTER_TABLESCAN = new OrcTableScanRule(
        operand(LogicalProject.class, operand(LogicalFilter.class, operand(LogicalTableScan.class, RelOptRule.none()))),
        "OrcTableScanRule:PROJECT_FILTER_TABLESCAN", true, true, false);

    public static final OrcTableScanRule PROJECT_FILTER_PROJECT_TABLESCAN = new OrcTableScanRule(
        operand(LogicalProject.class, operand(LogicalFilter.class,
            operand(LogicalProject.class, operand(LogicalTableScan.class, RelOptRule.none())))),
        "OrcTableScanRule:PROJECT_FILTER_PROJECT_TABLESCAN", true, true, true);

    public static final OrcTableScanRule FILTER_PROJECT_TABLESCAN = new OrcTableScanRule(
        operand(LogicalFilter.class, operand(LogicalProject.class, operand(LogicalTableScan.class, RelOptRule.none()))),
        "OrcTableScanRule:PROJECT_FILTER_TABLESCAN", false, true, true);

    public static final OrcTableScanRule PROJECT_TABLESCAN = new OrcTableScanRule(
        operand(LogicalProject.class, operand(LogicalTableScan.class, RelOptRule.none())),
        "OrcTableScanRule:PROJECT_TABLESCAN", false, false, true);

    public static final OrcTableScanRule FILTER_TABLESCAN = new OrcTableScanRule(
        operand(LogicalFilter.class, operand(LogicalTableScan.class, RelOptRule.none())),
        "OrcTableScanRule:FILTER_TABLESCAN", false, true, false);

    public static final OrcTableScanRule TABLESCAN = new OrcTableScanRule(
        operand(LogicalTableScan.class, RelOptRule.none()),
        "OrcTableScanRule:TABLESCAN", false, false, false);

    private boolean withInProject;

    private boolean withFilter;

    private boolean withOutProject;

    protected OrcTableScanRule(RelOptRuleOperand operand, String description, boolean withOutProject,
                               boolean withFilter, boolean withInProject) {
        super(operand, description);
        this.withOutProject = withOutProject;
        this.withFilter = withFilter;
        this.withInProject = withInProject;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OrcTableScan orcTableScan;
        if (withInProject && withFilter && !withOutProject) {
            LogicalFilter logicalFilter = call.rel(0);
            LogicalProject logicalProject = call.rel(1);
            LogicalTableScan logicalTableScan = call.rel(2);
            orcTableScan = OrcTableScan.create(logicalTableScan.getCluster(),
                logicalTableScan.getTable(), new ArrayList<>(), logicalFilter.getChildExps(),
                getColumnRefProjects(logicalProject.getProjects()), logicalTableScan.getHints(),
                logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                logicalTableScan.getFlashbackOperator(), logicalTableScan.getPartitions());
        } else if (!withInProject && withFilter && !withOutProject) {
            LogicalFilter logicalFilter = call.rel(0);
            LogicalTableScan logicalTableScan = call.rel(1);
            orcTableScan = OrcTableScan.create(logicalTableScan.getCluster(),
                logicalTableScan.getTable(), new ArrayList<>(), logicalFilter.getChildExps(), new ArrayList<>(),
                logicalTableScan.getHints(),
                logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                logicalTableScan.getFlashbackOperator(), logicalTableScan.getPartitions());
        } else if (withInProject && !withFilter && !withOutProject) {
            LogicalProject logicalProject = call.rel(0);
            LogicalTableScan logicalTableScan = call.rel(1);
            orcTableScan = OrcTableScan.create(logicalTableScan.getCluster(),
                logicalTableScan.getTable(), new ArrayList<>(), new ArrayList<>(),
                getColumnRefProjects(logicalProject.getProjects()), logicalTableScan.getHints(),
                logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                logicalTableScan.getFlashbackOperator(), logicalTableScan.getPartitions());
        } else if (!withInProject && !withFilter && !withOutProject) {
            LogicalTableScan logicalTableScan = call.rel(0);
            orcTableScan =
                OrcTableScan.create(logicalTableScan.getCluster(), logicalTableScan.getTable(), new ArrayList<>(),
                    new ArrayList<>(), new ArrayList<>(),
                    logicalTableScan.getHints(), logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                    logicalTableScan.getFlashbackOperator(),
                    logicalTableScan.getPartitions());
        } else if (!withInProject && withFilter && withOutProject) {
            LogicalProject logicalProject = call.rel(0);
            LogicalFilter logicalFilter = call.rel(1);
            LogicalTableScan logicalTableScan = call.rel(2);
            orcTableScan = OrcTableScan.create(logicalTableScan.getCluster(),
                logicalTableScan.getTable(), getColumnRefProjects(logicalProject.getProjects()),
                logicalFilter.getChildExps(), new ArrayList<>(), logicalTableScan.getHints(),
                logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                logicalTableScan.getFlashbackOperator(), logicalTableScan.getPartitions());
        } else if (withInProject && withFilter && withOutProject) {
            LogicalProject outProject = call.rel(0);
            LogicalFilter logicalFilter = call.rel(1);
            LogicalProject inProject = call.rel(2);
            LogicalTableScan logicalTableScan = call.rel(3);
            orcTableScan = OrcTableScan.create(logicalTableScan.getCluster(),
                logicalTableScan.getTable(), getColumnRefProjects(outProject.getProjects()),
                logicalFilter.getChildExps(), getColumnRefProjects(inProject.getProjects()),
                logicalTableScan.getHints(),
                logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                logicalTableScan.getFlashbackOperator(), logicalTableScan.getPartitions());
        } else {
            throw new AssertionError("impossible case");
        }
        call.transformTo(orcTableScan);
    }

    private List<Integer> getColumnRefProjects(List<RexNode> projects) {
        return projects.stream().map(expr -> ((RexInputRef) expr).getIndex()).collect(Collectors.toList());
    }
}


