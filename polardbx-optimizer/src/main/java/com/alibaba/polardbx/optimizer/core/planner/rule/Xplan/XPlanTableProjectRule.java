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

package com.alibaba.polardbx.optimizer.core.planner.rule.Xplan;

import com.alibaba.polardbx.optimizer.core.planner.Xplanner.XPlanUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class XPlanTableProjectRule extends RelOptRule {

    public XPlanTableProjectRule(RelOptRuleOperand operand, String description) {
        super(operand, "XPlan_rule:" + description);
    }

    public static final XPlanTableProjectRule INSTANCE = new XPlanTableProjectRule(
        operand(LogicalProject.class, operand(XPlanTableScan.class, none())), "project_tableScan_to_tableProject");

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = (LogicalProject) call.rels[0];
        XPlanTableScan tableScan = (XPlanTableScan) call.rels[1];

        if (XPlanUtil.isSimpleProject(project)) {
            if (tableScan.getProjects().isEmpty()) {
                tableScan.getProjects().addAll(project.getChildExps().stream()
                    .map(expr -> ((RexInputRef) expr).getIndex())
                    .collect(Collectors.toList()));
            } else {
                // Replace it.
                final List<Integer> projects = project.getChildExps().stream()
                    .map(expr -> tableScan.getProjects().get(((RexInputRef) expr).getIndex()))
                    .collect(Collectors.toList());
                tableScan.getProjects().clear();
                tableScan.getProjects().addAll(projects);
            }
            RelUtils.changeRowType(tableScan, project.getRowType());
            // Remove input to prevent infinite loop on project.
            RelUtils.changeRowType(project.getInput(), null);
            call.transformTo(tableScan);
        }
    }
}
