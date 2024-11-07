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

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

public class ProjectFoldRule extends RelOptRule {

    public ProjectFoldRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final ProjectFoldRule INSTANCE = new ProjectFoldRule(
        operand(Project.class, RelOptRule.any()), "project_fold");

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(call.rels[0]).getParamManager();
        return paramManager.getBoolean(ConnectionParams.ENABLE_CONSTANT_FOLD);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = (Project) call.rels[0];
        RelNode input = project.getInput();
        RexBuilder rb = call.builder().getRexBuilder();

        ExecutionContext ec = PlannerContext.getPlannerContext(project).getExecutionContext();
        // don't apply this rule if it will be cached
        if (CBOUtil.useColPlanCache(ec)) {
            return;
        }
        RexConstantFoldShuttle rexConstantFoldShuttle =
            new RexConstantFoldShuttle(rb, ec);
        if (project.getProjects() == null) {
            return;
        }
        List<RexNode> foldedProjects = new ArrayList<>(project.getProjects().size());
        boolean changed = false;
        for (RexNode projRex : project.getProjects()) {
            RexNode foldedRex = projRex.accept(rexConstantFoldShuttle);
            if (foldedRex != projRex) {
                changed = true;
            }
            foldedProjects.add(foldedRex);
        }
        if (!changed) {
            return;
        }
        // set constant fold flag to avoid cache this plan
        PlannerContext.getPlannerContext(project).setHasConstantFold(true);
        call.transformTo(project.copy(project.getTraitSet(), input, foldedProjects, project.getRowType()));
    }

    class RexConstantFoldShuttle extends RexShuttle {
        RexBuilder rexBuilder;

        ExecutionContext ec;

        RexConstantFoldShuttle(RexBuilder rexBuilder, ExecutionContext ec) {
            this.rexBuilder = rexBuilder;
            this.ec = ec;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            if (RexUtil.isConstant(call)) {
                Object obj = RexUtils.getEvalFuncExec(call, new ExprContextProvider(ec)).eval(null);
                if (obj instanceof RowValue) {
                    return super.visitCall(call);
                }

                try {
                    switch (call.getType().getSqlTypeName()) {
                    case DATETIME:
                    case TIME:
                    case DATE:
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    case DECIMAL:
                        return super.visitCall(call);
                    default:
                        RexNode node = rexBuilder.makeLiteral(obj, call.getType(), true);
                        if (node.getType().getSqlTypeName().equals(call.getType().getSqlTypeName())) {
                            return node;
                        } else {
                            return super.visitCall(call);
                        }
                    }
                } catch (AssertionError e) {
                    return super.visitCall(call);
                }
            }
            return super.visitCall(call);
        }
    }
}
