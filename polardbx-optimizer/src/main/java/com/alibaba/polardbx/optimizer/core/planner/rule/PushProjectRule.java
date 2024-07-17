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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by lingce.ldm on 2016/11/2.
 */
public class PushProjectRule extends RelOptRule {

    public PushProjectRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushProjectRule:" + description);
    }

    public static final PushProjectRule INSTANCE = new PushProjectRule(operand(Project.class,
        operand(LogicalView.class, none())), "INSTANCE");

    @Override
    public boolean matches(RelOptRuleCall call) {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = (Project) call.rels[0];
        LogicalView logicalView = (LogicalView) call.rels[1];
        PlannerContext context = call.getPlanner().getContext().unwrap(PlannerContext.class);

        if (logicalView instanceof OSSTableScan) {
            if (!((OSSTableScan) logicalView).canPushFilterProject()) {
                return;
            }
            // only push column ref
            PushProjector pushProjector =
                new PushProjector(
                    project, call.builder().getRexBuilder().makeLiteral(true), logicalView,
                    PushProjector.ExprCondition.FALSE, call.builder());
            RelNode pushProjectorResult = pushProjector.convertProject(null);
            if (pushProjectorResult == null || !(pushProjectorResult instanceof Project)) {
                return;
            }
            Project topProject = (Project) pushProjectorResult;
            if (topProject.getInput() instanceof Project) {
                Project refProject = (Project) topProject.getInput();
                if (refProject.getInput() instanceof OSSTableScan) {
                    OSSTableScan ossTableScan = (OSSTableScan) refProject.getInput();

                    LogicalView newOssTableScan = logicalView.copy(ossTableScan.getTraitSet());
                    // push column ref project
                    newOssTableScan.push(refProject);

                    LogicalProject logicalProjectStay =
                        LogicalProject.create(newOssTableScan, topProject.getProjects(), topProject.getRowType());

                    call.transformTo(logicalProjectStay);
                }
            } else if (topProject.getInput() instanceof OSSTableScan) {
                OSSTableScan ossTableScan = (OSSTableScan) topProject.getInput();

                LogicalView newOssTableScan = logicalView.copy(ossTableScan.getTraitSet());
                // push column ref project
                newOssTableScan.push(topProject);
                call.transformTo(newOssTableScan);
                return;
            }
            return;
        }

        if (logicalView instanceof OSSTableScan) {
            if (!((OSSTableScan) logicalView).canPushFilterProject()) {
                return;
            }
            // only push column ref
            PushProjector pushProjector =
                new PushProjector(
                    project, call.builder().getRexBuilder().makeLiteral(true), logicalView,
                    PushProjector.ExprCondition.FALSE, call.builder());
            RelNode pushProjectorResult = pushProjector.convertProject(null);
            if (pushProjectorResult == null || !(pushProjectorResult instanceof Project)) {
                return;
            }
            Project topProject = (Project) pushProjectorResult;
            if (topProject.getInput() instanceof Project) {
                Project refProject = (Project) topProject.getInput();
                if (refProject.getInput() instanceof OSSTableScan) {
                    OSSTableScan ossTableScan = (OSSTableScan) refProject.getInput();
                    LogicalView newOssTableScan = logicalView.copy(ossTableScan.getTraitSet());
                    // push column ref project
                    newOssTableScan.push(refProject);
                    LogicalProject logicalProjectStay =
                        LogicalProject.create(newOssTableScan, topProject.getProjects(), topProject.getRowType());
                    call.transformTo(logicalProjectStay);
                }
            } else if (topProject.getInput() instanceof OSSTableScan) {
                OSSTableScan ossTableScan = (OSSTableScan) topProject.getInput();
                LogicalView newOssTableScan = logicalView.copy(ossTableScan.getTraitSet());
                // push column ref project
                newOssTableScan.push(topProject);
                call.transformTo(newOssTableScan);
                return;
            }
            return;
        }
        final ParamManager paramManager = PlannerContext.getPlannerContext(project).getParamManager();
        boolean enablePushProject = paramManager.getBoolean(ConnectionParams.ENABLE_PUSH_PROJECT);
        if (!enablePushProject) {
            return;
        }

        // 判断是否有需要特殊处理的Project
        if (remainProject(project) || RelUtils.isNotPushLastInsertId(context, project)) {
            // 不下压或者构造新的project下压，保留原先的project

            // if project has subquery with single table && logicalview only possess single table,
            // then try push part of project down
            List<RexNode> subquerys =
                project.getProjects().stream().filter(e -> RexUtil.containsCorrelation(e)).collect(Collectors.toList());
            if (subquerys.size() > 0 && TableTopologyUtil
                .isAllSingleTableInSamePhysicalDB(RelOptUtil.findTables(logicalView))) {

                // find push part of projects
                List<RexNode> pushPart = subquerys.stream().filter(e -> couldPush(e)).collect(Collectors.toList());

                if (pushPart.size() == 0) {
                    return;
                }

                // build logicalproject to pushdown
                List<RexNode> pushPros = Lists.newLinkedList();
                for (int i = 0; i < logicalView.getRowType().getFieldCount(); i++) {
                    pushPros.add(call.builder().getRexBuilder().makeInputRef(logicalView, i));
                }
                pushPros.addAll(pushPart);

                RelNode logicalProjectPush = call.builder().push(logicalView).project(pushPros).build();

                // build logicalproject to stay
                List<RexNode> stayPros = Lists.newLinkedList();
                for (RexNode rex : project.getProjects()) {
                    if (pushPart.contains(rex)) {
                        // change rexnode that will be pushdown to rexinputref
                        stayPros.add(new RexInputRef(pushPros.indexOf(rex),
                            logicalProjectPush.getRowType().getFieldList().get(pushPros.indexOf(rex)).getType()));
                    } else {
                        stayPros.add(rex);
                    }
                }

                LogicalView newLogicalView = logicalView.copy(logicalProjectPush.getTraitSet());
                // push project
                newLogicalView.push(logicalProjectPush);

                LogicalProject logicalProjectStay =
                    LogicalProject.create(newLogicalView, stayPros, project.getRowType());

                call.transformTo(logicalProjectStay);
            } else {
                // try to push column ref
                RelNode pushProjectorResult =
                    new PushProjector(
                        project, call.builder().getRexBuilder().makeLiteral(true), logicalView,
                        PushProjector.ExprCondition.FALSE, call.builder()).convertProject(null);
                if (!(pushProjectorResult instanceof Project)) {
                    return;
                }
                Project topProject = (Project) pushProjectorResult;
                if (topProject.getInput() instanceof Project) {
                    LogicalView newLogicalView = logicalView.copy(project.getTraitSet());
                    // push column ref project
                    newLogicalView.push(topProject.getInput());

                    LogicalProject logicalProjectStay =
                        LogicalProject.create(newLogicalView, topProject.getProjects(), topProject.getRowType());
                    call.transformTo(logicalProjectStay);
                }
            }
        } else {
            LogicalView newLogicalView = logicalView.copy(project.getTraitSet());
            // 下压project
            newLogicalView.push(project);
            RelUtils.changeRowType(newLogicalView, project.getRowType());
            call.transformTo(newLogicalView);
        }
    }

    private boolean couldPush(RexNode e) {
        if (doNotPush(e, false)) {
            return false;
        }
        RexUtil.DynamicFinder dynamicFinder = new RexUtil.DynamicFinder();
        e.accept(dynamicFinder);
        Set<RelOptTable> tables = Sets.newHashSet();
        dynamicFinder.getScalar().stream().map(s -> RelOptUtil.findTables(s.getRel())).forEach(t -> tables.addAll(t));
        dynamicFinder.getCorrelateScalar().stream().map(s -> RelOptUtil.findTables(s.getRel()))
            .forEach(t -> tables.addAll(t));
        return TableTopologyUtil.isAllSingleTableInSamePhysicalDB(tables);
    }

    protected boolean remainProject(Project project) {
        return doNotPush(project);
    }

    public static boolean doNotPush(Project project) {
        return doNotPush(project, false);
    }

    public static boolean doNotPush(Project project, boolean postPlanner) {
        List<RexNode> exps = project.getChildExps();
        for (RexNode node : exps) {
            if (doNotPush(node, postPlanner)) {
                return true;
            }
            if (RexUtil.containsCorrelation(node)) {
                return true;
            }
        }
        return false;
    }

    private static boolean doNotPush(RexNode node, boolean postPlanner) {
        if (node instanceof RexOver) {
            return true;
        }

        if (node.getKind() == SqlKind.ROW) {
            return true;
        }

        if (node instanceof RexCall && ((RexCall) node).getOperator() == TddlOperatorTable.INTERVAL_PRIMARY) {
            return true;
        }

        if (RexUtil.containsUnPushableFunction(node, postPlanner)) {
            return true;
        }
        return false;
    }
}
