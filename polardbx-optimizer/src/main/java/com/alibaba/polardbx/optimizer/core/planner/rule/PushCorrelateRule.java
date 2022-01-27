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

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.PUSH_CORRELATE_MATERIALIZED_LIMIT;
import static org.apache.calcite.sql.SqlKind.DML;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW;

/**
 * @author fangwu
 */
public class PushCorrelateRule extends RelOptRule {

    public PushCorrelateRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushCorrelateRule:" + description);
    }

    public static final PushCorrelateRule INSTANCE = new PushCorrelateRule(
        operand(Correlate.class, some(operand(LogicalView.class, none()), operand(RelNode.class, any()))),
        "INSTANCE");

    @Override
    public boolean matches(RelOptRuleCall call) {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalCorrelate logicalCorrelate = (LogicalCorrelate) call.rels[0];
        final LogicalView leftView = (LogicalView) call.rels[1];
        RelNode rightPlan = call.rels[2];
        RelBuilder relBuilder = call.builder();

        if (PlannerContext.getPlannerContext(rightPlan).getSqlKind().belongsTo(DML)) {
            return;
        }

        // single table pushdown
        boolean allSingleTable = false;
        Set<RelOptTable> tables = RelOptUtil.findTables(leftView.getPushedRelNode());
        tables.addAll(RelOptUtil.findTables(rightPlan));
        RelNode subqueryRel = rightPlan;
        if (!RelUtils.isAllSingleTableInSameSchema(tables)) {
            // meaning has correlate columns
            if (RelOptUtil.getVariablesUsed(rightPlan).size() > 0) {
                return;
            }

            // right plan should contains correlate node
            if (RelOptUtil.findCorrelates(rightPlan).size() > 0) {
                return;
            }

            // for IN subquery
            ParamManager paramManager = PlannerContext.getPlannerContext(rightPlan).getParamManager();
            int limit = paramManager.getInt(PUSH_CORRELATE_MATERIALIZED_LIMIT);
            if (logicalCorrelate.getJoinType() == SemiJoinType.SEMI) {
                if (logicalCorrelate.getLeftConditions().size() != 1 ||
                    logicalCorrelate.getOpKind() != EQUALS ||
                    logicalCorrelate.getRight().getRowType().getFieldList().get(0).getType().getSqlTypeName()
                        == SqlTypeName.FLOAT ||
                    subqueryRel.estimateRowCount(subqueryRel.getCluster().getMetadataQuery()) > limit) {
                    return;
                }
                // optimize path for in subquery
            } else if (logicalCorrelate.getJoinType() == SemiJoinType.ANTI) {
                // not support pushing down anti subquery
                return;
            }
        } else {
            allSingleTable = true;
        }

        rightPlan = RelUtils.removeHepRelVertex(rightPlan);
        LogicalView newLogicalView = leftView.copy(leftView.getTraitSet().replace(Convention.NONE));
        relBuilder.push(newLogicalView);
        List<RexNode> projects = (List<RexNode>) relBuilder.getRexBuilder().identityProjects(leftView.getRowType());
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        rightPlan =
            Planner.getInstance().optimizeBySqlWriter(rightPlan, PlannerContext.getPlannerContext(rightPlan));
        RexDynamicParam rexDynamicParam =
            relBuilder.getRexBuilder()
                .makeDynamicParam(logicalCorrelate.getJoinType() == SemiJoinType.LEFT ?
                        relBuilder.getTypeFactory()
                            .createTypeWithNullability(rightPlan.getRowType().getFieldList().get(0).getType(), true) :
                        logicalCorrelate.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT), -2,
                    rightPlan);

        rexDynamicParam.setSemiType(logicalCorrelate.getJoinType());

        if (allSingleTable) {
            // LeftCondition and opKind need to be separeted when transforming correlate to subquery
            // This reversion need these two attributes in order to let physical sql working.
            rexDynamicParam.setLeftCondition(logicalCorrelate.getLeftConditions());
            rexDynamicParam.setSubqueryKind(logicalCorrelate.getOpKind());
        }
        builder.addAll(projects);
        // `not all single table` case do not support correlate columns

        ParamManager paramManager = PlannerContext.getPlannerContext(rightPlan).getParamManager();
        int limit = paramManager.getInt(PUSH_CORRELATE_MATERIALIZED_LIMIT);
        if (logicalCorrelate.getLeftConditions() != null &&
            logicalCorrelate.getLeftConditions().size() == 1 &&
            logicalCorrelate.getJoinType() == SemiJoinType.SEMI &&
            logicalCorrelate.getOpKind() == EQUALS &&
            subqueryRel.estimateRowCount(subqueryRel.getCluster().getMetadataQuery()) < limit
        ) {
            // Materialized optimize option for IN subquery
            if (logicalCorrelate.getJoinType() == SemiJoinType.SEMI) {
                RexBuilder rexBuilder = relBuilder.getRexBuilder();
                rexDynamicParam =
                    (RexDynamicParam) rexBuilder
                        .makeDynamicParam(rexDynamicParam.getRel().getRowType().getFieldList().get(0).getType(),
                            rexDynamicParam.getIndex(), rexDynamicParam.getRel()
                            , rexDynamicParam.getSubqueryOperands(), rexDynamicParam.getSubqueryOp(),
                            rexDynamicParam.getSubqueryKind());
                rexDynamicParam.setSemiType(SemiJoinType.LEFT);
                rexDynamicParam.setMaxOnerow(false);
                builder.add(rexBuilder.makeCall(IN, logicalCorrelate.getLeftConditions().get(0),
                    rexBuilder.makeCall(ROW, rexDynamicParam)));
            }
        } else {
            builder.add(rexDynamicParam);
        }

        List<RexNode> newProjects = builder.build();

        if (allSingleTable) {
            call.transformTo(relBuilder
                .project(newProjects, Collections.emptyList(), ImmutableSet.of(logicalCorrelate.getCorrelationId()))
                .build());
        } else {
            call.transformTo(relBuilder
                .project(newProjects, Collections.emptyList())
                .build());
        }
    }
}
