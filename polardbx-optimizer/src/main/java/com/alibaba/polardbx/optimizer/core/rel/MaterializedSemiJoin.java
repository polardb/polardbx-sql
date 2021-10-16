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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;
import java.util.Set;

public class MaterializedSemiJoin extends SemiJoin implements LookupJoin {

    private RelNode pushDownRelNode;
    private SqlOperator operator;
    private List<RexNode> operands;
    private String subqueryPosition;
    private RelOptCost fixedCost;
    private boolean distinctInput = true;
    private RelOptCost lookupCost;
    private Index lookupIndex;

    // ~ Constructors -----------------------------------------------------------

    public MaterializedSemiJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        JoinRelType joinRelType,
        List<RexNode> operands,
        Set<CorrelationId> variablesSet,
        SqlNodeList hints,
        SqlOperator operator,
        RelNode pushDownRelNode,
        String subqueryPosition,
        boolean distinctInput) {
        super(
            cluster,
            traitSet,
            left,
            right,
            condition,
            leftKeys,
            rightKeys,
            variablesSet,
            joinRelType,
            hints);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
        this.operands = operands;
        this.operator = operator;
        this.pushDownRelNode = pushDownRelNode;
        this.subqueryPosition = subqueryPosition;
        this.distinctInput = distinctInput;
    }

    public static MaterializedSemiJoin create(
        RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, LogicalSemiJoin semiJoin,
        boolean distinctInput) {
        final RelOptCluster cluster = left.getCluster();
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        return new MaterializedSemiJoin(
            cluster,
            traitSet,
            left,
            right,
            condition,
            joinInfo.leftKeys,
            joinInfo.rightKeys,
            semiJoin.getJoinType(),
            semiJoin.getOperands(),
            semiJoin.getVariablesSet(),
            semiJoin.getHints(),
            semiJoin.getOperator(),
            semiJoin.getPushDownRelNode(),
            semiJoin.getSubqueryPosition(),
            distinctInput);
    }

    public MaterializedSemiJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            JoinInfo.of(relInput.getInputs().get(0), relInput.getInputs().get(1),
                relInput.getExpression("condition")).leftKeys,
            JoinInfo.of(relInput.getInputs().get(0), relInput.getInputs().get(1),
                relInput.getExpression("condition")).rightKeys,
            ImmutableSet.<CorrelationId>of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
        if (relInput.get("operands") == null) {
            this.operands = ImmutableList.of();
        } else {
            this.operands = relInput.getExpressionList("operands");
        }
        this.distinctInput = relInput.getBoolean("distinctInput", true);
        if (this.getLeft() instanceof Gather) {
            ((Gather) this.getLeft()).setJoin(this);
        } else if (this.getLeft() instanceof LogicalView) {
            ((LogicalView) this.getLeft()).setJoin(this);
        } else if (this.getLeft() instanceof Project) {
            RelNode node = ((BKAJoin) ((Project) this.getLeft()).getInput()).getOuter();
            if (node instanceof Gather) {
                node = ((Gather) node).getInput();
            }
            ((LogicalIndexScan) node).setJoin(this);
        }
    }

    @Override
    public MaterializedSemiJoin copy(
        RelTraitSet traitSet,
        RexNode condition,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone) {
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        MaterializedSemiJoin materializedSemiJoin =
            new MaterializedSemiJoin(
                getCluster(),
                traitSet,
                left,
                right,
                condition,
                joinInfo.leftKeys,
                joinInfo.rightKeys,
                joinType,
                operands,
                variablesSet,
                hints,
                operator,
                pushDownRelNode,
                subqueryPosition,
                distinctInput);
        materializedSemiJoin.setFixedCost(this.fixedCost);
        return materializedSemiJoin;
    }

    public void setFixedCost(RelOptCost fixedCost) {
        this.fixedCost = fixedCost;
    }

    public RelOptCost getFixedCost() {
        return fixedCost;
    }

    public boolean isDistinctInput() {
        return distinctInput;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (fixedCost != null) {
            return fixedCost;
        }
        final double leftRowCount = mq.getRowCount(left);
        final double rightRowCount;
        if (distinctInput) {
            Double distinctRowCount =
                mq.getDistinctRowCount(right, ImmutableBitSet.range(0, right.getRowType().getFieldCount()), null);
            rightRowCount = distinctRowCount == null ? mq.getRowCount(right) : distinctRowCount;
        } else {
            rightRowCount = mq.getRowCount(right);
        }

        PlannerContext plannerContext = PlannerContext.getPlannerContext(this);

        // Prefer not to use materialized semi-join when inner side is too large
        final int materializedItemsLimit =
            plannerContext.getParamManager().getInt(ConnectionParams.MATERIALIZED_ITEMS_LIMIT);
        if (rightRowCount > materializedItemsLimit && joinType == JoinRelType.ANTI) {
            return planner.getCostFactory().makeHugeCost();
        }

        double rowCount = leftRowCount + rightRowCount;
        int batchSize = plannerContext.getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);
        double driveSideRowCount = rightRowCount;
        RelOptCost lookupSideCost = getLookupCost(mq);
        RelDataType driveRowType = right.getRowType();
        double lookupRowCount = mq.getRowCount(left);
        RelDataType lookupRowType = left.getRowType();

        double cpu = driveSideRowCount * lookupSideCost.getCpu();
        double memory = driveSideRowCount * MemoryEstimator.estimateRowSizeInArrayList(driveRowType);
        double io;
        if (joinType == JoinRelType.ANTI) {
            io = lookupSideCost.getIo();
        } else {
            io = Math.ceil(driveSideRowCount * lookupSideCost.getIo() / CostModelWeight.LOOKUP_NUM_PER_IO);
        }

        Index index = getLookupIndex();
        double selectivity = 1;
        if (index != null) {
            selectivity = index.getJoinSelectivity();
        }

        if (joinType == JoinRelType.ANTI) {
            batchSize = (int) driveSideRowCount;
        }

        double lookupCount = Math.ceil(driveSideRowCount / batchSize);
        double eachLookupNet = Math.ceil(batchSize * TableScanIOEstimator.estimateRowSize(
            lookupRowType) * lookupRowCount * selectivity / CostModelWeight.NET_BUFFER_SIZE) + lookupSideCost.getNet();
        double net = lookupCount * eachLookupNet;
        net -= lookupSideCost.getNet(); // minus lookupSide net to correct cumulative cost

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, io, net);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("operands", operands, operands != null && !operands.isEmpty())
            .item("distinctInput", distinctInput);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MaterializedSemiJoin");

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    @Override
    public RelOptCost getLookupCost(RelMetadataQuery mq) {
        if (lookupCost != null) {
            return lookupCost;
        }
        lookupCost = LookupJoin.getLookupCost(mq, left);
        return lookupCost;
    }

    @Override
    public Index getLookupIndex() {
        if (lookupIndex != null) {
            return lookupIndex;
        }
        lookupIndex = IndexUtil.selectJoinIndex(this);
        return lookupIndex;
    }
}
