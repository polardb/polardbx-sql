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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.index.Index;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.ImmutableIntList;

import java.util.Set;

/**
 * @author dylan
 */
public class MysqlSemiNLJoin extends SemiJoin implements MysqlRel {

    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;

    public MysqlSemiNLJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType,
        boolean semiJoinDone,
        ImmutableList<RelDataTypeField> systemFieldList,
        SqlNodeList hints) {
        super(cluster, traitSet, left, right, condition, leftKeys, rightKeys, variablesSet, joinType, hints);
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Preconditions.checkNotNull(systemFieldList);
    }

    public MysqlSemiNLJoin(RelInput relInput) {
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
        if (relInput.get("systemFields") == null) {
            this.systemFieldList = ImmutableList.of();
        } else {
            this.systemFieldList = (ImmutableList<RelDataTypeField>) relInput.get("systemFields");
        }
        this.semiJoinDone = relInput.getBoolean("semiJoinDone", false);
    }

    public static MysqlSemiNLJoin create(RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
                                         Set<CorrelationId> variablesSet,
                                         JoinRelType joinType, boolean semiJoinDone,
                                         ImmutableList<RelDataTypeField> systemFieldList, SqlNodeList hints) {
        final RelOptCluster cluster = left.getCluster();
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        return new MysqlSemiNLJoin(cluster, traitSet, left, right, condition, joinInfo.leftKeys,
            joinInfo.rightKeys, variablesSet, joinType, semiJoinDone, systemFieldList, hints);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public MysqlSemiNLJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                                RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        MysqlSemiNLJoin mysqlJoin = new MysqlSemiNLJoin(getCluster(),
            traitSet, left, right, conditionExpr, leftKeys, rightKeys,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints);
        return mysqlJoin;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
            .itemIf("semiJoinDone", semiJoinDone, semiJoinDone);
    }

    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }

    @Override
    public ImmutableList<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        String name = "MysqlSemiNLJoin";
        pw.item(RelDrdsWriter.REL_NAME, name);

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        final RelNode buildInput;
        final RelNode streamInput;

        streamInput = left;
        buildInput = right;

        final double streamRowCount = mq.getRowCount(streamInput);
        final double buildRowCount = mq.getRowCount(buildInput);

        if (Double.isInfinite(streamRowCount) || Double.isInfinite(buildRowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }
        double rowCount = streamRowCount + buildRowCount;

        double cpu = buildRowCount * streamRowCount;
        double memory = MemoryEstimator.estimateRowSizeInArrayList(buildInput.getRowType()) * buildRowCount;

        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    @Override
    public Index canUseIndex(RelMetadataQuery mq) {
        return MysqlRel.canUseIndex(left, mq);
    }
}

