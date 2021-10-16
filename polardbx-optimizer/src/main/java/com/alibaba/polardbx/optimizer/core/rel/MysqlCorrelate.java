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
import com.alibaba.polardbx.optimizer.index.Index;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * @author fangwu
 */
public class MysqlCorrelate extends Correlate implements MysqlRel {

    private List<RexNode> leftConditions;

    public MysqlCorrelate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        // TODO use condtion to cal the cost of correlate node
        List<RexNode> leftConditions,
        CorrelationId correlationId,
        ImmutableBitSet requiredColumns, SemiJoinType joinType) {
        super(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
        this.leftConditions = leftConditions;
    }

    @Override
    public Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId,
                          ImmutableBitSet requiredColumns, SemiJoinType joinType) {
        return new MysqlCorrelate(getCluster(), traitSet, left, right, this.leftConditions, correlationId,
            requiredColumns,
            joinType);
    }

    public static MysqlCorrelate create(RelTraitSet traitSet,
                                        RelNode left,
                                        RelNode right,
                                        // TODO use condtion to cal the cost of correlate node
                                        List<RexNode> leftConditions,
                                        CorrelationId correlationId,
                                        ImmutableBitSet requiredColumns, SemiJoinType joinType) {
        return new MysqlCorrelate(left.getCluster(), traitSet, left, right, leftConditions, correlationId,
            requiredColumns,
            joinType);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        String name = "MysqlCorrelate";
        pw.item(RelDrdsWriter.REL_NAME, name);

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        leftConditions.stream().forEach(r -> r.accept(visitor));
        return pw.item("leftConditions", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase());
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

