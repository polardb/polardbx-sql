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

package com.alibaba.polardbx.optimizer.core.rel.mpp;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.HASH_CPU_COST;
import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.RANDOM_CPU_COST;
import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.RANGE_PARTITION_CPU_COST;
import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.ROUND_ROBIN_CPU_COST;
import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.SERIALIZE_DESERIALIZE_CPU_COST;
import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.SINGLETON_CPU_COST;

public class MppExchange extends Exchange {
    private MppExchange(
        RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution
        , RelDataType rowType) {
        super(cluster, traitSet, input, distribution);
        assert traitSet.containsIfApplicable(MppConvention.INSTANCE);
        this.rowType = rowType;
    }

    /**
     * Creates a MppExchange by parsing serialized output.
     */
    public MppExchange(RelInput input) {
        super(input);
        traitSet = traitSet.replace(MppConvention.INSTANCE);
        this.rowType = input.getRowType("rowType");
    }

    /**
     * Creates a MppExchange.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    public static MppExchange create(RelNode input,
                                     RelDistribution distribution) {
        RelDataType rowType = input.getRowType();
        RelOptCluster cluster = input.getCluster();
        distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
        RelTraitSet traitSet =
            cluster.traitSet().replace(MppConvention.INSTANCE).replace(distribution);
        return new MppExchange(cluster, traitSet, input, distribution, rowType);
    }

    /**
     * Creates a MppExchange with sort.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     * @param collation Collation specification
     */
    public static MppExchange create(RelNode input,
                                     RelCollation collation,
                                     RelDistribution distribution) {
        RelDataType rowType = input.getRowType();
        RelOptCluster cluster = input.getCluster();
        distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traitSet =
            cluster.traitSet().replace(MppConvention.INSTANCE)
                .replace(distribution)
                .replace(collation);
        return new MppExchange(cluster, traitSet, input, distribution, rowType);
    }

    public boolean isMergeSortExchange() {
        return getTraitSet().getTrait(RelCollationTraitDef.INSTANCE).getFieldCollations().size() > 0;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public MppExchange copy(RelTraitSet traitSet, RelNode newInput,
                            RelDistribution newDistribution) {
        return new MppExchange(getCluster(), traitSet, newInput,
            newDistribution, rowType);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "Exchange");
        pw.input("input", input);
        pw.item("distribution", distribution)
            .item("collation", traitSet.getTrait(RelCollationTraitDef.INSTANCE));
        return pw;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("distribution", distribution)
            .item("collation", traitSet.getTrait(RelCollationTraitDef.INSTANCE))
            .item("rowType", rowType);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    public RelCollation getCollation() {
        return traitSet.simplify().getTrait(RelCollationTraitDef.INSTANCE);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        long rowSize = TableScanIOEstimator.estimateRowSize(getInput().getRowType());
        RelOptCostFactory costFactory = planner.getCostFactory();
        int parallelism =
            PlannerContext.getPlannerContext(this).getParamManager().getInt(ConnectionParams.BROADCAST_SHUFFLE_PARALLELISM);
        double cpuCost;
        switch (distribution.getType()) {
        case SINGLETON:
            cpuCost = (SINGLETON_CPU_COST + SERIALIZE_DESERIALIZE_CPU_COST) * rowCount;
            break;
        case RANDOM_DISTRIBUTED:
            cpuCost = (RANDOM_CPU_COST + SERIALIZE_DESERIALIZE_CPU_COST) * rowCount;
            break;
        case RANGE_DISTRIBUTED:
            cpuCost = (RANGE_PARTITION_CPU_COST + SERIALIZE_DESERIALIZE_CPU_COST) * rowCount;
            break;
        case BROADCAST_DISTRIBUTED:
            cpuCost = parallelism * SERIALIZE_DESERIALIZE_CPU_COST * rowCount;
            rowCount *= parallelism;
            break;
        case HASH_DISTRIBUTED:
            cpuCost =
                (HASH_CPU_COST + Math.exp(1.0 / distribution.getKeys().size() - 1) + SERIALIZE_DESERIALIZE_CPU_COST)
                    * rowCount;
            break;
        case ROUND_ROBIN_DISTRIBUTED:
            cpuCost = (ROUND_ROBIN_CPU_COST + SERIALIZE_DESERIALIZE_CPU_COST) * rowCount;
            break;
        case ANY:
            cpuCost = SERIALIZE_DESERIALIZE_CPU_COST * rowCount;
            break;
        default:
            cpuCost = SERIALIZE_DESERIALIZE_CPU_COST * rowCount;
        }
        return costFactory.makeCost(rowCount, cpuCost, 0, 0,
            Math.ceil(rowSize * rowCount / CostModelWeight.NET_BUFFER_SIZE));
    }

    @Override
    public boolean isEnforcer() {
        return true;
    }
}
