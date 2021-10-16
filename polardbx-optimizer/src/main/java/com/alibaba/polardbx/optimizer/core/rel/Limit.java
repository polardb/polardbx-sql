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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.CPU_START_UP_COST;

/**
 * Limit & Fetch Operator
 *
 */
public class Limit extends Sort {

    private Limit(RelOptCluster cluster, RelTraitSet traitSet,
                  RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        // can not copy collation to traitSet[collation] here, because a limit may have traitSet[collation] while no collation
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
    }

    public static Limit create(RelTraitSet traitSet, RelNode input, RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        return new Limit(cluster, traitSet, input, RelCollations.EMPTY, offset, fetch);
    }

    public Limit(RelInput input) {
        super(input);
        traitSet = traitSet.replace(DrdsConvention.INSTANCE);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Limit copy(RelTraitSet traitSet, RelNode newInput,
                      RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new Limit(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "Limit");
        assert fieldExps.size() == collation.getFieldCollations().size();
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double rowCount = mq.getRowCount(this);
        int chunkSize = PlannerContext.getPlannerContext(this).getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        final double size = Math.min(rowCount, chunkSize);
        final double memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType()) * size;
        final double cpu = rowCount + CPU_START_UP_COST;
        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }
}
