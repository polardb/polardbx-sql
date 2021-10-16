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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public final class Gather extends Exchange {

    /**
     * Creates a Gather.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public Gather(RelOptCluster cluster,
                  RelTraitSet traitSet,
                  RelNode input) {
        super(cluster, traitSet.replace(DrdsConvention.INSTANCE).replace(RelDistributions.SINGLETON), input,
            RelDistributions.SINGLETON);
    }

    public Gather(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet().replace(DrdsConvention.INSTANCE).replace(RelDistributions.SINGLETON),
            relInput.getInput(), RelDistributions.SINGLETON);
    }

    /**
     * Creates a Gather.
     */
    public static Gather create(RelNode input) {
        final RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(DrdsConvention.INSTANCE).replace(RelDistributions.SINGLETON);
        return new Gather(cluster, traitSet, input);
    }

    //~ Methods ----------------------------------------------------------------

    public void setJoin(Join join) {
        for (RelNode logicalView : this.getInputs()) {
            if (logicalView instanceof HepRelVertex) {
                logicalView = ((HepRelVertex) logicalView).getCurrentRel();
            }
            if (logicalView instanceof RelSubset) {
                logicalView = ((RelSubset) logicalView).getOriginal();
            }
            if (logicalView instanceof LogicalView) {
                ((LogicalView) logicalView).setJoin(join);
            } else {
                GeneralUtil
                    .nestedException("Unexpected RelNode type :" + logicalView.getClass() + ", expect LogicalView");
            }
        }
    }

    public Gather copy(RelTraitSet traitSet, RelNode input) {
        return new Gather(getCluster(), traitSet, input);
    }

    @Override
    public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
        assert newDistribution == RelDistributions.SINGLETON;
        return copy(traitSet, newInput);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "Gather");
        pw.item("concurrent", true);
        pw.input("input", input);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // no cost for Gather
        RelOptCost cost = planner.getCostFactory().makeTinyCost();
        return cost;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput());
    }

    @Deprecated
    public static double estimateRowCount(RelNode rel) {
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        return mq.getRowCount(rel.getInput(0));
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }
}
