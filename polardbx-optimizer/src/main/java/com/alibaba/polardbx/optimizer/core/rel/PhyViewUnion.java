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

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

/**
 * @author lingce.ldm 2017-12-26 19:38
 */
public final class PhyViewUnion extends Union {

    /**
     * Creates a PhyViewUnion.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public PhyViewUnion(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        List<RelNode> inputs) {
        super(cluster, traitSet, inputs, true);
    }

    /**
     * Creates a ViewUnion by parsing serialized output.
     */
    public PhyViewUnion(RelInput input) {
        super(input);
    }

    /**
     * Creates a LogicalUnion.
     */
    public static PhyViewUnion create(List<RelNode> inputs) {
        final RelOptCluster cluster = inputs.get(0).getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(DrdsConvention.INSTANCE);
        return new PhyViewUnion(cluster, traitSet, inputs);
    }

    //~ Methods ----------------------------------------------------------------

    public PhyViewUnion copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE);
        return new PhyViewUnion(getCluster(), traitSet, inputs);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "PhyViewUnion");
        pw.item("concurrent", true);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // no cost for viewUnion
        return planner.getCostFactory().makeTinyCost();
    }

}
