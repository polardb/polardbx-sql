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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.externalize.RelDrdsWriter;

import java.util.List;

/**
 * @author lingce.ldm 2017-12-26 19:38
 */
public final class AffectedRowsSum extends Union {
    private boolean isConcurrent = false;

    /**
     * Creates a AffectedRowsSum.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public AffectedRowsSum(RelOptCluster cluster,
                           RelTraitSet traitSet,
                           List<RelNode> inputs,
                           boolean isConcurrent) {
        super(cluster, traitSet, inputs, true);
        this.isConcurrent = isConcurrent;
    }

    /**
     * Creates a AffectedRowsSum.
     */
    public static AffectedRowsSum create(List<RelNode> inputs, boolean isConcurrent) {
        final RelOptCluster cluster = inputs.get(0).getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new AffectedRowsSum(cluster, traitSet, inputs, isConcurrent);
    }

    /**
     * Creates a AffectedRowsSum.
     */
    public static AffectedRowsSum create(RelNode input, boolean isConcurrent) {
        return create(ImmutableList.of(input), isConcurrent);
    }

    //~ Methods ----------------------------------------------------------------

    public AffectedRowsSum copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AffectedRowsSum(getCluster(), traitSet, inputs, isConcurrent);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "AffectedRowsSum");
        pw.item("Concurrent:", isConcurrent);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw;
    }

    public boolean isConcurrent() {
        return isConcurrent;
    }
}
