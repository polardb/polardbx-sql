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

package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * @author fangwu
 */
public class RecursiveCTEAnchor extends AbstractRelNode {
    private final String cteName;

    public RecursiveCTEAnchor(RelOptCluster cluster, RelTraitSet traitSet,
                              String cteName, RelDataType rowtype) {
        super(cluster, traitSet);
        this.cteName = cteName;
        this.rowType = rowtype;
    }

    public String getCteName() {
        return cteName;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (getInputs().equals(inputs)
            && traitSet == getTraitSet()) {
            return this;
        }
        return new RecursiveCTEAnchor(getCluster(), traitSet, cteName, rowType);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "RecursiveCTEAnchor");
        pw.item("cte_target", cteName);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return planner.getCostFactory().makeCost(1.0, 1.0, 0.0, 0.0, 0.0);
    }
}
