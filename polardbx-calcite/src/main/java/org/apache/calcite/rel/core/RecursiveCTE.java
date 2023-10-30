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
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * left rel is anchor part, right rel is body part
 *
 * @author fangwu
 */
public class RecursiveCTE extends BiRel {
    private String cteName;

    // special limit grammar for recursive cte
    private RexNode offset;
    private RexNode fetch;

    public RecursiveCTE(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode left, RelNode right, String cteName, RexNode offset, RexNode fetch) {
        // left -> anchor part; right -> recursive part
        super(cluster, traitSet, left, right);
        this.cteName = cteName;
        this.offset = offset;
        this.fetch = fetch;
    }

    /**
     * recursive rowtype must be same with anchor part
     */
    protected RelDataType deriveRowType() {
        return left.getRowType();
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "RecursiveCTE");
        pw.item("cte_name", cteName);
        pw.item("offset", offset);
        pw.item("fetch", fetch);
        return pw;
    }

    @Override
    public void explainForDisplay(RelWriter pw) {
        explainTermsForDisplay(pw).done(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (getInputs().equals(inputs)
            && traitSet == getTraitSet()) {
            return this;
        }
        return new RecursiveCTE(getCluster(), traitSet, inputs.get(0), inputs.get(1), cteName, offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        RelOptCost relOptCost = left.computeSelfCost(planner, mq);
        return planner.getCostFactory()
            .makeCost(relOptCost.getRows() * 1000, relOptCost.getCpu() * 1000, relOptCost.getMemory(),
                relOptCost.getIo(),
                relOptCost.getNet());
    }

    public String getCteName() {
        return cteName;
    }

    public RexNode getOffset() {
        return offset;
    }

    public RexNode getFetch() {
        return fetch;
    }
}
