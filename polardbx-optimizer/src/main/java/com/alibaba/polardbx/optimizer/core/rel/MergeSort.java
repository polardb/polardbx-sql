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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * MergeSort Operator, copy from LogicalSort
 *
 * @author lingce.ldm 2017-08-03 15:17
 */
public final class MergeSort extends Sort {

    public MergeSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation,
                     RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
    }

    /**
     * Creates a MergeSort.
     *
     * @param input Input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning
     * first row
     * @param fetch Expression for number of rows to fetch
     */
    public static MergeSort create(RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traitSet =
            input.getTraitSet().replace(Convention.NONE).replace(collation).replace(RelDistributions.SINGLETON);
        return new MergeSort(cluster, traitSet, input, collation, offset, fetch);
    }

    public static MergeSort create(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset,
                                   RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        return new MergeSort(cluster, traitSet, input, collation, offset, fetch);
    }

    public MergeSort(RelInput input) {
        super(input);
    }

    // ~ Methods
    // ----------------------------------------------------------------

    @Override
    public MergeSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset,
                          RexNode fetch) {
        return new MergeSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MergeSort");
        assert fieldExps.size() == collation.getFieldCollations().size();
        if (pw.nest()) {
            pw.item("collation", collation);
        } else {
            List<String> sortString = new ArrayList<String>(fieldExps.size());
            for (int i = 0; i < fieldExps.size(); i++) {
                StringBuilder sb = new StringBuilder();
                RexExplainVisitor visitor = new RexExplainVisitor(this);
                fieldExps.get(i).accept(visitor);
                sb.append(visitor.toSqlString())
                    .append(" ")
                    .append(collation.getFieldCollations().get(i).getDirection().shortString);
                sortString.add(sb.toString());
            }
            pw.item("sort", StringUtils.join(sortString, ","));
        }
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // no cost for merge sort
        return planner.getCostFactory().makeTinyCost();
    }
}
