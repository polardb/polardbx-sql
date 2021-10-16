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
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.CPU_START_UP_COST;

/**
 * In-memory Sort Operator
 *
 */
public class MemSort extends Sort {

    private MemSort(RelOptCluster cluster, RelTraitSet traitSet,
                    RelNode input, RelCollation collation) {
        super(cluster, traitSet, input, collation, null, null);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
    }

    public static MemSort create(RelTraitSet traitSet, RelNode input, RelCollation collation) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        return new MemSort(cluster, traitSet.replace(collation), input, collation);
    }

    public MemSort(RelInput input) {
        super(input);
        traitSet = traitSet.replace(DrdsConvention.INSTANCE);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public MemSort copy(RelTraitSet traitSet, RelNode newInput,
                        RelCollation newCollation, RexNode fetch, RexNode offset) {
        assert fetch == null && offset == null : "fetch and offset must be null";
        return new MemSort(getCluster(), traitSet, newInput, newCollation);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MemSort");
        assert fieldExps.size() == collation.getFieldCollations().size();
        if (pw.nest()) {
            pw.item("collation", collation);
        } else {
            List<String> sortList = new ArrayList<String>(fieldExps.size());
            for (int i = 0; i < fieldExps.size(); i++) {
                StringBuilder sb = new StringBuilder();
                RexExplainVisitor visitor = new RexExplainVisitor(this);
                fieldExps.get(i).accept(visitor);
                sb.append(visitor.toSqlString()).append(" ").append(
                    collation.getFieldCollations().get(i).getDirection().shortString);
                sortList.add(sb.toString());
            }

            String sortString = StringUtils.join(sortList, ",");
            pw.itemIf("sort", sortString, !StringUtils.isEmpty(sortString));
        }
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double rowCount = mq.getRowCount(this.input);
        final double cpu = CPU_START_UP_COST +
            Util.nLogN(rowCount) * CostModelWeight.INSTANCE.getSortWeight() * collation.getFieldCollations().size();
        final double memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType()) * mq.getRowCount(this);
        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    @Override
    public boolean isEnforcer() {
        return true;
    }
}
