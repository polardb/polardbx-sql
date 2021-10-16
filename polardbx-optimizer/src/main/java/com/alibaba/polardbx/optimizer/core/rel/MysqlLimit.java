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

import com.alibaba.polardbx.optimizer.index.Index;
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
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class MysqlLimit extends Sort implements MysqlRel {

    protected MysqlLimit(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert traitSet.containsIfApplicable(Convention.NONE);
    }

    public MysqlLimit(RelInput input) {
        super(input);
    }

    public static MysqlLimit create(RelNode input, RelCollation collation,
                                    RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traitSet =
            input.getTraitSet().replace(Convention.NONE).replace(collation).replace(RelDistributions.SINGLETON);
        return new MysqlLimit(cluster, traitSet, input, collation, offset, fetch);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public MysqlLimit copy(RelTraitSet traitSet, RelNode newInput,
                           RelCollation newCollation, RexNode offset, RexNode fetch) {
        MysqlLimit mysqlLimit = new MysqlLimit(getCluster(), traitSet, newInput, newCollation,
            offset, fetch);
        return mysqlLimit;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MysqlLimit");
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
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // limit
        final double rowCount = mq.getRowCount(this);
        return planner.getCostFactory().makeCost(rowCount, rowCount, 0, 0, 0);
    }

    public boolean findMysqlLimitBlockingPoint() {
        return findBlockingPoint(this.input);
    }

    private boolean findBlockingPoint(RelNode node) {
        // TODO: more precise blocking point
        if (node instanceof MysqlAgg) {
            return true;
        } else if (node instanceof MysqlSort) {
            return true;
        } else if (node instanceof MysqlTopN) {
            return true;
        } else if (node instanceof Join) {
            return findBlockingPoint(((Join) node).getOuter());
        } else if (node instanceof SingleRel) {
            return findBlockingPoint(((SingleRel) node).getInput());
        } else {
            return false;
        }
    }

    @Override
    public Index canUseIndex(RelMetadataQuery mq) {
        return MysqlRel.canUseIndex(getInput(), mq);
    }
}

