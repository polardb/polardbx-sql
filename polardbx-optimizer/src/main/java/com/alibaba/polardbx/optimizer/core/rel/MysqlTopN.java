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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class MysqlTopN extends Sort implements MysqlRel {

    protected MysqlTopN(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert traitSet.containsIfApplicable(Convention.NONE);
    }

    public MysqlTopN(RelInput input) {
        super(input);
    }

    public static MysqlTopN create(RelNode input, RelCollation collation,
                                   RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traitSet =
            input.getTraitSet().replace(Convention.NONE).replace(collation).replace(RelDistributions.SINGLETON);
        return new MysqlTopN(cluster, traitSet, input, collation, offset, fetch);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public MysqlTopN copy(RelTraitSet traitSet, RelNode newInput,
                          RelCollation newCollation, RexNode offset, RexNode fetch) {
        MysqlTopN mysqlTopN = new MysqlTopN(getCluster(), traitSet, newInput, newCollation,
            offset, fetch);
        return mysqlTopN;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MysqlTopN");
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
        Index index = canUseIndex(this, getCluster().getMetadataQuery());
        if (index != null) {
            pw.item("index", index.getIndexMeta().getPhysicalIndexName());
        }
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double inputRowCount = mq.getRowCount(this.input);
        final double outputRowCount = mq.getRowCount(this);
        final double cpu;
        final double memory;

        if (canUseIndex(this, mq) != null) {
            cpu = mq.getRowCount(this);
            memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType());
        } else {
            cpu = Util.nLogk(inputRowCount, outputRowCount) * CostModelWeight.INSTANCE.getSortWeight()
                * collation.getFieldCollations().size();
            memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType()) * mq.getRowCount(this);
        }
        return planner.getCostFactory().makeCost(inputRowCount, cpu, memory, 0, 0);
    }

    @Override
    public Index canUseIndex(RelMetadataQuery mq) {
        return canUseIndex(this, mq);
    }

    private static Index canUseIndex(MysqlTopN mysqlTopN, RelMetadataQuery mq) {
        MysqlTableScan mysqlTableScan = getAccessMysqlTableScan(mysqlTopN.getInput());
        Index index = MysqlRel.canUseIndex(mysqlTopN.getInput(), mq);
        return MysqlSort.canUseIndex(mysqlTopN, mysqlTableScan, index, mq);
    }

    private static MysqlTableScan getAccessMysqlTableScan(RelNode input) {
        // for now only support single table
        if (input instanceof MysqlTableScan) {
            return (MysqlTableScan) input;
        } else if (input instanceof LogicalProject) {
            return getAccessMysqlTableScan(((LogicalProject) input).getInput());
        } else if (input instanceof LogicalFilter) {
            return getAccessMysqlTableScan(((LogicalFilter) input).getInput());
        } else if (input instanceof MysqlIndexNLJoin
            || input instanceof MysqlNLJoin
            || input instanceof MysqlHashJoin) {
            if (((Join) input).getJoinType() == JoinRelType.RIGHT) {
                return getAccessMysqlTableScan(((Join) input).getRight());
            } else {
                return getAccessMysqlTableScan(((Join) input).getLeft());
            }
        } else if (input instanceof MysqlSemiIndexNLJoin
            || input instanceof MysqlSemiNLJoin
            || input instanceof MysqlSemiHashJoin) {
            return getAccessMysqlTableScan(((Join) input).getLeft());
        } else if (input instanceof MysqlAgg) {
            RelMetadataQuery mq = input.getCluster().getMetadataQuery();
            // only consider MysqlAgg can use Index
            if (((MysqlAgg) input).canUseIndex(mq) != null) {
                return getAccessMysqlTableScan(((MysqlAgg) input).getInput());
            } else {
                return null;
            }
        } else {
            return null;
        }
    }
}
