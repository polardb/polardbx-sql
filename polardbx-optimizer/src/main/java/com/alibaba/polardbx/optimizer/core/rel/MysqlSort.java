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

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author dylan
 */
public class MysqlSort extends Sort implements MysqlRel {

    protected MysqlSort(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert traitSet.containsIfApplicable(Convention.NONE);
    }

    public MysqlSort(RelInput input) {
        super(input);
    }

    public static MysqlSort create(RelNode input, RelCollation collation,
                                   RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traitSet =
            input.getTraitSet().replace(Convention.NONE).replace(collation).replace(RelDistributions.SINGLETON);
        return new MysqlSort(cluster, traitSet, input, collation, offset, fetch);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public MysqlSort copy(RelTraitSet traitSet, RelNode newInput,
                          RelCollation newCollation, RexNode offset, RexNode fetch) {
        MysqlSort mysqlSort = new MysqlSort(getCluster(), traitSet, newInput, newCollation,
            offset, fetch);
        return mysqlSort;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MysqlSort");
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
        Index index = MysqlSort.canUseIndex(this, getCluster().getMetadataQuery());
        if (index != null) {
            pw.item("index", index.getIndexMeta().getPhysicalIndexName());
        }
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double rowCount = mq.getRowCount(this.input);
        final double cpu;
        final double memory;

        if (MysqlSort.canUseIndex(this, mq) != null) {
            cpu = rowCount;
            memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType());
        } else {
            cpu = Util.nLogN(rowCount) * CostModelWeight.INSTANCE.getSortWeight()
                * collation.getFieldCollations().size();
            memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType()) * rowCount;
        }
        return planner.getCostFactory().makeCost(rowCount, cpu, memory, 0, 0);
    }

    @Override
    public Index canUseIndex(RelMetadataQuery mq) {
        return canUseIndex(this, mq);
    }

    private static Index canUseIndex(MysqlSort mysqlSort, RelMetadataQuery mq) {
        MysqlTableScan mysqlTableScan = getAccessMysqlTableScan(mysqlSort.getInput());
        Index index = MysqlRel.canUseIndex(mysqlSort.getInput(), mq);
        return canUseIndex(mysqlSort, mysqlTableScan, index, mq);
    }

    public static Index canUseIndex(Sort mysqlSort, MysqlTableScan mysqlTableScan, Index accessIndex,
                                    RelMetadataQuery mq) {
        if (mysqlTableScan != null) {

            Set<Integer> orderKeyColumnSet = new HashSet<>();
            List<Integer> orderByColumn = new ArrayList<>();
            int ascCount = 0;
            int descCount = 0;
            for (RelFieldCollation relFieldCollation : mysqlSort.getCollation().getFieldCollations()) {
                RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(mysqlSort.getInput(),
                    relFieldCollation.getFieldIndex());
                if (relColumnOrigin == null) {
                    return null;
                }

                // FIXME: Is that condition strict enough ?
                if (relColumnOrigin.getOriginTable() != mysqlTableScan.getTable()) {
                    return null;
                }

                if (orderKeyColumnSet.add(relColumnOrigin.getOriginColumnOrdinal())) {
                    orderByColumn.add(relColumnOrigin.getOriginColumnOrdinal());
                    if (relFieldCollation.direction.isDescending()) {
                        descCount++;
                    } else {
                        ascCount++;
                    }
                }
            }

            if (ascCount != 0 && descCount != 0) {
                // not the same direction;
                return null;
            }

            Index orderByIndex;
            if (accessIndex == null) {
                // mysql tablescan without access index, we can use a index suitable to order by
                orderByIndex = IndexUtil.selectIndexForOrderBy(CBOUtil.getTableMeta(mysqlTableScan.getTable()),
                    orderByColumn, IndexUtil.getCanUseIndexSet(mysqlTableScan));
            } else {
                // mysql tablescan with access index, check whether order by can use
                orderByIndex =
                    IndexUtil.canIndexUsedByOrderBy(CBOUtil.getTableMeta(mysqlTableScan.getTable()),
                        accessIndex.getIndexMeta(), accessIndex.getPrefixTypeList(), orderByColumn);
            }

            return orderByIndex;
        } else {
            return null;
        }
    }

    private static MysqlTableScan getAccessMysqlTableScan(RelNode input) {
        // for now only support single table
        if (input instanceof MysqlTableScan) {
            return (MysqlTableScan) input;
        } else if (input instanceof LogicalProject) {
            return getAccessMysqlTableScan(((LogicalProject) input).getInput());
        } else if (input instanceof LogicalFilter) {
            return getAccessMysqlTableScan(((LogicalFilter) input).getInput());
        } else {
            return null;
        }
    }
}
