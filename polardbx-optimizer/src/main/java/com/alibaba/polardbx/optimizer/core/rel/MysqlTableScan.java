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

import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.TUPLE_HEADER_SIZE;

/**
 * @author dylan
 */
public class MysqlTableScan extends TableScan implements MysqlRel {

    private ImmutableList<RexNode> filters;

    private Join join = null;

    private List<Index> accessIndexList;

    private RelNode nodeForMetaQuery;

    MysqlTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                   RelOptTable table, ImmutableList<RexNode> filters, SqlNodeList hints, SqlNode indexNode,
                   RexNode flashback, SqlNode partitions) {
        super(cluster, traitSet, table, hints, indexNode, flashback, partitions);
        this.filters = Preconditions.checkNotNull(filters);
    }

    public static MysqlTableScan create(RelOptCluster cluster, RelOptTable relOptTable, List<RexNode> filters,
                                        SqlNodeList hints, SqlNode indexNode, RexNode flashback, SqlNode partitions) {
        return new MysqlTableScan(cluster, cluster.traitSetOf(Convention.NONE), relOptTable,
            ImmutableList.copyOf(filters), hints, indexNode, flashback, partitions);
    }

    @Override
    public RelDataType deriveRowType() {
        return table.getRowType();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("filters", filters, !filters.isEmpty());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (join != null) {
            Index index = IndexUtil.selectJoinIndex(join, false);
            if (index != null) {
                this.accessIndexList = new ArrayList<>();
                this.accessIndexList.add(index);
                double selectivity = index.getTotalSelectivity();
                double rows = Math.max(table.getRowCount() * selectivity, 1);
                double cpu = Math.max(table.getRowCount() * selectivity, 1);
                double memory = 0;
                double size = (TUPLE_HEADER_SIZE + TableScanIOEstimator.estimateRowSize(table.getRowType()))
                    * table.getRowCount() * selectivity;
                double io = Math.ceil(size / CostModelWeight.RAND_IO_PAGE_SIZE);
                RelOptCost indexCost = planner.getCostFactory().makeCost(rows, cpu, memory, io, 0);
                return indexCost;
            }
        }

        return getNonJoinCost(planner, mq, true);
    }

    public RelOptCost getNonJoinCost(RelOptPlanner planner, RelMetadataQuery mq, boolean updateAccessIndexList) {
        double rowCount = mq.getRowCount(this);
        TableScanIOEstimator tableScanIOEstimator = new TableScanIOEstimator(this, mq);
        double io;
        if (filters.isEmpty()) {
            io = tableScanIOEstimator.getMaxIO();
        } else {
            io = tableScanIOEstimator.evaluate(filters.get(0));
        }
        if (updateAccessIndexList) {
            this.accessIndexList = tableScanIOEstimator.getAccessIndexList();
        }
        return planner.getCostFactory().makeCost(rowCount, rowCount, 0, io, 0);
    }

    public void setFilters(ImmutableList<RexNode> filters) {
        this.filters = filters;
    }

    public ImmutableList<RexNode> getFilters() {
        return filters;
    }

    public RelNode getNodeForMetaQuery() {
        if (nodeForMetaQuery == null) {
            synchronized (this) {
                if (filters.isEmpty()) {
                    nodeForMetaQuery =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, partitions);
                } else {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, partitions);
                    LogicalFilter logicalFilter = LogicalFilter.create(logicalTableScan, filters.get(0));
                    nodeForMetaQuery = logicalFilter;
                }
            }
        }
        return nodeForMetaQuery;
    }

    public void setJoin(Join join) {
        this.join = join;
    }

    public Set<Integer> getColumnCouldUseIndexForJoin(boolean onlyConsiderEqualPredicate) {
        if (filters.isEmpty()) {
            return new HashSet<>();
        }

        Set<Integer> result = new HashSet<>();
        List<RexNode> conjunctions = RelOptUtil.conjunctions(filters.get(0));
        for (RexNode pred : conjunctions) {
            // only consider CNF
            if (pred.isA(SqlKind.EQUALS) && pred instanceof RexCall) {
                RexCall filterCall = (RexCall) pred;
                RexNode leftRexNode = filterCall.getOperands().get(0);
                RexNode rightRexNode = filterCall.getOperands().get(1);
                if (leftRexNode instanceof RexInputRef) {
                    int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                    result.add(leftIndex);
                }
                if (rightRexNode instanceof RexInputRef) {
                    int rightIndex = ((RexInputRef) rightRexNode).getIndex();
                    result.add(rightIndex);
                }
            } else if (!onlyConsiderEqualPredicate && pred.isA(SqlKind.IN) && pred instanceof RexCall) {
                RexCall in = (RexCall) pred;
                if (in.getOperands().size() == 2) {
                    RexNode leftRexNode = in.getOperands().get(0);
                    RexNode rightRexNode = in.getOperands().get(1);
                    /** In Predicate contains one column */
                    if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
                        .isA(SqlKind.ROW)) {
                        result.add(((RexInputRef) leftRexNode).getIndex());
                    }
                }
            }
        }
        return result;
    }

    public List<Index> getAccessIndexList(RelMetadataQuery mq) {
        if (accessIndexList == null) {
            // compute cost to get accessIndexList
            mq.getCumulativeCost(this);
        }
        return accessIndexList;
    }

    @Override
    public Index canUseIndex(RelMetadataQuery mq) {
        Index accessIndex = null;
        List<Index> accessIndexList = getAccessIndexList(mq);
        if (accessIndexList != null && accessIndexList.size() == 1) {
            accessIndex = accessIndexList.get(0);
        }
        return accessIndex;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MysqlTableScan");
        pw.item("name", getTable().getQualifiedName());
        if (!filters.isEmpty()) {
            RexExplainVisitor visitor = new RexExplainVisitor(getNodeForMetaQuery());
            filters.get(0).accept(visitor);
            pw.item("filter", visitor.toSqlString());
        }
        RelMetadataQuery mq = this.getCluster().getMetadataQuery();
        List<Index> accessIndexList = getAccessIndexList(mq);

        boolean hasJoinIndex = false;

        if (join != null) {
            Index index = IndexUtil.selectJoinIndex(join, false);
            if (index != null) {
                pw.item("joinIndex", index.getIndexMeta().getPhysicalIndexName());
                hasJoinIndex = true;
            }
        }

        if (!hasJoinIndex && accessIndexList != null && !accessIndexList.isEmpty()) {
            Set<String> indexNameSet =
                accessIndexList.stream().map(x -> x.getIndexMeta().getPhysicalIndexName()).collect(
                    Collectors.toSet());
            StringBuilder stringBuilder = new StringBuilder();
            boolean first = true;
            for (String indexName : indexNameSet) {
                if (!first) {
                    stringBuilder.append(",");
                }
                first = false;
                stringBuilder.append(indexName);
            }
            pw.item("index", stringBuilder.toString());
        }

        return pw;
    }
}
