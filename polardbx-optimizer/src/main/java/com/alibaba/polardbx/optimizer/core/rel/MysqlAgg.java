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
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author dylan
 */
public class MysqlAgg extends Aggregate implements MysqlRel {

    public static Index OPTIMIZED_AWAY_INDEX = new Index(
        new IndexMeta("optimized_away_table", new ArrayList<>(), new ArrayList<>(),
            IndexType.NONE, Relationship.NONE, false, false, false,
            "optimized_away_index"), Arrays.asList(Index.PredicateType.EQUAL), 1, 1);

    public MysqlAgg(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
    }

    public MysqlAgg(RelInput input) {
        super(input);
    }

    public static MysqlAgg create(final RelNode input,
                                  ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                                  List<AggregateCall> aggCalls) {
        return create_(input, false, groupSet, groupSets, aggCalls);
    }

    private static MysqlAgg create_(final RelNode input,
                                    boolean indicator,
                                    ImmutableBitSet groupSet,
                                    List<ImmutableBitSet> groupSets,
                                    List<AggregateCall> aggCalls) {
        final RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new MysqlAgg(cluster, traitSet, input, indicator, groupSet,
            groupSets, aggCalls);
    }

    @Override
    public MysqlAgg copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
                         List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        MysqlAgg mysqlAgg = new MysqlAgg(getCluster(),
            traitSet,
            input,
            indicator,
            groupSet,
            groupSets,
            aggCalls);
        return mysqlAgg;
    }

    public MysqlAgg copy(RelNode input, ImmutableBitSet groupSet, List<AggregateCall> aggCalls) {
        return copy(traitSet, input, indicator, groupSet, null, aggCalls);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "MysqlAgg");

        List<String> groupList = new ArrayList<String>(groupSet.length());

        for (int groupIndex : groupSet.asList()) {
            RexExplainVisitor visitor = new RexExplainVisitor(this);
            groupList.add(visitor.getField(groupIndex).getKey());
        }
        pw.itemIf("group", StringUtils.join(groupList, ","), !groupList.isEmpty());
        int groupCount = groupSet.cardinality();
        for (int i = groupCount; i < getRowType().getFieldCount(); i++) {
            RexExplainVisitor visitor = new RexExplainVisitor(this);
            aggCalls.get(i - groupCount).accept(visitor);
            pw.item(rowType.getFieldList().get(i).getKey(), visitor.toSqlString());
        }
        Index index = canUseIndex(getCluster().getMetadataQuery());
        if (index != null) {
            pw.item("index", index.getIndexMeta().getPhysicalIndexName());
        }

        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this.input);
        if (Double.isInfinite(rowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        final double sortCpu;
        Index aggIndex = canUseIndex(mq);

        if (aggIndex == OPTIMIZED_AWAY_INDEX) {
            return planner.getCostFactory().makeTinyCost();
        }

        if (aggIndex != null) {
            MysqlTableScan mysqlTableScan = getAccessMysqlTableScan(this.getInput());
            Index tableScanIndex = mysqlTableScan.canUseIndex(mq);
            if (tableScanIndex == null) {
                if (aggIndex.getIndexMeta().isPrimaryKeyIndex()) {
                    // clustering index
                    sortCpu = 0;
                } else {
                    // non-clustering index
                    // choose min cost index of aggIndex or tableScanIndex
                    if (overJoin(this)) {
                        sortCpu = Math.min(
                            Util.nLogN(rowCount) * CostModelWeight.INSTANCE.getSortWeight() * groupSet.cardinality(),
                            mq.getCumulativeCost(mysqlTableScan).getIo() * CostModelWeight.INSTANCE.getIoWeight());
                    } else {
                        sortCpu = 0;
                    }
                }
            } else {
                // aggIndex and tableScanIndex must be the same index
                sortCpu = 0;
            }
        } else {
            sortCpu = Util.nLogN(rowCount) * CostModelWeight.INSTANCE.getSortWeight()
                * groupSet.cardinality();
        }
        final double sortAggWeight = CostModelWeight.INSTANCE.getSortAggWeight();
        final double useAggSize =
            aggCalls.stream().filter(x -> x.getAggregation().kind != SqlKind.__FIRST_VALUE
                && x.getAggregation().getKind() != SqlKind.FIRST_VALUE).count();
        // 1 for grouping
        final double aggCpu = rowCount * sortAggWeight * Math.max(useAggSize, 1);
        final double memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType()) * mq.getRowCount(this);

        return planner.getCostFactory().makeCost(rowCount, sortCpu + aggCpu, memory, 0, 0);
    }

    private boolean overJoin(RelNode rel) {
        if (rel instanceof Join) {
            return true;
        }
        for (RelNode input : rel.getInputs()) {
            if (overJoin(input)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Index canUseIndex(RelMetadataQuery mq) {
        MysqlTableScan mysqlTableScan = getAccessMysqlTableScan(this.getInput());
        if (mysqlTableScan != null) {
            Index accessIndex = MysqlRel.canUseIndex(this.getInput(), mq);

            Set<Integer> groupKeyColumnSet = new HashSet<>();
            List<Integer> groupByColumn = new ArrayList<>();
            for (Integer i : this.getGroupSet()) {
                RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(this.getInput(), i);
                if (relColumnOrigin == null) {
                    return null;
                }

                // FIXME: Is that condition strict enough ?
                if (relColumnOrigin.getOriginTable() != mysqlTableScan.getTable()) {
                    return null;
                }

                if (groupKeyColumnSet.add(relColumnOrigin.getOriginColumnOrdinal())) {
                    groupByColumn.add(relColumnOrigin.getOriginColumnOrdinal());
                }
            }
            Index groupByIndex;
            if (accessIndex == null) {
                // mysql tablescan without access index, we can use a index suitable to group by
                if (!groupByColumn.isEmpty()) {
                    groupByIndex = IndexUtil.selectIndexForGroupBy(CBOUtil.getTableMeta(mysqlTableScan.getTable()),
                        groupByColumn, IndexUtil.getCanUseIndexSet(mysqlTableScan));
                } else {
                    // select min(a), max(b) from t;
                    Set<Integer> minMaxAggCallKeyColumnSet = getMinMaxAggCallKeyColumnSet();
                    if (minMaxAggCallKeyColumnSet == null) {
                        return null;
                    }

                    // if have filter, but no index can use, impossible to optimize away
                    if (!mysqlTableScan.getFilters().isEmpty()) {
                        return null;
                    }

                    groupByIndex =
                        IndexUtil.selectIndexForMinMaxAggCall(CBOUtil.getTableMeta(mysqlTableScan.getTable()),
                            minMaxAggCallKeyColumnSet, IndexUtil.getCanUseIndexSet(mysqlTableScan));
                }
            } else {
                // mysql tablescan with access index, check whether group by can use
                if (!groupByColumn.isEmpty()) {
                    groupByIndex = IndexUtil.canIndexUsedByGroupBy(CBOUtil.getTableMeta(mysqlTableScan.getTable()),
                        accessIndex.getIndexMeta(), accessIndex.getPrefixTypeList(), groupByColumn);
                } else {
                    // select min(a), max(b) from t;
                    Set<Integer> minMaxAggCallKeyColumnSet = getMinMaxAggCallKeyColumnSet();
                    if (minMaxAggCallKeyColumnSet == null) {
                        return null;
                    }
                    groupByIndex =
                        IndexUtil.canIndexUsedByMinMaxAggCall(CBOUtil.getTableMeta(mysqlTableScan.getTable()),
                            accessIndex.getIndexMeta(), minMaxAggCallKeyColumnSet);
                }
            }

            return groupByIndex;
        } else {
            return null;
        }
    }

    private Set<Integer> getMinMaxAggCallKeyColumnSet() {
        if (getAggCallList().stream().allMatch(x -> x.getAggregation().getKind().belongsTo(SqlKind.MIN_MAX_AGG))) {
            if (overJoin(this)) {
                return null;
            }
            Set<Integer> aggCallKeyColumnSet = new HashSet<>();
            for (AggregateCall aggregateCall : getAggCallList()) {
                if (aggregateCall.getAggregation().getKind().belongsTo(SqlKind.MIN_MAX_AGG)) {
                    RelColumnOrigin columnOrigin = this.getCluster().getMetadataQuery().getColumnOrigin(this.getInput(),
                        aggregateCall.getArgList().get(0));
                    if (columnOrigin != null) {
                        aggCallKeyColumnSet.add(columnOrigin.getOriginColumnOrdinal());
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            }
            return aggCallKeyColumnSet;
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
        } else {
            return null;
        }
    }
}

