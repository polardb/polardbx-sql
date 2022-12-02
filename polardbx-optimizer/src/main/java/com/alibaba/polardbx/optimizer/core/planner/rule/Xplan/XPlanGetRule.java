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

package com.alibaba.polardbx.optimizer.core.planner.rule.Xplan;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanEqualTuple;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class XPlanGetRule extends RelOptRule {

    public XPlanGetRule(RelOptRuleOperand operand, String description) {
        super(operand, "XPlan_rule:" + description);
    }

    public static final XPlanGetRule INSTANCE = new XPlanGetRule(
        operand(LogicalFilter.class, operand(XPlanTableScan.class, none())), "filter_tableScan_to_get");

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = (LogicalFilter) call.rels[0];
        XPlanTableScan tableScan = (XPlanTableScan) call.rels[1];

        if (!tableScan.getGetExprs().isEmpty() || tableScan.getGetIndex() != null) {
            return; // Already pushed.
        }

        final List<RexNode> restConditions = new ArrayList<>();
        final List<XPlanEqualTuple> andConditions = getAndLookupCondition(filter, restConditions);
        if (andConditions.isEmpty()) {
            return; // No conditions to generate get.
        }

        final TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        final List<Pair<String, List<Pair<Integer, Long>>>> lookupIndexes = findGetIndex(tableMeta, andConditions);
        if (lookupIndexes.isEmpty()) {
            return; // No indexed to lookup.
        }

        final Integer useIndex = Optional.ofNullable(tableScan.getIndexNode())
            // FORCE INDEX
            .filter(indexNode -> indexNode instanceof SqlNodeList && ((SqlNodeList) indexNode).size() > 0)
            // If more than one index specified, choose first one only
            .map(indexNode -> (SqlIndexHint) ((SqlNodeList) indexNode).get(0))
            // only support force index
            .filter(SqlIndexHint::forceIndex)
            // Dealing with force index(`xxx`), `xxx` will decoded as string.
            .map(indexNode -> GlobalIndexMeta.getIndexName(RelUtils.lastStringValue(indexNode.getIndexList().get(0))))
            .flatMap(indexName -> {
                // Find index from hint if can use.
                final List<String> indexNameList = Pair.left(lookupIndexes);
                for (int i = 0; i < indexNameList.size(); ++i) {
                    if (indexNameList.get(i).equalsIgnoreCase(indexName)) {
                        return Optional.of(i);
                    }
                }
                return Optional.empty();
            }).orElse(null);

        final String useIndexName;
        final List<Pair<Integer, Long>> useConditions;
        if (useIndex != null) {
            useIndexName = lookupIndexes.get(useIndex).getKey();
            useConditions = lookupIndexes.get(useIndex).getValue();
        } else {
            // Select minimum row count.
            double minRowCount = Double.MAX_VALUE;
            int selected = -1;

            final RelMetadataQuery mq = tableScan.getCluster().getMetadataQuery();
            for (int idx = 0; idx < lookupIndexes.size(); ++idx) {
                RexNode cond = null;
                for (Pair<Integer, Long> conditonIdx : lookupIndexes.get(idx).getValue()) {
                    final RexNode rex = tableScan.getCluster().getRexBuilder()
                        .makeCall(andConditions.get(conditonIdx.left).getOperator(),
                            andConditions.get(conditonIdx.left).getKey(),
                            andConditions.get(conditonIdx.left).getValue());
                    if (null == cond) {
                        cond = rex;
                    } else {
                        cond = tableScan.getCluster().getRexBuilder()
                            .makeCall(SqlStdOperatorTable.AND, cond, rex);
                    }
                }
                if (null == cond) {
                    cond = tableScan.getCluster().getRexBuilder().makeLiteral(true);
                }
                final double rowCount = RelMdUtil.estimateFilteredRows(tableScan, cond, mq);
                if (rowCount < minRowCount) {
                    minRowCount = rowCount;
                    selected = idx;
                }
            }

            useIndexName = lookupIndexes.get(selected).getKey();
            useConditions = lookupIndexes.get(selected).getValue();
        }

        final List<XPlanEqualTuple> getExpr = useConditions.stream()
            .map(cond -> andConditions.get(cond.left))
            .collect(Collectors.toList());
        tableScan.getGetExprs().add(getExpr);
        tableScan.setGetIndex(useIndexName);
        tableScan.resetNodeForMetaQuery();

        final boolean needExtraFilter = useConditions.stream()
            .anyMatch(cond -> cond.right != 0); // Any sub part in equal condition.
        if (needExtraFilter || DynamicConfig.getInstance().getXprotoAlwaysKeepFilterOnXplanGet()) {
            return; // Just keep the filter and keep the original rel node tree.
        }

        if (useConditions.size() == andConditions.size() && restConditions.isEmpty()) {
            // Full push down.
            call.transformTo(tableScan);
        } else {
            // Part push down.

            // Generate rest filter.
            RexNode restFilter = null;
            for (RexNode rex : restConditions) {
                if (null == restFilter) {
                    restFilter = rex;
                } else {
                    restFilter = tableScan.getCluster().getRexBuilder()
                        .makeCall(SqlStdOperatorTable.AND, restFilter, rex);
                }
            }
            final Set<Integer> useConditionSet =
                useConditions.stream().map(cond -> cond.left).collect(Collectors.toSet());
            for (int idx = 0; idx < andConditions.size(); ++idx) {
                if (useConditionSet.contains(idx)) {
                    continue;
                }
                final XPlanEqualTuple tuple = andConditions.get(idx);
                final RexNode rex = tableScan.getCluster().getRexBuilder()
                    .makeCall(tuple.getOperator(), tuple.getKey(), tuple.getValue());
                if (null == restFilter) {
                    restFilter = rex;
                } else {
                    restFilter = tableScan.getCluster().getRexBuilder()
                        .makeCall(SqlStdOperatorTable.AND, restFilter, rex);
                }
            }
            if (null == restFilter) {
                throw GeneralUtil.nestedException("XPlan part filter push down fatal.");
            }

            final LogicalFilter newFilter = LogicalFilter.create(tableScan, restFilter);
            call.transformTo(newFilter);
        }
    }

    private static List<Pair<String, List<Pair<Integer, Long>>>> findGetIndex(TableMeta tableMeta,
                                                                              List<XPlanEqualTuple> conditions) {
        final Map<String, Integer> keyMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int idx = 0; idx < conditions.size(); ++idx) {
            keyMap.put(tableMeta.getAllColumns().get(conditions.get(idx).getKey().getIndex()).getName(), idx);
        }
        final List<Pair<String, List<Pair<Integer, Long>>>> result = new ArrayList<>();

        // Note: Indexes include PRIMARY.
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            if (indexMeta.getPhysicalIndexName().equalsIgnoreCase("PRIMARY") && !tableMeta.isHasPrimaryKey()) {
                continue;
            }
            final List<Pair<Integer, Long>> conditionIndexes = new ArrayList<>();
            for (IndexColumnMeta columnMeta : indexMeta.getKeyColumnsExt()) {
                final Integer idx = keyMap.get(columnMeta.getColumnMeta().getName());
                if (null == idx) {
                    break;
                } else {
                    conditionIndexes.add(Pair.of(idx, columnMeta.getSubPart())); // Record the sub part length.
                }
            }
            if (conditionIndexes.size() > 0) {
                result.add(Pair.of(indexMeta.getPhysicalIndexName(), conditionIndexes));
            }
        }
        return result;
    }

    private static List<XPlanEqualTuple> getAndLookupCondition(LogicalFilter filter, List<RexNode> restCondition) {
        final Queue<RexCall> scanQueue = new LinkedList<>();
        final RexNode cnf = RexUtil.toSimpleCnf(filter.getCluster().getRexBuilder(), filter.getCondition());
        if (!(cnf instanceof RexCall)) {
            restCondition.add(cnf);
            return Collections.emptyList();
        }

        scanQueue.offer((RexCall) cnf);

        final List<XPlanEqualTuple> result = new ArrayList<>();

        // BFS.
        while (!scanQueue.isEmpty()) {
            final RexCall call = scanQueue.poll();
            switch (call.getOperator().getKind()) {
            case AND:
                // Push children.
                for (RexNode node : call.getOperands()) {
                    if (node instanceof RexCall) {
                        scanQueue.offer((RexCall) node);
                    } else {
                        restCondition.add(node);
                    }
                }
                break;

            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
                // Record equal condition.
                if (call.getOperands().size() != 2 || !(call.getOperands().get(0) instanceof RexInputRef)) {
                    restCondition.add(call);
                    continue; // Not supported.
                }
                final RexNode value = call.getOperands().get(1);
                if (value.getKind() != SqlKind.DYNAMIC_PARAM && value.getKind() != SqlKind.LITERAL) {
                    restCondition.add(call);
                    continue; // Can not cast to scalar.
                }
                // Dealing key.
                result.add(new XPlanEqualTuple(call.getOperator(), (RexInputRef) call.getOperands().get(0), value));
                break;

            default:
                restCondition.add(call); // Not supported.
            }
        }
        return result;
    }
}
