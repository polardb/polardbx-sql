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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.EnumSqlType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.TUPLE_HEADER_SIZE;
import static com.alibaba.polardbx.optimizer.selectivity.TableScanSelectivityEstimator.LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT;
import static com.alibaba.polardbx.optimizer.selectivity.TableScanSelectivityEstimator.LACK_OF_STATISTICS_INDEX_RANGE_ROW_COUNT;

/**
 * @author dylan
 */
public class TableScanIOEstimator extends AbstractIOEstimator {
    private final TableScan tableScan;
    private final Double tableRowCount;
    private final TableMeta tableMeta;

    private final long rowSize;
    private List<Index> accessIndexList;
    private Set<String> canUseIndexSet;

    public TableScanIOEstimator(TableScan tableScan, RelMetadataQuery metadataQuery) {
        super(metadataQuery, tableScan.getCluster().getRexBuilder(),
            (Math.ceil((TUPLE_HEADER_SIZE + TableScanIOEstimator.estimateRowSize(tableScan.getRowType()))
                * tableScan.getTable().getRowCount() / CostModelWeight.SEQ_IO_PAGE_SIZE)),
            PlannerContext.getPlannerContext(tableScan));
        this.tableScan = tableScan;
        this.tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        this.tableRowCount = tableMeta.getRowCount(plannerContext);
        this.rowSize = TUPLE_HEADER_SIZE + TableScanIOEstimator.estimateRowSize(tableScan.getRowType());
        this.accessIndexList = new ArrayList<>();
        this.canUseIndexSet = IndexUtil.getCanUseIndexSet(tableScan);
    }

    @Override
    public Double visitCall(RexCall call) {
        accessIndexList = new ArrayList<>();
        if (call.getOperator() == SqlStdOperatorTable.OR) {
            List<List<Index>> orAccessIndexLists = new ArrayList<>();
            Double orIO = call.getOperands().stream().map(
                rexNode -> {
                    Double result = this.evaluate(rexNode);
                    orAccessIndexLists.add(accessIndexList);
                    return result;
                }).reduce(0.0, (a, b) -> normalize(a) + normalize(b));

            boolean mergeIndex = true;
            for (List<Index> ail : orAccessIndexLists) {
                if (ail.isEmpty()) {
                    mergeIndex = false;
                }
            }

            // deduplicate
            Map<IndexMeta, Index> deDuplicateMap = new HashMap<>();
            if (mergeIndex) {
                for (List<Index> ail : orAccessIndexLists) {
                    for (Index index : ail) {
                        deDuplicateMap.put(index.getIndexMeta(), index);
                    }
                }
            }

            accessIndexList = new ArrayList<>();
            accessIndexList.addAll(deDuplicateMap.values());

            return normalize(orIO);
        } else if (call.getOperator() == SqlStdOperatorTable.NOT) {
            Double io = this.evaluate(call.getOperands().get(0));
            return normalize(io);
        } else {
            return normalize(getTableScanConjunctionsIO(call));
        }
    }

    public double getTableScanConjunctionsIO(RexNode predicate) {
        if ((predicate == null) || predicate.isAlwaysTrue() || tableMeta == null) {
            return getMaxIO();
        }

        if (predicate.isAlwaysFalse()) {
            return 0;
        }

        if (tableRowCount == null || tableRowCount <= 0) {
            return getMaxIO();
        }

        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);

        Pair<Index, Long> pair;
        /*
         * deal with primary key with equal predicate
         * Example: t.primary_key = 1
         */
        if ((pair = primaryKeyAppearEqualPredicate(tableMeta, conjunctions)) != null) {
            accessIndexList.add(pair.getKey());
            return pair.getValue();
        }

        /*
         * deal with primary key with in predicate
         * Example: t.primary_key in (1,2,3,4,5)
         */
        if ((pair = primaryKeyAppearInPredicate(tableMeta, conjunctions)) != null) {
            accessIndexList.add(pair.getKey());
            return pair.getValue();
        }

        /*
         * deal with unique key with equal predicate
         * Example: t.unique_key = 1
         */
        if ((pair = uniqueKeyAppearEqualPredicate(tableMeta, conjunctions)) != null) {
            accessIndexList.add(pair.getKey());
            return pair.getValue();
        }

        /*
         * deal with unique key with in predicate
         * Example: t.unique_key in (1,2,3,4,5)
         */
        if ((pair = uniqueKeyAppearInPredicate(tableMeta, conjunctions)) != null) {
            accessIndexList.add(pair.getKey());
            return pair.getValue();
        }

        double io;
        if ((pair = otherIndexIO(tableMeta, conjunctions, plannerContext)) != null) {
            accessIndexList.add(pair.getKey());
            io = pair.getValue();
        } else {
            io = getMaxIO();
        }

        /*
         * deal with other predicate
         */
        List<Index> minIoAccessIndexList = accessIndexList;
        for (RexNode otherPredicate : conjunctions) {
            if (otherPredicate instanceof RexCall) {
                RexCall otherCall = (RexCall) otherPredicate;
                if (otherCall.getOperator() == SqlStdOperatorTable.OR) {
                    double orIo = this.evaluate(otherCall);
                    io = Math.min(io, orIo);
                    if (orIo < io) {
                        minIoAccessIndexList = accessIndexList;
                    }
                } else if (otherCall.getOperator() == SqlStdOperatorTable.NOT) {
                    double notIo = this.evaluate(otherCall);
                    io = Math.min(io, notIo);
                    if (notIo < io) {
                        minIoAccessIndexList = accessIndexList;
                    }
                }
            }
        }

        accessIndexList = minIoAccessIndexList;

        return normalize(io);
    }

    private Pair<Index, Long> primaryKeyAppearEqualPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        if (!tableMeta.isHasPrimaryKey()) {
            return null;
        }

        if (!canUseIndexSet.contains(tableMeta.getPrimaryIndex().getPhysicalIndexName())) {
            return null;
        }

        if (indexAppearEqualPredicate(tableMeta.getPrimaryIndex(), tableMeta, conjunctions)) {
            Index index = new Index(tableMeta.getPrimaryIndex(),
                tableMeta.getPrimaryIndex().getKeyColumns().stream().map(x -> Index.PredicateType.EQUAL).collect(
                    Collectors.toList()),
                1 / tableRowCount, 1);
            return Pair.of(index, 1L);
        }
        return null;
    }

    private Pair<Index, Long> uniqueKeyAppearEqualPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        for (IndexMeta indexMeta : tableMeta.getSecondaryIndexes()) {

            if (!canUseIndexSet.contains(indexMeta.getPhysicalIndexName())) {
                continue;
            }

            if (indexMeta.isUniqueIndex() && indexAppearEqualPredicate(indexMeta, tableMeta, conjunctions)) {
                Index index =
                    new Index(indexMeta, indexMeta.getKeyColumns().stream().map(x -> Index.PredicateType.EQUAL).collect(
                        Collectors.toList()), 1 / tableRowCount, 1);
                return Pair.of(index, 1L);
            }
        }
        return null;
    }

    private IndexContext oneIndexColumnRangeContext(List<RexNode> conjunctions, ColumnMeta indexColumnMeta,
                                                    PlannerContext plannerContext) {
        IndexContext indexContext = null;
        List<Object> toRemovePredicateList = new ArrayList<>();

        DataType dataType = StatisticManager.getInstance()
            .getDataType(tableMeta.getSchemaName(), tableMeta.getTableName(), indexColumnMeta.getName());
        boolean lowerInclusive = false;
        boolean upperInclusive = false;
        Object lower = null;
        Object upper = null;
        for (RexNode pred : conjunctions) {
            if (!(pred instanceof RexCall)) {
                continue;
            }
            boolean remove = false;
            List<RexNode> operands = ((RexCall) pred).getOperands();
            if (pred.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
                if (operands.size() == 2) {
                    Object value;
                    if (operands.get(0) instanceof RexInputRef
                        && ((RexInputRef) operands.get(0)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(1), plannerContext)) != null) {
                        if (upper == null) {
                            upper = value;
                            upperInclusive = true;
                        } else if (dataType != null && dataType.compare(value, upper) < 0) {
                            upper = value;
                            upperInclusive = true;
                        }
                        remove = true;
                    } else if (operands.get(1) instanceof RexInputRef
                        && ((RexInputRef) operands.get(1)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(0), plannerContext)) != null) {
                        if (lower == null) {
                            lower = value;
                            lowerInclusive = true;
                        } else if (dataType != null && dataType.compare(value, lower) > 0) {
                            lower = value;
                            lowerInclusive = true;
                        }
                        remove = true;
                    }
                }
            } else if (pred.isA(SqlKind.LESS_THAN)) {
                if (operands.size() == 2) {
                    Object value;
                    if (operands.get(0) instanceof RexInputRef
                        && ((RexInputRef) operands.get(0)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(1), plannerContext)) != null) {
                        if (upper == null) {
                            upper = value;
                            upperInclusive = false;
                        } else if (dataType != null && dataType.compare(value, upper) <= 0) {
                            upper = value;
                            upperInclusive = false;
                        }
                        remove = true;
                    } else if (operands.get(1) instanceof RexInputRef
                        && ((RexInputRef) operands.get(1)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(0), plannerContext)) != null) {
                        if (lower == null) {
                            lower = value;
                            lowerInclusive = false;
                        } else if (dataType != null && dataType.compare(value, lower) >= 0) {
                            lower = value;
                            lowerInclusive = false;
                        }
                        remove = true;
                    }
                }
            } else if (pred.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
                if (operands.size() == 2) {
                    Object value;
                    if (operands.get(0) instanceof RexInputRef
                        && ((RexInputRef) operands.get(0)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(1), plannerContext)) != null) {
                        if (lower == null) {
                            lower = value;
                            lowerInclusive = true;
                        } else if (dataType != null && dataType.compare(value, lower) > 0) {
                            lower = value;
                            lowerInclusive = true;
                        }
                        remove = true;
                    } else if (operands.get(1) instanceof RexInputRef
                        && ((RexInputRef) operands.get(1)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(0), plannerContext)) != null) {
                        if (upper == null) {
                            upper = value;
                            upperInclusive = true;
                        } else if (dataType != null && dataType.compare(value, upper) < 0) {
                            upper = value;
                            upperInclusive = true;
                        }
                        remove = true;
                    }
                }
            } else if (pred.isA(SqlKind.GREATER_THAN)) {
                if (operands.size() == 2) {
                    Object value;
                    if (operands.get(0) instanceof RexInputRef
                        && ((RexInputRef) operands.get(0)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(1), plannerContext)) != null) {
                        if (lower == null) {
                            lower = value;
                            lowerInclusive = false;
                        } else if (dataType != null && dataType.compare(value, lower) >= 0) {
                            lower = value;
                            lowerInclusive = false;
                        }
                        remove = true;
                    } else if (operands.get(1) instanceof RexInputRef
                        && ((RexInputRef) operands.get(1)).getIndex() == DrdsRelMdSelectivity
                        .getColumnIndex(tableMeta, indexColumnMeta)
                        && (value = DrdsRexFolder.fold(operands.get(0), plannerContext)) != null) {
                        if (upper == null) {
                            upper = value;
                            upperInclusive = false;
                        } else if (dataType != null && dataType.compare(value, upper) <= 0) {
                            upper = value;
                            upperInclusive = false;
                        }
                        remove = true;
                    }
                }
            } else if (pred.isA(SqlKind.BETWEEN)) {
                Object leftValue;
                Object rightValue;
                if (operands.size() == 3 && operands.get(0) instanceof RexInputRef
                    && ((RexInputRef) operands.get(0)).getIndex() == DrdsRelMdSelectivity
                    .getColumnIndex(tableMeta, indexColumnMeta)
                    && (leftValue = DrdsRexFolder.fold(operands.get(1), plannerContext)) != null
                    && (rightValue = DrdsRexFolder.fold(operands.get(2), plannerContext)) != null) {
                    if (lower == null) {
                        lower = leftValue;
                        lowerInclusive = true;
                    } else if (dataType != null && dataType.compare(leftValue, lower) > 0) {
                        lower = leftValue;
                        lowerInclusive = true;
                    }
                    if (upper == null) {
                        upper = rightValue;
                        upperInclusive = true;
                    } else if (dataType != null && dataType.compare(rightValue, upper) < 0) {
                        upper = rightValue;
                        upperInclusive = true;
                    }
                    remove = true;
                }
            } else if (pred.isA(SqlKind.LIKE)) {
                Object likeValue;
                Object leftValue;
                Object rightValue;
                if (operands.size() == 2 && operands.get(0) instanceof RexInputRef
                    && ((RexInputRef) operands.get(0)).getIndex() == DrdsRelMdSelectivity
                    .getColumnIndex(tableMeta, indexColumnMeta)
                    && (likeValue = DrdsRexFolder.fold(operands.get(1), plannerContext)) != null) {
                    final String likeString;
                    if (likeValue instanceof char[]) {
                        likeString = String.valueOf(likeValue);
                    } else if (likeValue instanceof String) {
                        likeString = (String) likeValue;
                    } else {
                        continue;
                    }

                    int idx = likeString.indexOf("%");
                    if (idx == -1) {
                        leftValue = likeString;
                        rightValue = likeString;
                    } else if (idx == 0) {
                        continue;
                    } else {
                        leftValue = likeString.substring(0, idx);
                        char[] rightCharArray = new char[idx];
                        for (int i = 0; i < idx; i++) {
                            rightCharArray[i] = likeString.charAt(i);
                        }
                        if (rightCharArray[idx - 1] != 255) {
                            rightCharArray[idx - 1]++;
                        }
                        rightValue = String.valueOf(rightCharArray);
                    }

                    if (lower == null) {
                        lower = leftValue;
                        lowerInclusive = true;
                    } else if (dataType != null && dataType.compare(leftValue, lower) > 0) {
                        lower = leftValue;
                        lowerInclusive = true;
                    }
                    if (upper == null) {
                        upper = rightValue;
                        upperInclusive = true;
                    } else if (dataType != null && dataType.compare(rightValue, upper) < 0) {
                        upper = rightValue;
                        upperInclusive = true;
                    }
                    remove = true;
                }
            } else if (pred.isA(SqlKind.NOT_EQUALS)) {

            } else if (pred.isA(SqlKind.NOT_IN)) {

            }

            if (remove) {
                toRemovePredicateList.add(pred);
            }
        }

        if (lower == null && upper == null) {
            return null;
        } else {
            StatisticResult statisticResult = StatisticManager.getInstance()
                .getRangeCount(tableMeta.getSchemaName(), tableMeta.getTableName(), indexColumnMeta.getName(), lower,
                    lowerInclusive, upper,
                    upperInclusive, plannerContext.isNeedStatisticTrace());
            if (plannerContext.isNeedStatisticTrace()) {
                plannerContext.recordStatisticTrace(statisticResult.getTrace());
            }
            long count = statisticResult.getLongValue();
            if (count >= 0) {
                // pass
            } else {
                // lack of statistics
                count = Math.min(LACK_OF_STATISTICS_INDEX_RANGE_ROW_COUNT, tableRowCount.longValue());
            }
            indexContext =
                new IndexContext(Index.PredicateType.RANGE, indexColumnMeta.getName(), count / tableRowCount, 1);
        }

        for (Object obj : toRemovePredicateList) {
            conjunctions.remove(obj);
        }
        return indexContext;
    }

    private IndexContext oneIndexColumnInContext(RexNode pred, ColumnMeta indexColumnMeta,
                                                 PlannerContext plannerContext) {
        if (pred.isA(SqlKind.IN) && pred instanceof RexCall) {
            RexCall in = (RexCall) pred;
            if (in.getOperands().size() == 2) {
                RexNode leftRexNode = in.getOperands().get(0);
                RexNode rightRexNode = in.getOperands().get(1);
                /** In Predicate contains one column */
                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
                    .isA(SqlKind.ROW)) {
                    int leftIndex = ((RexInputRef) leftRexNode).getIndex();

                    ColumnMeta columnMeta = findColumnMeta(tableMeta, leftIndex);
                    long frequency = 0;
                    if (columnMeta != null && columnMeta.equals(indexColumnMeta)) {
                        int fanOut = ((RexCall) rightRexNode).operands.size();
                        StatisticResult statisticResult;
                        for (RexNode rexNode : ((RexCall) rightRexNode).operands) {
                            Object value = DrdsRexFolder.fold(rexNode, plannerContext);
                            if (value instanceof List) {
                                statisticResult = StatisticManager.getInstance()
                                    .getFrequency(tableMeta.getSchemaName(), tableMeta.getTableName(),
                                        columnMeta.getName(),
                                        (List) value, plannerContext.isNeedStatisticTrace());
                            } else {
                                statisticResult = StatisticManager.getInstance()
                                    .getFrequency(tableMeta.getSchemaName(), tableMeta.getTableName(),
                                        columnMeta.getName(),
                                        value == null ? null : value.toString(),
                                        plannerContext.isNeedStatisticTrace());
                            }
                            if (plannerContext.isNeedStatisticTrace()) {
                                plannerContext.recordStatisticTrace(statisticResult.getTrace());
                            }
                            frequency += statisticResult.getLongValue();

                        }

                        // if statistic result is empty, assign one default value
                        if (frequency < 0) {
                            // Meaning lack of statistics
                            frequency = Math.min(LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT, tableRowCount.longValue())
                                * fanOut;
                        }

                        return
                            new IndexContext(Index.PredicateType.IN, columnMeta.getOriginColumnName(),
                                frequency / (double) tableRowCount.longValue(), 1);
                    }
                }
            }
            /** TODO:In Predicate contains more than two column */
        }
        return null;
    }

    private IndexContext oneIndexColumnEqualContext(RexNode pred, ColumnMeta indexColumnMeta,
                                                    PlannerContext plannerContext) {
        if (pred.isA(SqlKind.EQUALS) && pred instanceof RexCall) { // equal
            RexCall filterCall = (RexCall) pred;
            RexNode leftRexNode = filterCall.getOperands().get(0);
            RexNode rightRexNode = filterCall.getOperands().get(1);
            RexInputRef inputRef;
            Object value;

            if (leftRexNode instanceof RexInputRef && !(rightRexNode instanceof RexInputRef)) {
                inputRef = (RexInputRef) leftRexNode;
                value = DrdsRexFolder.fold(rightRexNode, plannerContext);
            } else if (rightRexNode instanceof RexInputRef && !(leftRexNode instanceof RexInputRef)) {
                inputRef = (RexInputRef) rightRexNode;
                value = DrdsRexFolder.fold(leftRexNode, plannerContext);
            } else {
                return null;
            }

            ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef.getIndex());

            if (columnMeta != null && value != null && indexColumnMeta.equals(columnMeta)) {
                StatisticResult statisticResult = StatisticManager.getInstance().getFrequency(tableMeta.getSchemaName(),
                    tableMeta.getTableName(), columnMeta.getName(), value.toString(),
                    plannerContext.isNeedStatisticTrace());
                if (plannerContext.isNeedStatisticTrace()) {
                    plannerContext.recordStatisticTrace(statisticResult.getTrace());
                }
                long count = statisticResult.getLongValue();
                if (count >= 0) {

                } else {
                    // lack of statistics
                    count = Math.min(LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT, tableRowCount.longValue());
                }
                IndexContext indexContext =
                    new IndexContext(Index.PredicateType.EQUAL, columnMeta.getOriginColumnName(),
                        count / tableRowCount, 1);
                return indexContext;
            }
        }
        return null;
    }

    private IndexContext oneIndexColumnIsNullContext(RexNode pred, ColumnMeta indexColumnMeta,
                                                     PlannerContext plannerContext) {
        if (pred.isA(SqlKind.IS_NULL) && pred instanceof RexCall) { // isNull
            RexCall filterCall = (RexCall) pred;
            RexNode leftRexNode = filterCall.getOperands().get(0);
            RexInputRef inputRef;
            Object value;

            if (leftRexNode instanceof RexInputRef) {
                inputRef = (RexInputRef) leftRexNode;
            } else {
                return null;
            }

            ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef.getIndex());

            if (columnMeta != null && indexColumnMeta.equals(columnMeta)) {
                StatisticResult statisticResult = StatisticManager.getInstance()
                    .getNullCount(tableMeta.getSchemaName(), tableMeta.getTableName(), columnMeta.getName(),
                        plannerContext.isNeedStatisticTrace());
                if (plannerContext.isNeedStatisticTrace()) {
                    plannerContext.recordStatisticTrace(statisticResult.getTrace());
                }
                long count = statisticResult.getLongValue();
                if (count >= 0) {
                    IndexContext indexContext =
                        new IndexContext(Index.PredicateType.EQUAL, columnMeta.getOriginColumnName(),
                            count / tableRowCount, 1);
                    return indexContext;
                }
            }
        }
        return null;
    }

    private IndexContext oneIndexColumnContext(ColumnMeta indexColumnMeta, List<RexNode> conjunctions,
                                               PlannerContext plannerContext) {
        // check equal and in
        for (RexNode pred : conjunctions) {
            IndexContext indexContext = null;
            indexContext = oneIndexColumnEqualContext(pred, indexColumnMeta, plannerContext);
            if (indexContext != null) {
                conjunctions.remove(pred);
                return indexContext;
            }

            indexContext = oneIndexColumnIsNullContext(pred, indexColumnMeta, plannerContext);
            if (indexContext != null) {
                conjunctions.remove(pred);
                return indexContext;
            }

            indexContext = oneIndexColumnInContext(pred, indexColumnMeta, plannerContext);
            if (indexContext != null) {
                conjunctions.remove(pred);
                return indexContext;
            }
        }
        // then check range
        return oneIndexColumnRangeContext(conjunctions, indexColumnMeta, plannerContext);
    }

    class IndexContext {
        public Index.PredicateType predicateType;
        public double selectivity;
        public int fanOut;

        public String columnName;

        public IndexContext(Index.PredicateType predicateType, String columnName, double selectivity,
                            int fanOut) {
            this.predicateType = predicateType;
            this.selectivity = selectivity;
            this.fanOut = fanOut;
            this.columnName = columnName;
        }
    }

    /**
     * @param indexMeta index meta data
     * @param conjunctions predicate list concatenated with and
     * @param plannerContext plannerContext use to calculate io
     * @return the chosen index and its io
     */
    private Pair<Index, Long> oneIndexIO(IndexMeta indexMeta, final List<RexNode> conjunctions,
                                         PlannerContext plannerContext) {
        List<RexNode> newConjunctions = new ArrayList<>();
        newConjunctions.addAll(conjunctions);
        List<IndexContext> indexContextList = new ArrayList<>();

        for (ColumnMeta indexColumnMeta : indexMeta.getKeyColumns()) {
            IndexContext indexContext = oneIndexColumnContext(indexColumnMeta, newConjunctions, plannerContext);
            if (indexContext == null) {
                break;
            }
            indexContextList.add(indexContext);
            if (indexContext.predicateType == Index.PredicateType.RANGE) {
                break;
            }
        }

        if (indexContextList.isEmpty()) {
            return null;
        }

        // try handle multi index
        if (indexMeta.getKeyColumns().size() > 1) {
            mergeIndexContextList(tableMeta.getSchemaName(), tableMeta.getTableName(), indexMeta.getKeyColumns(),
                indexContextList);
        }

        double rowCount = tableRowCount;
        int fanOut = 1;
        for (IndexContext indexContext : indexContextList) {
            rowCount = Math.ceil(rowCount * indexContext.selectivity);
            fanOut *= indexContext.fanOut;
        }

        double ioPageSize;
        if (indexContextList.get(indexContextList.size() - 1).predicateType == Index.PredicateType.RANGE) {
            if (indexMeta.isPrimaryKeyIndex()) {
                // clustering index
                ioPageSize = CostModelWeight.SEQ_IO_PAGE_SIZE;
            } else {
                // non-clustering index
                ioPageSize = CostModelWeight.SEQ_IO_PAGE_SIZE / 2;
            }
        } else {
            ioPageSize = CostModelWeight.RAND_IO_PAGE_SIZE;
        }

        long io = (long) Math.ceil(rowCount * rowSize / ioPageSize) * fanOut;
        return Pair.of(new Index(indexMeta,
            indexContextList.stream().map(x -> x.predicateType).collect(Collectors.toList()),
            rowCount / tableRowCount, 1), io);
    }

    /**
     * if multi index has special statistic info and the condition columns match all key columns.
     * then use the special statistic info to remeasure the cost.
     *
     * @param keyColumns key columns in the index
     * @param indexContextList index context list
     */
    private void mergeIndexContextList(String schemaName, String logicalTableName,
                                       List<ColumnMeta> keyColumns,
                                       List<IndexContext> indexContextList) {
        List<IndexContext> mergeList = Lists.newLinkedList();
        for (ColumnMeta columnMeta : keyColumns) {
            for (IndexContext indexContext : indexContextList) {
                if (mergeList.contains(indexContext)) {
                    continue;
                }
                if (indexContext.columnName.equalsIgnoreCase(columnMeta.getOriginColumnName()) && (
                    indexContext.predicateType == Index.PredicateType.EQUAL
                        || indexContext.predicateType == Index.PredicateType.IN)) {
                    mergeList.add(indexContext);
                    break;
                }
            }
        }

        // size equal meaning totally match this index(multi column)
        if (mergeList.size() == keyColumns.size()) {
            int fanOut = 1;
            for (IndexContext indexContext : mergeList) {
                fanOut *= indexContext.fanOut;
            }

            Set<String> cols = keyColumns.stream().map(columnMeta -> columnMeta.getOriginColumnName()).collect(
                Collectors.toSet());

            String columnsName = StatisticUtils.buildColumnsName(cols);
            StatisticResult result =
                StatisticManager.getInstance().getCardinality(schemaName, logicalTableName, columnsName, true,
                    plannerContext.isNeedStatisticTrace());
            if (plannerContext.isNeedStatisticTrace()) {
                plannerContext.recordStatisticTrace(result.getTrace());
            }
            // empty meaning the multi column statistic info is not exists, give up
            if (result.getSource() == StatisticResultSource.NULL) {
                return;
            }
            // invalid statistic info, give up
            if (result.getLongValue() < 0) {
                return;
            }
            IndexContext replace =
                new IndexContext(Index.PredicateType.EQUAL, columnsName,
                    1D / result.getLongValue(), fanOut);
            indexContextList.removeAll(mergeList);
            indexContextList.add(replace);
        }
    }

    /**
     * @param tableMeta table meta data
     * @param conjunctions predicate list concatenated with and
     * @param plannerContext plannerContext use to calculate io
     * @return consider each indexes of table and choose the min-io one
     */
    private Pair<Index, Long> otherIndexIO(TableMeta tableMeta, List<RexNode> conjunctions,
                                           PlannerContext plannerContext) {
        Pair<Index, Long> result = null;
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {

            if (!canUseIndexSet.contains(indexMeta.getPhysicalIndexName())) {
                continue;
            }

            Pair<Index, Long> pair = oneIndexIO(indexMeta, conjunctions, plannerContext);
            if (pair != null) {
                if (result == null) {
                    result = pair;
                } else if (pair.getValue() < result.getValue()) {
                    result = pair;
                } else if (pair.getValue().equals(result.getValue())
                    && pair.getKey().getIndexMeta().getKeyColumns().size()
                    > result.getKey().getIndexMeta().getKeyColumns().size()) {
                    result = pair;
                }
            }
        }
        return result;
    }

    private ColumnMeta findColumnMeta(TableMeta tableMeta, int index) {
        if (index < 0 || index > tableMeta.getAllColumns().size()) {
            return null;
        }
        return tableMeta.getAllColumns().get(index);
    }

    /**
     * return Index if primary key appear in predicate
     * otherwise return null
     **/
    private Pair<Index, Long> primaryKeyAppearInPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        if (!tableMeta.isHasPrimaryKey()) {
            return null;
        }

        if (!canUseIndexSet.contains(tableMeta.getPrimaryIndex().getPhysicalIndexName())) {
            return null;
        }

        long inValue = indexKeyAppearInPredicate(tableMeta.getPrimaryIndex(), tableMeta, conjunctions);
        if (inValue > 0) {
            Index index = new Index(tableMeta.getPrimaryIndex(),
                tableMeta.getPrimaryIndex().getKeyColumns().stream().map(x -> Index.PredicateType.IN).collect(
                    Collectors.toList()), inValue / tableRowCount, 1);
            return Pair.of(index, inValue);
        } else {
            return null;
        }
    }

    private Pair<Index, Long> uniqueKeyAppearInPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        Pair<Index, Long> result = null;
        for (IndexMeta indexMeta : tableMeta.getSecondaryIndexes()) {

            if (!canUseIndexSet.contains(indexMeta.getPhysicalIndexName())) {
                continue;
            }

            if (indexMeta.isUniqueIndex()) {
                long inNum = indexKeyAppearInPredicate(indexMeta, tableMeta, conjunctions);
                Index index;
                if (inNum > 0) {
                    index =
                        new Index(indexMeta,
                            indexMeta.getKeyColumns().stream().map(x -> Index.PredicateType.IN).collect(
                                Collectors.toList()), inNum / tableRowCount, 1);
                } else {
                    continue;
                }

                if (result == null) {
                    result = Pair.of(index, inNum);
                } else if (result != null && inNum < result.getValue()) {
                    result = Pair.of(index, inNum);
                }
            }
        }
        return result;
    }

    /**
     * @param indexMeta index meta data
     * @param tableMeta table meta data
     * @param conjunctions predicate list concatenated with and
     * Example: [t.a = 1, t.b in (2,3,4), t.c > 3] stand for t.a = 1 and t.b in (2,3,4) and t.c > 3
     * @return the number size of In predicate if index in (xxx, xx, xxx), otherwise 0;
     */
    private long indexKeyAppearInPredicate(IndexMeta indexMeta, TableMeta tableMeta, List<RexNode> conjunctions) {
        for (RexNode pred : conjunctions) {
            long size = indexCoverByInPredicate(indexMeta, tableMeta, pred);
            if (size > 0) {
                return size;
            }
        }
        return 0;
    }

    /**
     * @param indexMeta index meta data
     * @param tableMeta table meta data
     * @param pred predicate Example: t.b in (2,3,4)
     * @return the number size of In predicate if index in (xxx, xx, xxx), otherwise 0;
     */
    private long indexCoverByInPredicate(IndexMeta indexMeta, TableMeta tableMeta, RexNode pred) {
        if (!pred.isA(SqlKind.IN) || !(pred instanceof RexCall)) {
            return 0;
        }
        List<ColumnMeta> indexColumnMetaList = indexMeta.getKeyColumns();
        RexCall filterCall = (RexCall) pred;
        if (filterCall.getOperands().size() == 2) {
            RexNode leftRexNode = filterCall.getOperands().get(0);
            RexNode rightRexNode = filterCall.getOperands().get(1);
            /** index only contains one column */
            if (indexColumnMetaList.size() == 1) {
                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
                    .isA(SqlKind.ROW)) {
                    int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                    if (leftIndex == DrdsRelMdSelectivity.getColumnIndex(tableMeta, indexColumnMetaList.get(0))) {
                        return ((RexCall) rightRexNode).getOperands().size();
                    }
                }
            }

            /** index contains more than one column */
            if (leftRexNode instanceof RexCall && leftRexNode.isA(SqlKind.ROW)
                && rightRexNode instanceof RexCall && leftRexNode.isA(SqlKind.ROW)
                && indexCoverByInPredicate(indexColumnMetaList, ((RexCall) leftRexNode).getOperands(), tableMeta)) {
                return ((RexCall) rightRexNode).getOperands().size();
            }
        }
        return 0;
    }

    /**
     * @return true if all index columns are covered by RexNode in inList
     */
    private boolean indexCoverByInPredicate(List<ColumnMeta> indexColumnMetaList, List<RexNode> inList,
                                            TableMeta tableMeta) {
        boolean cover = true;
        for (ColumnMeta columnMeta : indexColumnMetaList) {
            boolean subCover = false;
            for (RexNode inRexNode : inList) {
                if (inRexNode instanceof RexInputRef) {
                    int idx = ((RexInputRef) inRexNode).getIndex();
                    if (idx == DrdsRelMdSelectivity.getColumnIndex(tableMeta, columnMeta)) {
                        subCover = true;
                    }
                }
            }
            cover &= subCover;
        }
        return cover;
    }

    /**
     * @param indexMeta index meta data
     * @param tableMeta table meta data
     * @param conjunctions predicate list concatenated with and
     * Example: [t.a = 1, t.b = 2, t.c > 3] stand for t.a = 1 and t.b = 2 and t.c > 3
     * @return true if indexMeta in EqualPredicate, otherwise false
     */
    private boolean indexAppearEqualPredicate(IndexMeta indexMeta, TableMeta tableMeta, List<RexNode> conjunctions) {
        boolean indexMatch = false;
        List<ColumnMeta> indexColumnMetaList = indexMeta.getKeyColumns();
        for (ColumnMeta columnMeta : indexColumnMetaList) {
            boolean subMatch = false;
            for (RexNode pred : conjunctions) {
                if (pred.isA(SqlKind.EQUALS) && pred instanceof RexCall) {
                    RexCall filterCall = (RexCall) pred;
                    RexNode leftRexNode = filterCall.getOperands().get(0);
                    RexNode rightRexNode = filterCall.getOperands().get(1);
                    if (leftRexNode instanceof RexInputRef) {
                        int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                        if (leftIndex == DrdsRelMdSelectivity.getColumnIndex(tableMeta, columnMeta)) {
                            subMatch = true;
                            break;
                        }
                    }
                    if (rightRexNode instanceof RexInputRef) {
                        int rightIndex = ((RexInputRef) rightRexNode).getIndex();
                        if (rightIndex == DrdsRelMdSelectivity.getColumnIndex(tableMeta, columnMeta)) {
                            subMatch = true;
                            break;
                        }
                    }
                }
            }
            indexMatch = subMatch;
            if (subMatch == false) {
                break;
            }
        }
        return indexMatch;
    }

    public List<Index> getAccessIndexList() {
        return accessIndexList;
    }

    public static long estimateRowSize(RelDataType rowType) {
        return estimateRowSize(rowType.getFieldList());
    }

    private static long estimateRowSize(List<RelDataTypeField> fields) {
        long result = 0;
        for (RelDataTypeField field : fields) {
            if (field.getType() instanceof BasicSqlType) {
                BasicSqlType sqlType = (BasicSqlType) field.getType();
                result += estimateFieldSize(sqlType);
            } else if (field.getType().isStruct()) {
                result += estimateRowSize(field.getType().getFieldList());
            } else if (field.getType() instanceof EnumSqlType) {
                result += 10;
            } else {
                throw new AssertionError();
            }
        }
        return result;
    }

    private static long estimateFieldSize(BasicSqlType type) {
        switch (type.getSqlTypeName()) {
        case BOOLEAN:
            return 1;
        case TINYINT:
        case TINYINT_UNSIGNED:
        case SMALLINT:
            return Short.BYTES;
        case SMALLINT_UNSIGNED:
        case MEDIUMINT:
        case MEDIUMINT_UNSIGNED:
        case INTEGER:
        case SIGNED:
        case BIT:
            return Integer.BYTES;
        case INTEGER_UNSIGNED:
        case UNSIGNED:
        case BIGINT:
        case YEAR:
            return Long.BYTES;
        case BIGINT_UNSIGNED:
            return 9;
        case DECIMAL:
        case BIG_BIT:
            return 20; // average
        case FLOAT:
            return Float.BYTES;
        case REAL:
        case DOUBLE:
            return Double.BYTES;
        case DATETIME:
        case DATE:
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return 12; // see TimestampBlock
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
            return 8; // see TimeBlock
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_DAY_MICROSECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
            // These should not appear in MySQL result set... Anyway giving an estimation is not bad
            return Long.BYTES;
        case ENUM:
            return 10;
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY: {
            int precision = type.getPrecision();
            if (precision <= 0) {
                precision = 20;
            }
            return Math.min(precision, 100); // treat VARCHAR(256) as length 100
        }

        case NULL:
            return 0;
        default:
            // for all strange values
            return 100;
        }
    }
}

