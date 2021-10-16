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

package com.alibaba.polardbx.optimizer.selectivity;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMdSelectivity;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableScanSelectivityEstimator extends AbstractSelectivityEstimator {
    private final TableScan tableScan;
    private final Double tableRowCount;
    private final TableMeta tableMeta;
    private final PlannerContext plannerContext;

    public static int LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT = 100;

    public static int LACK_OF_STATISTICS_INDEX_RANGE_ROW_COUNT = 1000;

    public TableScanSelectivityEstimator(TableScan tableScan, RelMetadataQuery metadataQuery) {
        super(metadataQuery, tableScan.getCluster().getRexBuilder());
        this.tableScan = tableScan;
        this.tableRowCount = metadataQuery.getRowCount(tableScan);
        this.tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        this.plannerContext = PlannerContext.getPlannerContext(tableScan);
    }

    @Override
    public Double visitCall(RexCall call) {
        if (call.getOperator() == SqlStdOperatorTable.OR) {
            Double selectivityOr =
                call.getOperands().stream().map(
                    rexNode -> this.evaluate(rexNode)).reduce(0.0, (a, b) -> a + b - a * b);
            return normalize(selectivityOr);
        } else if (call.getOperator() == SqlStdOperatorTable.NOT) {
            Double selectivity = this.evaluate(call.getOperands().get(0));
            return normalize((1 - selectivity));
        } else {
            return normalize(getTableScanConjunctionsSelectivity(call));
        }
    }

    public double getTableScanConjunctionsSelectivity(RexNode predicate) {
        if ((predicate == null) || predicate.isAlwaysTrue() || tableMeta == null) {
            return 1.0;
        }

        if (tableRowCount == null || tableRowCount <= 0) {
            return RelMdUtil.guessSelectivity(predicate);
        }

        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);

        /*
         * deal with primary key with equal predicate
         * Example: t.primary_key = 1
         */
        if (primaryKeyAppearEqualPredicate(tableMeta, conjunctions)) {
            return Math.min(1.0 / tableRowCount, 0.1);
        }

        /*
         * deal with primary key with in predicate
         * Example: t.primary_key in (1,2,3,4,5)
         */
        long inNum = primaryKeyAppearInPredicate(tableMeta, conjunctions);
        if (inNum > 0) {
            return Math.min(inNum / tableRowCount, 0.9);
        }

        /*
         * deal with unique key with equal predicate
         * Example: t.unique_key = 1
         */
        if (uniqueKeyAppearEqualPredicate(tableMeta, conjunctions)) {
            return Math.min(1.0 / tableRowCount, 0.1);
        }

        /*
         * deal with unique key with in predicate
         * Example: t.unique_key in (1,2,3,4,5)
         */
        inNum = uniqueKeyAppearInPredicate(tableMeta, conjunctions);
        if (inNum > 0) {
            return Math.min((1.0 * inNum) / tableRowCount, 0.9);
        }

        /*
         * deal with other key with equal predicate, if hava Fequency statistic
         */
        double equalSelectivity = 1;
        Long otherNumOfEqual = otherKeyAppearEqualPredicate(tableMeta, conjunctions, plannerContext);
        if (otherNumOfEqual != null) {
            equalSelectivity = otherNumOfEqual / tableRowCount;
        }

        /*
         * deal with other key with in predicate, if hava Fequency statistic
         */
        double inSelectivity = 1;
        Long otherNumOfIn = otherColumnAppearInPredicate(tableMeta, conjunctions, plannerContext);
        if (otherNumOfIn != null) {
            inSelectivity = otherNumOfIn / tableRowCount;
        }
        /*
         * deal with other key with in null or not in null predicate, if hava null count statistic
         */
        double nullSelectivity = 1;
        Long columnOfNullOrNotNull = columnAppearNullOrNotNullPredicate(tableMeta, conjunctions, plannerContext);
        if (columnOfNullOrNotNull != null) {
            nullSelectivity = Math.min(columnOfNullOrNotNull / tableRowCount, 1);
        }

        /*
         * deal with other key with in null or not in null predicate, if hava null count statistic
         */
        double rangeSelectivity = 1;
        Long rangeCount = columnAppearRangePredicate(tableMeta, conjunctions, plannerContext);
        if (rangeCount != null) {
            rangeSelectivity = Math.min(rangeCount / tableRowCount, 1);
        }

        /*
         * deal with not equal predicate
         */
        double notEqualSelectivity = columnAppearNotEqualPredicate(tableMeta, conjunctions, plannerContext);
        notEqualSelectivity = notEqualSelectivity > 1 ? 1 : notEqualSelectivity;

        /*
         * deal with other predicate
         */
        double otherPredicateSelectivity = 1.0;
        for (RexNode otherPredicate : conjunctions) {
            if (otherPredicate instanceof RexCall) {
                RexCall otherCall = (RexCall) otherPredicate;
                if (otherCall.getOperator() == SqlStdOperatorTable.OR) {
                    otherPredicateSelectivity *= this.evaluate(otherCall);
                } else if (otherCall.getOperator() == SqlStdOperatorTable.NOT) {
                    otherPredicateSelectivity *= this.evaluate(otherCall);
                } else {
                    otherPredicateSelectivity *= RelMdUtil.guessSelectivity(otherPredicate);
                }
            } else {
                otherPredicateSelectivity *= RelMdUtil.guessSelectivity(otherPredicate);
            }
        }

        return Math.min(equalSelectivity, 1f) * Math.min(inSelectivity, 1f)
            * nullSelectivity * rangeSelectivity * otherPredicateSelectivity * notEqualSelectivity;
    }

    private boolean primaryKeyAppearEqualPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        if (!tableMeta.isHasPrimaryKey()) {
            return false;
        }
        return indexAppearEqualPredicate(tableMeta.getPrimaryIndex(), tableMeta, conjunctions);
    }

    private boolean uniqueKeyAppearEqualPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        for (IndexMeta indexMeta : tableMeta.getSecondaryIndexes()) {
            if (indexMeta.isUniqueIndex() && indexAppearEqualPredicate(indexMeta, tableMeta, conjunctions)) {
                return true;
            }
        }
        return false;
    }

    /**
     * return rowCount of Equal Predicate
     * if no statistic, return null
     */
    private Long otherKeyAppearEqualPredicate(TableMeta tableMeta, List<RexNode> conjunctions,
                                              PlannerContext plannerContext) {
        Long result = null;
        List<Object> toRemovePredicateList = new ArrayList<>();
        Set<Integer> rememberSet = new HashSet<>();

        for (RexNode pred : conjunctions) {
            if (pred.isA(SqlKind.EQUALS) && pred instanceof RexCall) {
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
                    continue;
                }

                if (rememberSet.contains(inputRef.getIndex())) {
                    toRemovePredicateList.add(pred);
                    continue;
                }

                ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef.getIndex());
                if (columnMeta != null && value != null) {
                    StatisticResult statisticResult =
                        OptimizerContext.getContext(tableMeta.getSchemaName()).getStatisticManager().getFrequency(
                            tableMeta.getTableName(), columnMeta.getName(), value.toString());
                    long count = statisticResult.getLongValue();
                    if (count >= 0) {
                        if (result == null) {
                            result = count;
                        } else {
                            result = (long) (result * count / tableRowCount);
                        }
                        toRemovePredicateList.add(pred);
                        rememberSet.add(inputRef.getIndex());
                    } else if (CBOUtil.isIndexColumn(tableMeta, columnMeta)) {
                        // lack of statistics
                        count = Math.min(LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT, tableRowCount.longValue());
                        if (result == null) {
                            result = count;
                        } else {
                            result = (long) (result * count / tableRowCount);
                        }
                        toRemovePredicateList.add(pred);
                        rememberSet.add(inputRef.getIndex());
                    }

                }
            }
        }
        for (Object obj : toRemovePredicateList) {
            conjunctions.remove(obj);
        }
        return result;
    }

    /**
     * return rowCount of In Predicate
     * if no statistic, return null
     */
    private Long otherColumnAppearInPredicate(TableMeta tableMeta, List<RexNode> conjunctions,
                                              PlannerContext plannerContext) {
        Long result = null;
        List<Object> toRemovePredicateList = new ArrayList<>();
        Set<Integer> rememberSet = new HashSet<>();
        for (RexNode pred : conjunctions) {
            if (!pred.isA(SqlKind.IN) || !(pred instanceof RexCall)) {
                continue;
            }
            RexCall filterCall = (RexCall) pred;
            Long countIn = singleColumnAppearInPredicate(tableMeta, filterCall, rememberSet,
                toRemovePredicateList, plannerContext);

            if (countIn != null) {
                if (result == null) {
                    result = countIn;
                } else {
                    result = (long) (result * countIn / tableRowCount);
                }
            }
            /** TODO:In Predicate contains more than two column */
        }
        for (Object obj : toRemovePredicateList) {
            conjunctions.remove(obj);
        }
        return result;
    }

    private Long singleColumnAppearInPredicate(TableMeta tableMeta, RexCall in, Set<Integer> rememberSet,
                                               List<Object> toRemovePredicateList, PlannerContext plannerContext) {
        Long inCount = null;
        if (in.getOperands().size() == 2) {
            RexNode leftRexNode = in.getOperands().get(0);
            RexNode rightRexNode = in.getOperands().get(1);
            /** In Predicate contains one column */
            if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
                .isA(SqlKind.ROW)) {
                int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                ColumnMeta columnMeta = findColumnMeta(tableMeta, leftIndex);
                if (columnMeta != null) {

                    if (rememberSet.contains(leftIndex)) {
                        toRemovePredicateList.add(in);
                        return null;
                    }

                    for (RexNode rexNode : ((RexCall) rightRexNode).operands) {
                        Object value = DrdsRexFolder.fold(rexNode, plannerContext);
                        if (value != null) {
                            StatisticResult statisticResult =
                                OptimizerContext.getContext(tableMeta.getSchemaName()).getStatisticManager()
                                    .getFrequency(tableMeta.getTableName(), columnMeta.getName(), value.toString());
                            long count = statisticResult.getLongValue();
                            if (count >= 0) {
                                if (inCount == null) {
                                    inCount = count;
                                } else {
                                    inCount += count;
                                }
                            } else if (CBOUtil.isIndexColumn(tableMeta, columnMeta)) {
                                // lack of statistics
                                count = Math.min(LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT, tableRowCount.longValue());
                                if (inCount == null) {
                                    inCount = count;
                                } else {
                                    inCount += count;
                                }
                            }
                        }
                    }

                    if (inCount != null) {
                        toRemovePredicateList.add(in);
                        rememberSet.add(leftIndex);
                    }
                }
            }
        }
        return inCount;
    }

    private ColumnMeta findColumnMeta(TableMeta tableMeta, int index) {
        if (index < 0 || index > tableMeta.getAllColumns().size()) {
            return null;
        }
        return tableMeta.getAllColumns().get(index);
    }

    /**
     * return min Count of column in range Predicate
     * if no statistic, return -1
     */
    private Long columnAppearRangePredicate(TableMeta tableMeta, List<RexNode> conjunctions,
                                            PlannerContext plannerContext) {
        Long minCount = null;
        List<ColumnMeta> columnMetaList = tableMeta.getAllColumns();
        List<Object> toRemovePredicateList = new ArrayList<>();
        for (ColumnMeta columnMeta : columnMetaList) {
            DataType dataType = OptimizerContext.getContext(tableMeta.getSchemaName()).getStatisticManager()
                .getDataType(tableMeta.getTableName(), columnMeta.getName());
//            if (histogram == null) {
//                continue;
//            }
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                            .getColumnIndex(tableMeta, columnMeta)
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
                        .getColumnIndex(tableMeta, columnMeta)
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
                        .getColumnIndex(tableMeta, columnMeta)
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
                        if (idx == 0) {
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
                continue;
            } else {
                StatisticResult statisticResult =
                    OptimizerContext.getContext(tableMeta.getSchemaName()).getStatisticManager()
                        .getRangeCount(tableMeta.getTableName(), columnMeta.getName(), lower, lowerInclusive, upper,
                            upperInclusive);
                long count = statisticResult.getLongValue();
                if (count >= 0) {
                    if (minCount == null) {
                        minCount = count;
                    } else if (count < minCount) {
                        minCount = count;
                    }
                } else if (CBOUtil.isIndexColumn(tableMeta, columnMeta)) {
                    // lack of statistics
                    count = Math.min(LACK_OF_STATISTICS_INDEX_RANGE_ROW_COUNT, tableRowCount.longValue());
                    if (minCount == null) {
                        minCount = count;
                    } else if (count < minCount) {
                        minCount = count;
                    }
                }
            }

        }
        for (Object obj : toRemovePredicateList) {
            conjunctions.remove(obj);
        }
        return minCount;
    }

    /**
     * return min Count of in null or not in null Predicate
     * if no statistic, return -1
     */
    private double columnAppearNotEqualPredicate(TableMeta tableMeta, List<RexNode> conjunctions,
                                                 PlannerContext plannerContext) {
        double selectivity = 1D;
        List<RexNode> toRemovePredicateList = Lists.newLinkedList();
        for (RexNode predicate : conjunctions) {
            if (predicate.isA(SqlKind.NOT_EQUALS) && predicate instanceof RexCall) {
                RexCall filterCall = (RexCall) predicate;
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
                    continue;
                }

                ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef.getIndex());
                if (columnMeta != null && value != null) {
                    StatisticResult countResult =
                        OptimizerContext.getContext(tableMeta.getSchemaName()).getStatisticManager().getFrequency(
                            tableMeta.getTableName(), columnMeta.getName(), value.toString());
                    long count = countResult.getLongValue();
                    if (count >= 0) {
                        selectivity = selectivity * (tableRowCount - count) / tableRowCount;
                    } else if (CBOUtil.isIndexColumn(tableMeta, columnMeta)) {
                        // lack of statistics
                        count = Math.min(LACK_OF_STATISTICS_INDEX_EQUAL_ROW_COUNT, tableRowCount.longValue());
                        selectivity = selectivity * (tableRowCount - count) / tableRowCount;
                    }
                    toRemovePredicateList.add(predicate);
                }
            }
        }
        for (Object obj : toRemovePredicateList) {
            conjunctions.remove(obj);
        }
        return selectivity;
    }

    /**
     * return min Count of in null or not in null Predicate
     * if no statistic, return -1
     */
    private Long columnAppearNullOrNotNullPredicate(TableMeta tableMeta, List<RexNode> conjunctions,
                                                    PlannerContext plannerContext) {
        Long minCount = null;
        List<Object> toRemovePredicateList = new ArrayList<>();
        for (RexNode pred : conjunctions) {
            if ((pred.isA(SqlKind.IS_NULL) || pred.isA(SqlKind.IS_NOT_NULL))
                && pred instanceof RexCall && ((RexCall) pred).getOperands().size() == 1) {
                RexCall isNullRexCall = (RexCall) pred;
                RexNode operand = isNullRexCall.getOperands().get(0);
                if (operand instanceof RexInputRef) {
                    int operandIndex = ((RexInputRef) operand).getIndex();
                    ColumnMeta columnMeta = findColumnMeta(tableMeta, operandIndex);
                    if (columnMeta != null) {
                        long count;
                        StatisticResult statisticResult =
                            OptimizerContext.getContext(tableMeta.getSchemaName()).getStatisticManager().getNullCount(
                                tableMeta.getTableName(), columnMeta.getName());
                        long nullCount = statisticResult.getLongValue();
                        if (pred.isA(SqlKind.IS_NULL)) {
                            count = Math.min(nullCount, (long) tableMeta.getRowCount());
                        } else { // is not null
                            count = Math.max((long) tableMeta.getRowCount() - nullCount, 0);
                        }

                        if (minCount == null) {
                            minCount = count;
                        } else if (count < minCount) {
                            minCount = count;
                        }
                        toRemovePredicateList.add(pred);
                        break;
                    }
                }
            }
        }
        for (Object obj : toRemovePredicateList) {
            conjunctions.remove(obj);
        }
        return minCount;
    }

    /**
     * return in value number if primary key appear in predicate
     * otherwise return 0
     **/
    private long primaryKeyAppearInPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        if (!tableMeta.isHasPrimaryKey()) {
            return 0;
        }
        return indexKeyAppearInPredicate(tableMeta.getPrimaryIndex(), tableMeta, conjunctions);
    }

    private long uniqueKeyAppearInPredicate(TableMeta tableMeta, List<RexNode> conjunctions) {
        long result = 0;
        for (IndexMeta indexMeta : tableMeta.getSecondaryIndexes()) {
            if (indexMeta.isUniqueIndex()) {
                long inNum = indexKeyAppearInPredicate(indexMeta, tableMeta, conjunctions);
                if (result == 0) {
                    result = inNum;
                } else if (inNum != 0 && inNum < result) {
                    result = inNum;
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
}
