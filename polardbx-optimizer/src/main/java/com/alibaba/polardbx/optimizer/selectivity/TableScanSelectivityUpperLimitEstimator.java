package com.alibaba.polardbx.optimizer.selectivity;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMdSelectivity;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * estimate the upper bound of Selectivity for any possible parameter
 */
public class TableScanSelectivityUpperLimitEstimator extends RexVisitorImpl<UpperSelectivity> {
    private final double tableRowCount;
    private final TableMeta tableMeta;
    private final PlannerContext plannerContext;
    private final RexBuilder rexBuilder;

    public static int DEFAULT_IN_VALUES = 50;

    private static final Set<SqlKind> FREE_OP = ImmutableSet.<SqlKind>builder()
        .add(SqlKind.LESS_THAN_OR_EQUAL)
        .add(SqlKind.LESS_THAN)
        .add(SqlKind.GREATER_THAN)
        .add(SqlKind.GREATER_THAN_OR_EQUAL)
        .add(SqlKind.NOT_EQUALS)
        .add(SqlKind.NOT_IN)
        .add(SqlKind.BETWEEN)
        .add(SqlKind.NOT_BETWEEN)
        .add(SqlKind.LIKE)
        .add(SqlKind.IS_DISTINCT_FROM)
        .build();

    private static final Set<SqlKind> BOUND_OP = ImmutableSet.<SqlKind>builder()
        .add(SqlKind.EQUALS)
        .add(SqlKind.IN)
        .add(SqlKind.IS_NOT_DISTINCT_FROM)
        .add(SqlKind.IS_NULL)
        .add(SqlKind.IS_NOT_NULL)
        .build();

    private static final Set<SqlKind> EQ_OP = ImmutableSet.<SqlKind>builder()
        .add(SqlKind.EQUALS)
        .add(SqlKind.IS_NOT_DISTINCT_FROM)
        .build();

    private static final Set<SqlKind> COMPOSITE_OP = ImmutableSet.<SqlKind>builder()
        .add(SqlKind.AND)
        .add(SqlKind.OR)
        .add(SqlKind.NOT)
        .build();

    public TableScanSelectivityUpperLimitEstimator(TableScan tableScan) {
        super(true);
        this.tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        this.tableRowCount = tableMeta.getRowCount(null);
        this.plannerContext = PlannerContext.getPlannerContext(tableScan);
        this.rexBuilder = tableScan.getCluster().getRexBuilder();
    }

    public UpperSelectivity evaluate(RexNode predicate) {
        try {
            if (predicate == null) {
                return UpperSelectivity.createBound(1D);
            } else {
                RexNode simplifiedPredicate =
                    new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true, RexUtil.EXECUTOR).simplify(predicate);
                if (simplifiedPredicate.isAlwaysTrue()) {
                    return UpperSelectivity.createBound(1D);
                } else if (simplifiedPredicate.isAlwaysFalse()) {
                    return UpperSelectivity.createBound(0D);
                } else {
                    UpperSelectivity value = simplifiedPredicate.accept(this);
                    if (value == null) {
                        return UpperSelectivity.UNKNOWN;
                    }
                    return value;
                }
            }
        } catch (Throwable e) {
            return UpperSelectivity.UNKNOWN;
        }
    }

    @Override
    public UpperSelectivity visitCall(RexCall call) {
        switch (call.getKind()) {
        case OR:
            return orSelectivity(call);
        case AND:
            return andSelectivity(call);
        case NOT:
            return notSelectivity(call);
        default:
            return defaultSelectivity(call);
        }
    }

    private UpperSelectivity orSelectivity(RexNode predicate) {
        List<RexNode> disjunctions = RelOptUtil.disjunctions(predicate);
        List<UpperSelectivity> upperBound = Lists.newArrayList();
        for (RexNode rexNode : disjunctions) {
            if (rexNode instanceof RexCall) {
                RexCall rexCall = (RexCall) rexNode;
                if (rexCall.getKind().belongsTo(COMPOSITE_OP)) {
                    upperBound.add(evaluate(rexCall));
                } else {
                    upperBound.add(defaultSelectivity(rexCall));
                }
            }
        }
        return UpperSelectivity.or(upperBound);
    }

    private UpperSelectivity andSelectivity(RexNode predicate) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);
        Map<Integer, UpperSelectivity> selectivityMap = Maps.newHashMap();
        List<UpperSelectivity> upperBound = Lists.newArrayList();
        Set<Integer> eqColumns = Sets.newHashSet();
        for (RexNode rexNode : conjunctions) {
            if (rexNode instanceof RexCall) {
                RexCall rexCall = (RexCall) rexNode;
                if (rexCall.getKind().belongsTo(COMPOSITE_OP)) {
                    upperBound.add(evaluate(rexCall));
                } else {
                    UpperSelectivity result = defaultSelectivity(rexCall);
                    if (result.getIdx() < 0) {
                        upperBound.add(result);
                    } else {
                        if (rexCall.getKind().belongsTo(EQ_OP)) {
                            eqColumns.add(result.getIdx());
                        }
                        UpperSelectivity oldResult = selectivityMap.get(result.getIdx());
                        if (oldResult == null || oldResult.getSelectivity() > result.getSelectivity()) {
                            selectivityMap.put(result.getIdx(), result);
                        }
                    }
                }
            }
        }
        // composite pk or uk
        if (pkukEqualPredicate(eqColumns)) {
            return UpperSelectivity.createBound(1 / tableRowCount);
        }

        // add smallest Selectivity for each column
        upperBound.addAll(selectivityMap.values());
        return UpperSelectivity.and(upperBound);
    }

    private UpperSelectivity notSelectivity(RexCall predicate) {
        return UpperSelectivity.not(evaluate(predicate.getOperands().get(0)));
    }

    /**
     * @param predicate the predicate tested
     * @return Selectivity upper bound of predicate
     */
    private UpperSelectivity defaultSelectivity(RexCall predicate) {
        if (predicate == null || tableMeta == null) {
            return UpperSelectivity.UNKNOWN;
        }

        if (predicate.isAlwaysTrue()) {
            return UpperSelectivity.createBound(1D);
        }

        if (predicate.isAlwaysFalse()) {
            return UpperSelectivity.createBound(0D);
        }

        if (FREE_OP.contains(predicate.getKind())) {
            switch (predicate.getKind()) {
            case LESS_THAN_OR_EQUAL:
            case LESS_THAN:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case NOT_EQUALS:
            case IS_DISTINCT_FROM:
                return freeVarOpOneConstOrOneConstOpOp(predicate);
            case NOT_IN:
                return freeVarNotIn(predicate);
            case BETWEEN:
            case NOT_BETWEEN:
                return freeVarOpTwoConst(predicate);
            case LIKE:
                return freeVarOpOneConst(predicate);
            default:
                return UpperSelectivity.UNKNOWN;
            }
        }

        if (BOUND_OP.contains(predicate.getKind())) {
            switch (predicate.getKind()) {
            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
                return boundVarEqOneConstOrOneConstEqOp(predicate);
            case IS_NULL:
                return boundVarNull(predicate);
            case IS_NOT_NULL:
                return boundVarNotNull(predicate);
            case IN:
                return boundVarIn(predicate);
            default:
                return UpperSelectivity.UNKNOWN;
            }
        }

        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a op ?' or '? op a'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity freeVarOpOneConstOrOneConstOpOp(RexCall pred) {
        if (pred.getOperands().size() != 2) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        RexNode rightRexNode = pred.getOperands().get(1);
        int inputRef = -1;
        Object value = null;

        if (leftRexNode instanceof RexInputRef && !(rightRexNode instanceof RexInputRef)) {
            inputRef = ((RexInputRef) leftRexNode).getIndex();
            value = DrdsRexFolder.fold(rightRexNode, plannerContext);
        } else if (rightRexNode instanceof RexInputRef && !(leftRexNode instanceof RexInputRef)) {
            inputRef = ((RexInputRef) rightRexNode).getIndex();
            value = DrdsRexFolder.fold(leftRexNode, plannerContext);
        }
        ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef);
        if (columnMeta != null && value != null) {
            return UpperSelectivity.FREE;
        }
        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a not in (?)'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity freeVarNotIn(RexCall pred) {
        if (pred.getOperands().size() != 2) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        RexNode rightRexNode = pred.getOperands().get(1);
        if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
            .isA(SqlKind.ROW)) {
            int leftIndex = ((RexInputRef) leftRexNode).getIndex();
            ColumnMeta columnMeta = findColumnMeta(tableMeta, leftIndex);
            if (columnMeta != null) {
                for (RexNode rexNode : ((RexCall) rightRexNode).operands) {
                    if (DrdsRexFolder.fold(rexNode, plannerContext) == null) {
                        return UpperSelectivity.UNKNOWN;
                    }
                }
                return UpperSelectivity.FREE;
            }
        }
        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a op ?,?'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity freeVarOpTwoConst(RexCall pred) {
        if (pred.getOperands().size() != 3) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        RexNode midRexNode = pred.getOperands().get(1);
        RexNode rightRexNode = pred.getOperands().get(2);
        if (leftRexNode instanceof RexInputRef
            && !(midRexNode instanceof RexInputRef)
            && !(rightRexNode instanceof RexCall)) {
            int leftIndex = ((RexInputRef) leftRexNode).getIndex();
            ColumnMeta columnMeta = findColumnMeta(tableMeta, leftIndex);
            if (columnMeta != null) {
                if (DrdsRexFolder.fold(midRexNode, plannerContext) != null
                    && DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                    return UpperSelectivity.FREE;
                }
            }
        }
        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a op ?'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity freeVarOpOneConst(RexCall pred) {
        if (pred.getOperands().size() != 2) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        RexNode rightRexNode = pred.getOperands().get(1);
        if (leftRexNode instanceof RexInputRef && !(rightRexNode instanceof RexInputRef)) {
            int inputRef = ((RexInputRef) leftRexNode).getIndex();
            ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef);
            if (columnMeta != null) {
                if (DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                    return UpperSelectivity.FREE;
                }
            }
        }
        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a eq ?' or '? eq a'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity boundVarEqOneConstOrOneConstEqOp(RexCall pred) {
        if (pred.getOperands().size() != 2) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        RexNode rightRexNode = pred.getOperands().get(1);
        int inputRef = -1;
        Object value = null;

        if (leftRexNode instanceof RexInputRef && !(rightRexNode instanceof RexInputRef)) {
            inputRef = ((RexInputRef) leftRexNode).getIndex();
            value = DrdsRexFolder.fold(rightRexNode, plannerContext);
        } else if (rightRexNode instanceof RexInputRef && !(leftRexNode instanceof RexInputRef)) {
            inputRef = ((RexInputRef) rightRexNode).getIndex();
            value = DrdsRexFolder.fold(leftRexNode, plannerContext);
        }
        ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef);
        if (columnMeta != null && value != null) {
            // pk or uk
            if (pkukEqualPredicate(Sets.newHashSet(inputRef))) {
                return UpperSelectivity.createBound(1 / tableRowCount, inputRef);
            }
            StatisticResult statisticResult =
                StatisticManager.getInstance().getFrequencyUpperLimit(tableMeta.getSchemaName(),
                    tableMeta.getTableName(), columnMeta.getName(), false);
            long count = statisticResult.getLongValue();
            if (count >= 0) {
                return UpperSelectivity.createBound(count / tableRowCount, inputRef);
            }
        }
        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a is null'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity boundVarNull(RexCall pred) {
        if (pred.getOperands().size() != 1) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        if (leftRexNode instanceof RexInputRef) {
            int inputRef = ((RexInputRef) leftRexNode).getIndex();
            ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef);
            if (columnMeta != null) {
                StatisticResult statisticResult =
                    StatisticManager.getInstance().getNullCount(tableMeta.getSchemaName(),
                        tableMeta.getTableName(), columnMeta.getName(), false);
                long count = statisticResult.getLongValue();
                if (count >= 0) {
                    return UpperSelectivity.createBound(count / tableRowCount, inputRef);
                }
            }
        }
        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a is not null'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity boundVarNotNull(RexCall pred) {
        if (pred.getOperands().size() != 1) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        if (leftRexNode instanceof RexInputRef) {
            int inputRef = ((RexInputRef) leftRexNode).getIndex();
            ColumnMeta columnMeta = findColumnMeta(tableMeta, inputRef);
            if (columnMeta != null) {
                StatisticResult statisticResult =
                    StatisticManager.getInstance().getNullCount(tableMeta.getSchemaName(),
                        tableMeta.getTableName(), columnMeta.getName(), false);
                long count = statisticResult.getLongValue();
                if (count >= 0) {
                    return UpperSelectivity.createBound(1 - (count / tableRowCount), inputRef);
                }
            }
        }
        return UpperSelectivity.UNKNOWN;
    }

    /**
     * check whether the predicate is 'a in (?)'
     *
     * @param pred the predicate tested
     */
    private UpperSelectivity boundVarIn(RexCall pred) {
        if (pred.getOperands().size() != 2) {
            return UpperSelectivity.UNKNOWN;
        }
        RexNode leftRexNode = pred.getOperands().get(0);
        RexNode rightRexNode = pred.getOperands().get(1);
        if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
            .isA(SqlKind.ROW)) {
            int leftIndex = ((RexInputRef) leftRexNode).getIndex();
            ColumnMeta columnMeta = findColumnMeta(tableMeta, leftIndex);
            if (columnMeta == null) {
                return UpperSelectivity.UNKNOWN;
            }
            // non-constant
            if (DrdsRexFolder.fold(rightRexNode, plannerContext) == null) {
                return UpperSelectivity.UNKNOWN;
            }

            long inSize = -1;
            // to judge between Rawstring and none-Rawstring
            RexCall inValues = ((RexCall) rightRexNode);
            if (inValues.getOperands().size() != 1) {
                // in (?,?), can't be RawString
                // pk or uk
                if (pkukEqualPredicate(Sets.newHashSet(leftIndex))) {
                    inSize = inValues.getOperands().size();
                } else {
                    StatisticResult statisticResult =
                        StatisticManager.getInstance().getFrequencyUpperLimit(tableMeta.getSchemaName(),
                            tableMeta.getTableName(), columnMeta.getName(), plannerContext.isNeedStatisticTrace());
                    long count = statisticResult.getLongValue();
                    if (count >= 0) {
                        inSize = count * inValues.getOperands().size();
                    }
                }
            } else {
                // pk or uk
                if (pkukEqualPredicate(Sets.newHashSet(leftIndex))) {
                    inSize = DEFAULT_IN_VALUES;
                } else {
                    // in (?) or in (RAW(?))
                    StatisticResult statisticResult =
                        StatisticManager.getInstance().getFrequencyUpperLimit(tableMeta.getSchemaName(),
                            tableMeta.getTableName(), columnMeta.getName(), plannerContext.isNeedStatisticTrace());
                    long count = statisticResult.getLongValue();
                    if (count >= 0) {
                        inSize = count;
                        if (inValues.getOperands().get(0) instanceof RexDynamicParam) {
                            int index = ((RexDynamicParam) inValues.getOperands().get(0)).getIndex();
                            // valid index
                            if (index >= 0) {
                                ParameterContext parameterContext =
                                    plannerContext.getParams().getCurrentParameter().get(index + 1);
                                if (parameterContext.getValue() instanceof RawString) {
                                    inSize *= DEFAULT_IN_VALUES;
                                }
                            }
                        }
                    }
                }
            }
            if (inSize >= 0) {
                return UpperSelectivity.createBound(inSize / tableRowCount, leftIndex);
            }

        }

        return UpperSelectivity.UNKNOWN;
    }

    private boolean pkukEqualPredicate(Set<Integer> columnIndexes) {
        // try pk
        if (tableMeta.isHasPrimaryKey()) {
            if (indexAppearEqualPredicate(tableMeta.getPrimaryIndex(), columnIndexes)) {
                return true;
            }
        }

        // try uk
        for (IndexMeta indexMeta : tableMeta.getSecondaryIndexes()) {
            if (indexMeta.isUniqueIndex()) {
                if (indexAppearEqualPredicate(indexMeta, columnIndexes)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param indexMeta index meta data
     * @param columnIndexes columns in predicate
     * @return true if indexMeta in EqualPredicate, otherwise false
     */
    private boolean indexAppearEqualPredicate(IndexMeta indexMeta, Set<Integer> columnIndexes) {
        for (ColumnMeta columnMeta : indexMeta.getKeyColumns()) {
            if (!columnIndexes.contains(DrdsRelMdSelectivity.getColumnIndex(tableMeta, columnMeta))) {
                return false;
            }
        }
        return true;
    }

    private ColumnMeta findColumnMeta(TableMeta tableMeta, int index) {
        if (index < 0 || index > tableMeta.getAllColumns().size()) {
            return null;
        }
        return tableMeta.getAllColumns().get(index);
    }
}
