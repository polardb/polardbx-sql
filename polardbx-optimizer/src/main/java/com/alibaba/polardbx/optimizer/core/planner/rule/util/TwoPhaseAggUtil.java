package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCheckSumMergeFunction;
import org.apache.calcite.sql.fun.SqlCheckSumV2MergeFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlFinalHyperloglogFunction;
import org.apache.calcite.sql.fun.SqlPartialHyperloglogFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.canSplitDistinct;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.haveAggWithDistinct;

public class TwoPhaseAggUtil {

    public static class TwoPhaseAggComponent {

        private List<RexNode> projectChildExps; //          Upper Project
        private List<AggregateCall> globalAggCalls; //      Global Agg
        private ImmutableBitSet globalAggGroupSet;
        private List<AggregateCall> partialAggCalls;//      Partial Agg
        private ImmutableBitSet partialAggGroupSet;

        public TwoPhaseAggComponent(List<RexNode> projectChildExps,
                                    List<AggregateCall> globalAggCalls, ImmutableBitSet globalAggGroupSet,
                                    List<AggregateCall> partialAggCalls, ImmutableBitSet partialAggGroupSet) {
            this.projectChildExps = projectChildExps;
            this.globalAggCalls = globalAggCalls;
            this.globalAggGroupSet = globalAggGroupSet;
            this.partialAggCalls = partialAggCalls;
            this.partialAggGroupSet = partialAggGroupSet;
        }

        public List<RexNode> getProjectChildExps() {
            return projectChildExps;
        }

        public List<AggregateCall> getGlobalAggCalls() {
            return globalAggCalls;
        }

        public List<AggregateCall> getPartialAggCalls() {
            return partialAggCalls;
        }

        public ImmutableBitSet getGlobalAggGroupSet() {
            return globalAggGroupSet;
        }

        public ImmutableBitSet getPartialAggGroupSet() {
            return partialAggGroupSet;
        }
    }

    public static TwoPhaseAggComponent splitAggWithDistinct(Aggregate agg, LogicalView logicalView,
                                                            Set<Integer> shardIndex) {
        boolean enablePushDownDistinct = PlannerContext.getPlannerContext(
            agg).getParamManager().getBoolean(ConnectionParams.ENABLE_PUSHDOWN_DISTINCT);
        boolean enableSplitDistinct = false;
        if (enablePushDownDistinct && agg.groupSets.size() <= 1 && haveAggWithDistinct(
            agg.getAggCallList())) {
            enableSplitDistinct = canSplitDistinct(shardIndex, agg);
        }

        TwoPhaseAggComponent twoPhaseAggComponent = null;
        if (enableSplitDistinct) {
            twoPhaseAggComponent = TwoPhaseAggUtil.splitAgg(
                agg, logicalView instanceof OSSTableScan, true);
        }

        if (twoPhaseAggComponent == null) {
            if (haveAggWithDistinct(agg.getAggCallList())) {
                return null;
            }

            twoPhaseAggComponent = TwoPhaseAggUtil.splitAgg(
                agg, logicalView instanceof OSSTableScan, false);
        }
        return twoPhaseAggComponent;
    }

    public static TwoPhaseAggComponent splitAgg(Aggregate agg) {
        return splitAgg(agg, true, false);
    }

    public static TwoPhaseAggComponent splitAgg(Aggregate agg, boolean canSplitOrcHash, boolean withDistinct) {
        TddlTypeFactoryImpl tddlTypeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        List<RexNode> childExps = new ArrayList<>();
        final int aggGroupSetCardinality = agg.getGroupSet().cardinality();
        for (int i = 0; i < aggGroupSetCardinality; i++) {
            childExps.add(new RexInputRef(i, agg.getRowType().getFieldList().get(i).getType()));
        }

        List<AggregateCall> globalAggCalls = new ArrayList<>();
        List<AggregateCall> partialAggCalls = new ArrayList<>();

        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall aggCall = agg.getAggCallList().get(i);
            boolean isDistinct = aggCall.isDistinct() && withDistinct;
            try {
                if (isDistinct) {
                    buildTwoAggCallsWithDistinct(
                        aggCall, aggGroupSetCardinality, globalAggCalls, partialAggCalls, childExps);
                } else {
                    buildTwoAggCalls(
                        aggCall, tddlTypeFactory, aggGroupSetCardinality, globalAggCalls, partialAggCalls, childExps,
                        canSplitOrcHash, agg.getCluster().getRexBuilder());
                }
            } catch (UnsupportedOperationException t) {
                // ignore
                return null;
            }
        }
        return new TwoPhaseAggComponent(childExps, globalAggCalls,
            ImmutableBitSet.range(agg.getGroupSet().cardinality()),
            partialAggCalls, agg.getGroupSet());
    }

    private static void buildTwoAggCallsWithDistinct(AggregateCall aggCall,
                                                     int aggGroupSetCardinality,
                                                     List<AggregateCall> globalAggCalls,
                                                     List<AggregateCall> partialAggCalls,
                                                     List<RexNode> childExps) {
        SqlAggFunction function = aggCall.getAggregation();
        switch (function.getKind()) {
        case COUNT:
            SqlSumEmptyIsZeroAggFunction sumAggFunction = new SqlSumEmptyIsZeroAggFunction();

            AggregateCall sumAggregateCall = AggregateCall.create(sumAggFunction,
                false,
                aggCall.isApproximate(),
                ImmutableList.of(aggGroupSetCardinality + partialAggCalls.size()),
                aggCall.filterArg,
                aggCall.getType(),
                aggCall.getName());

            globalAggCalls.add(sumAggregateCall);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

            partialAggCalls.add(aggCall);
            break;
        case SUM:
            AggregateCall newAggCall =
                aggCall.copy(
                    ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()), -1).withDistinct(false);
            globalAggCalls.add(newAggCall);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

            partialAggCalls.add(aggCall);
            break;
        default:
            throw new UnsupportedOperationException();
        }

    }

    private static void buildTwoAggCalls(AggregateCall aggCall, TddlTypeFactoryImpl tddlTypeFactory,
                                         int aggGroupSetCardinality,
                                         List<AggregateCall> globalAggCalls, List<AggregateCall> partialAggCalls,
                                         List<RexNode> childExps,
                                         boolean canSplitOrcHash, RexBuilder rexBuilder) {

        SqlAggFunction function = aggCall.getAggregation();
        switch (function.getKind()) {
        case COUNT:
            SqlSumEmptyIsZeroAggFunction sumAggFunction = new SqlSumEmptyIsZeroAggFunction();

            AggregateCall sumAggregateCall = AggregateCall.create(sumAggFunction,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                ImmutableList.of(aggGroupSetCardinality + partialAggCalls.size()),
                aggCall.filterArg,
                aggCall.getType(),
                aggCall.getName());

            globalAggCalls.add(sumAggregateCall);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

            if (aggCall.getArgList().size() > 1) {
                //make sure the count with one column, not with multi columns
                aggCall = aggCall.copy(ImmutableList.of(aggCall.getArgList().get(0)), aggCall.filterArg);
            }

            partialAggCalls.add(aggCall);
            break;
        case AVG:
            SqlSumAggFunction partialSumAggFunc = new SqlSumAggFunction(null);
            AggregateCall partialSumAggCall = AggregateCall.create(partialSumAggFunc,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                aggCall.getArgList(),
                aggCall.filterArg,
                aggCall.getType(),
                "partial_sum");

            AggregateCall globalSumAggCall =
                partialSumAggCall.copy(ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()),
                    -1,
                    false,
                    "global_sum");

            partialAggCalls.add(partialSumAggCall);

            SqlCountAggFunction pushedCountFunc = new SqlCountAggFunction("COUNT");
            AggregateCall partialCountAggCall = AggregateCall.create(pushedCountFunc,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                aggCall.getArgList(),
                aggCall.filterArg,
                tddlTypeFactory.createSqlType(SqlTypeName.BIGINT),
                "partial_count");

            AggregateCall globalCountAggCall = AggregateCall.create(partialSumAggFunc,
                partialCountAggCall.isDistinct(),
                partialCountAggCall.isApproximate(),
                ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()),
                partialCountAggCall.filterArg,
                partialCountAggCall.getType(),
                "global_count");

            partialAggCalls.add(partialCountAggCall);

            RexInputRef partialSumRef =
                new RexInputRef(globalSumAggCall.getArgList().get(0), partialSumAggCall.getType());
            RexInputRef partialCountRef =
                new RexInputRef(globalCountAggCall.getArgList().get(0), partialCountAggCall.getType());

            RexCall divide = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
                partialSumRef,
                partialCountRef);

            RelDataType relDataType = aggCall.getType();
            if (!divide.getType().getSqlTypeName().equals(relDataType.getSqlTypeName())) {
                RexNode castNode = rexBuilder.makeCastForConvertlet(relDataType, divide);
                childExps.add(castNode);
            } else {
                childExps.add(divide);
            }

            globalAggCalls.add(globalSumAggCall);
            globalAggCalls.add(globalCountAggCall);
            break;
        case MIN:
        case MAX:
        case SUM:
        case BIT_OR:
        case BIT_XOR:
        case __FIRST_VALUE:
            AggregateCall newAggCall =
                aggCall.copy(ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()), -1);
            globalAggCalls.add(newAggCall);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

            partialAggCalls.add(aggCall);
            break;
        case GROUP_CONCAT:
            GroupConcatAggregateCall groupConcatAggregateCall = (GroupConcatAggregateCall) aggCall;
            if (groupConcatAggregateCall.getOrderList() != null
                && groupConcatAggregateCall.getOrderList().size() != 0) {
                throw new UnsupportedOperationException();
            }
            GroupConcatAggregateCall newGroupConcatAggregateCall =
                groupConcatAggregateCall.copy(ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()),
                    -1, groupConcatAggregateCall.getOrderList());
            globalAggCalls.add(newGroupConcatAggregateCall);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

            partialAggCalls.add(aggCall);
            break;
        case HYPER_LOGLOG:
            SqlPartialHyperloglogFunction partialHllFunction = new SqlPartialHyperloglogFunction();
            AggregateCall partialHllAggregateCall = AggregateCall.create(partialHllFunction,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                aggCall.getArgList(),
                aggCall.filterArg,
                tddlTypeFactory.createSqlType(SqlTypeName.VARBINARY),
                "partial_hll");

            SqlFinalHyperloglogFunction finalHllFunction = new SqlFinalHyperloglogFunction();
            AggregateCall finalHllAggregateCall = AggregateCall.create(finalHllFunction,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                ImmutableList.of(aggGroupSetCardinality + partialAggCalls.size()),
                aggCall.filterArg,
                aggCall.getType(),
                "final_hll");

            globalAggCalls.add(finalHllAggregateCall);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(),
                tddlTypeFactory.createSqlType(SqlTypeName.VARBINARY)));

            partialAggCalls.add(partialHllAggregateCall);
            break;
        case CHECK_SUM:
            if (!canSplitOrcHash) {
                throw new UnsupportedOperationException();
            }
            SqlCheckSumMergeFunction crcAggFunction = new SqlCheckSumMergeFunction();

            AggregateCall crcHashAggregateCall = AggregateCall.create(crcAggFunction,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                ImmutableList.of(aggGroupSetCardinality + partialAggCalls.size()),
                aggCall.filterArg,
                aggCall.getType(),
                aggCall.getName());

            globalAggCalls.add(crcHashAggregateCall);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

            partialAggCalls.add(aggCall);
            break;
        case CHECK_SUM_V2:
            if (!canSplitOrcHash) {
                throw new UnsupportedOperationException();
            }
            SqlCheckSumV2MergeFunction func = new SqlCheckSumV2MergeFunction();

            AggregateCall call = AggregateCall.create(func,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                ImmutableList.of(aggGroupSetCardinality + partialAggCalls.size()),
                aggCall.filterArg,
                aggCall.getType(),
                aggCall.getName());

            globalAggCalls.add(call);

            childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

            partialAggCalls.add(aggCall);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }
}
