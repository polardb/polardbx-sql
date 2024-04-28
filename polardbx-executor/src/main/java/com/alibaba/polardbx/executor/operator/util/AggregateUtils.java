package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumMerge;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumV2Merge;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CumeDist;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.DenseRank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FinalHyperLoglog;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FirstValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.GroupConcat;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.HyperLoglog;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.InternalFirstValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Lag;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.LastValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Lead;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.NThValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.NTile;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.PartialHyperLoglog;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.PercentRank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Rank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.RowNumber;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SingleValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum0;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.core.WindowAggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract AggHandler
 *
 * @author <a href="mailto:eric.fy@alibaba-inc.com">Eric Fu</a>
 */
public abstract class AggregateUtils {

    public static final int MAX_HASH_TABLE_SIZE = 131064;
    public static final int MIN_HASH_TABLE_SIZE = 1024;

    public static List<Aggregator> convertAggregators(List<AggregateCall> aggCallList,
                                                      ExecutionContext executionContext,
                                                      MemoryAllocatorCtx memoryAllocator) {
        List<Aggregator> aggList = new ArrayList<>(aggCallList.size());
        for (int i = 0, n = aggCallList.size(); i < n; i++) {
            AggregateCall call = aggCallList.get(i);
            boolean isDistinct = call.isDistinct();
            // int precision = call.getType().getPrecision();
            // int scale = call.getType().getScale();
            // check is call.isDistinct()
            SqlKind function = call.getAggregation().getKind();
            int filterArg = call.filterArg;

            Integer index = -1;
            if (call.getArgList() != null && call.getArgList().size() > 0) {
                index = call.getArgList().get(0);
            }
            switch (function) {
            case AVG: {
                aggList.add(new AvgV2(index, isDistinct, memoryAllocator, filterArg));
                break;
            }
            case SUM: {
                aggList.add(new SumV2(index, isDistinct, memoryAllocator, filterArg));
                break;
            }
            case SUM0: {
                aggList.add(new Sum0(index, isDistinct, memoryAllocator, filterArg));
                break;
            }
            case COUNT: {
                int[] countIdx = call.getArgList().isEmpty() ? ArrayUtils.EMPTY_INT_ARRAY : Arrays
                    .stream(call.getArgList().toArray(new Integer[0]))
                    .mapToInt(Integer::valueOf)
                    .toArray();
                aggList.add(new CountV2(countIdx, isDistinct, memoryAllocator, filterArg));
                break;
            }
            case SINGLE_VALUE: {
                aggList.add(new SingleValue(index, filterArg));
                break;
            }
            case MAX: {
                aggList.add(new MaxV2(index, filterArg));
                break;
            }
            case MIN: {
                aggList.add(new MinV2(index, filterArg));
                break;
            }
            case BIT_OR: {
                aggList.add(new BitOr(index, filterArg));
                break;
            }
            case BIT_AND: {
                aggList.add(new BitAnd(index, filterArg));
                break;
            }
            case BIT_XOR: {
                aggList.add(new BitXor(index, filterArg));
                break;
            }
            case GROUP_CONCAT: {
                assert call instanceof GroupConcatAggregateCall;
                GroupConcatAggregateCall groupConcatAggregateCall = (GroupConcatAggregateCall) call;
                int groupConcatMaxLen = executionContext.getParamManager().getInt(
                    ConnectionParams.GROUP_CONCAT_MAX_LEN);
                List<String> ascOrDescList = groupConcatAggregateCall.getAscOrDescList();
                List<Boolean> isAscList = new ArrayList<>(ascOrDescList.size());
                for (int j = 0; j < ascOrDescList.size(); j++) {
                    isAscList.add("ASC".equals(ascOrDescList.get(j)));
                }
                aggList.add(new GroupConcat(GroupConcat.toIntArray(groupConcatAggregateCall.getArgList()),
                    isDistinct,
                    groupConcatAggregateCall.getSeparator(),
                    groupConcatAggregateCall.getOrderList(),
                    isAscList,
                    groupConcatMaxLen,
                    executionContext.getEncoding(),
                    memoryAllocator,
                    filterArg));
                break;
            }
            case __FIRST_VALUE: {
                aggList.add(new InternalFirstValue(index, filterArg));
                break;
            }
            case ROW_NUMBER: {
                aggList.add(new RowNumber());
                break;
            }
            case RANK: {
                aggList.add(new Rank(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case DENSE_RANK: {
                aggList.add(new DenseRank(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case PERCENT_RANK: {
                aggList
                    .add(new PercentRank(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case HYPER_LOGLOG: {
                aggList.add(
                    new HyperLoglog(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case PARTIAL_HYPER_LOGLOG: {
                aggList.add(
                    new PartialHyperLoglog(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case FINAL_HYPER_LOGLOG: {
                aggList.add(
                    new FinalHyperLoglog(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case CHECK_SUM: {
                aggList.add(new CheckSum(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case CHECK_SUM_MERGE: {
                aggList.add(
                    new CheckSumMerge(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case CHECK_SUM_V2: {
                aggList.add(new CheckSumV2(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case CHECK_SUM_V2_MERGE: {
                aggList.add(
                    new CheckSumV2Merge(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case CUME_DIST: {
                aggList.add(new CumeDist(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg));
                break;
            }
            case FIRST_VALUE: {
                aggList.add(new FirstValue(index, filterArg));
                break;
            }
            case LAST_VALUE: {
                aggList.add(new LastValue(index, filterArg));
                break;
            }
            case NTH_VALUE: {
                WindowAggregateCall windowAggregateCall = (WindowAggregateCall) call;
                aggList.add(new NThValue(windowAggregateCall.getArgList().get(0),
                    windowAggregateCall.getNTHValueOffset(call.getArgList().get(1)), filterArg));
                break;
            }
            case NTILE: {
                WindowAggregateCall windowAggregateCall = (WindowAggregateCall) call;
                aggList.add(new NTile(windowAggregateCall.getNTileOffset(index), filterArg));
                break;
            }
            case LAG:
            case LEAD: {
                WindowAggregateCall windowAggregateCall = (WindowAggregateCall) call;
                int lagLeadOffset = 1;
                if (call.getArgList().size() > 1) {
                    lagLeadOffset = windowAggregateCall.getLagLeadOffset(call.getArgList().get(1));
                }
                String defaultValue = null;
                if (call.getArgList().size() > 2) {
                    defaultValue = windowAggregateCall.getLagLeadDefaultValue(call.getArgList().get(2));
                }
                if (function == SqlKind.LAG) {
                    aggList.add(new Lag(index, lagLeadOffset,
                        defaultValue, filterArg));
                } else {
                    aggList.add(new Lead(index, lagLeadOffset,
                        defaultValue, filterArg));
                }
                break;
            }
            default:
                throw new UnsupportedOperationException(
                    "Unsupported agg function to convert:" + function.name());
            }
        }
        return aggList;
    }

    public static int[] convertBitSet(ImmutableBitSet gp) {
        List<Integer> list = gp.asList();
        int[] groups = new int[list.size()];
        for (int i = 0, n = list.size(); i < n; i++) {
            groups[i] = list.get(i);
        }
        return groups;
    }

    public static DataType[] collectDataTypes(List<DataType> columns, int[] indexes) {
        DataType[] result = new DataType[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            result[i] = columns.get(indexes[i]);
        }
        return result;
    }

    public static DataType[] collectDataTypes(List<DataType> columns) {
        return collectDataTypes(columns, 0, columns.size());
    }

    public static DataType[] collectDataTypes(List<DataType> columns, int start, int end) {
        DataType[] result = new DataType[end - start];
        for (int i = start; i < end; i++) {
            result[i - start] = columns.get(i);
        }
        return result;
    }

    public static int estimateHashTableSize(int expectedOutputRowCount, ExecutionContext context) {

        int maxHashTableSize;
        int hashTableFactor = context.getParamManager().getInt(ConnectionParams.AGG_MAX_HASH_TABLE_FACTOR);
        if (hashTableFactor > 1) {
            maxHashTableSize = MAX_HASH_TABLE_SIZE * hashTableFactor;
        } else {
            maxHashTableSize = MAX_HASH_TABLE_SIZE * MemoryManager.getInstance().getAggHashTableFactor();
        }
        int minHashTableSize =
            MIN_HASH_TABLE_SIZE / context.getParamManager().getInt(ConnectionParams.AGG_MIN_HASH_TABLE_FACTOR);
        if (expectedOutputRowCount > maxHashTableSize) {
            expectedOutputRowCount = maxHashTableSize;
        } else if (expectedOutputRowCount < minHashTableSize) {
            expectedOutputRowCount = minHashTableSize;
        }
        return expectedOutputRowCount;
    }
}
