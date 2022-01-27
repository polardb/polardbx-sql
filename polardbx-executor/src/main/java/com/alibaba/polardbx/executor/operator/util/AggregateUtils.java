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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Avg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Byte2ByteMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Byte2ByteMin;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Byte2DecimalAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Byte2DecimalSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Byte2UInt64BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Byte2UInt64BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Byte2UInt64BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Count;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountRow;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CumeDist;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Decimal2DecimalAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Decimal2DecimalMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Decimal2DecimalMin;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Decimal2DecimalSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Decimal2UInt64BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Decimal2UInt64BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Decimal2UInt64BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.DenseRank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Double2DoubleAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Double2DoubleMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Double2DoubleMin;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Double2DoubleSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FirstValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Float2DoubleAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Float2DoubleSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Float2FloatMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Float2FloatMin;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.GroupConcat;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Int2DecimalAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Int2DecimalSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Int2IntMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Int2IntMin;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Int2UInt64BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Int2UInt64BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Int2UInt64BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.InternalFirstValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Lag;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.LastValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Lead;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2DecimalAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2DecimalSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2LongMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2LongMin;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2LongSum0;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2UInt64BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2UInt64BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2UInt64BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Max;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Min;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.NThValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.NTile;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.OtherType2UInt64BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.OtherType2UInt64BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.OtherType2UInt64BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.PercentRank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Rank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.RowNumber;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Short2DecimalAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Short2DecimalSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Short2ShortMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Short2ShortMin;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Short2UInt64BitAnd;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Short2UInt64BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Short2UInt64BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SingleValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SpecificType2DecimalAvg;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SpecificType2DoubleAvgV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum0;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.WrapedLong2WarpedLongMax;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.WrapedLong2WarpedLongMin;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.core.WindowAggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract AggHandler
 *
 */
public abstract class AggregateUtils {

    public static List<Aggregator> convertAggregators(List<DataType> inputTypes,
                                                      List<DataType> aggOutputTypes,
                                                      List<AggregateCall> aggCallList,
                                                      ExecutionContext executionContext,
                                                      MemoryAllocatorCtx memoryAllocator) {
        List<Aggregator> aggList = new ArrayList<>(aggCallList.size());
        for (int i = 0, n = aggCallList.size(); i < n; i++) {
            AggregateCall call = aggCallList.get(i);
            SqlKind function = call.getAggregation().getKind();
            if (SqlKind.WINDOW_FUNCTION.contains(function)) {
                aggList.add(convertWindowFunction(call));
                continue;
            }
            boolean isDistinct = call.isDistinct();
            int filterArg = call.filterArg;

            Integer index = -1;
            if (call.getArgList() != null && call.getArgList().size() > 0) {
                index = call.getArgList().get(0);
            }

            DataType inputType = index >= 0 ? inputTypes.get(index) : null;
            Class inputTypeClass = inputType == null ? null : inputType.getDataClass();
            DataType outputType = aggOutputTypes.get(i);
            Class outputTypeClass = outputType == null ? null : outputType.getDataClass();

            switch (function) {
            case AVG: {
                if (outputTypeClass == Decimal.class) {
                    if (inputTypeClass == Decimal.class) {
                        aggList.add(new Decimal2DecimalAvg(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Byte.class) {
                        aggList.add(new Byte2DecimalAvg(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Short.class) {
                        aggList.add(new Short2DecimalAvg(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Integer.class) {
                        aggList.add(new Int2DecimalAvg(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Long.class) {
                        aggList.add(new Long2DecimalAvg(index, isDistinct, inputType, outputType, filterArg));
                    } else {
                        aggList.add(new Avg(index, isDistinct, outputType, filterArg));
                    }
                } else if (outputTypeClass == Double.class) {
                    if (inputTypeClass == Double.class) {
                        aggList.add(new Double2DoubleAvg(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Float.class) {
                        aggList.add(new Float2DoubleAvg(index, isDistinct, inputType, outputType, filterArg));
                    } else {
                        aggList.add(new Avg(index, isDistinct, outputType, filterArg));
                    }
                } else {
                    aggList.add(new Avg(index, isDistinct, outputType, filterArg));
                }
                break;
            }
            case SUM: {
                if (outputTypeClass == Decimal.class) {
                    if (inputTypeClass == Decimal.class) {
                        aggList.add(new Decimal2DecimalSum(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Byte.class) {
                        aggList.add(new Byte2DecimalSum(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Short.class) {
                        aggList.add(new Short2DecimalSum(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Integer.class) {
                        aggList.add(new Int2DecimalSum(index, isDistinct, inputType, outputType, filterArg));
                    } else if (inputTypeClass == Long.class) {
                        aggList.add(new Long2DecimalSum(index, isDistinct, inputType, outputType, filterArg));
                    } else {
                        aggList.add(new Sum(index, isDistinct, outputType, filterArg));
                    }
                } else if (outputTypeClass == Double.class && inputTypeClass == Double.class) {
                    aggList.add(new Double2DoubleSum(index, isDistinct, inputType, outputType, filterArg));
                } else if (outputTypeClass == Double.class && inputTypeClass == Float.class) {
                    aggList.add(new Float2DoubleSum(index, isDistinct, inputType, outputType, filterArg));
                } else {
                    aggList.add(new Sum(index, isDistinct, outputType, filterArg));
                }
                break;
            }
            case SUM0: {
                if (inputTypeClass == Long.class && outputTypeClass == Long.class) {
                    aggList.add(new Long2LongSum0(index, isDistinct, inputType, outputType, filterArg));
                } else {
                    aggList.add(new Sum0(index, isDistinct, outputType, filterArg));
                }
                break;
            }
            case COUNT: {
                int[] countIdx = call.getArgList().isEmpty() ? ArrayUtils.EMPTY_INT_ARRAY : Arrays
                    .stream(call.getArgList().toArray(new Integer[0])).mapToInt(Integer::valueOf).toArray();
                if (countIdx.length == 0 || (countIdx[0] >= inputTypes.size())) {
                    aggList.add(new CountRow(new int[0], isDistinct, filterArg));
                } else {
                    aggList.add(new Count(countIdx, isDistinct, filterArg));
                }
                break;
            }
            case SINGLE_VALUE: {
                aggList.add(new SingleValue(index, filterArg));
                break;
            }
            case MAX: {
                if (inputTypeClass == Byte.class && outputTypeClass == Byte.class) {
                    aggList.add(new Byte2ByteMax(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Short.class && outputTypeClass == Short.class) {
                    aggList.add(new Short2ShortMax(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Integer.class && outputTypeClass == Integer.class) {
                    aggList.add(new Int2IntMax(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Long.class && outputTypeClass == Long.class) {
                    aggList.add(new Long2LongMax(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Decimal.class && outputTypeClass == Decimal.class) {
                    aggList.add(new Decimal2DecimalMax(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Float.class && outputTypeClass == Float.class) {
                    aggList.add(new Float2FloatMax(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Double.class && outputTypeClass == Double.class) {
                    aggList.add(new Double2DoubleMax(index, inputType, outputType, filterArg));
                } else if ((inputTypeClass == outputTypeClass) && (inputTypeClass == Date.class
                    || inputTypeClass == Time.class || inputTypeClass == Timestamp.class)) {
                    aggList.add(new WrapedLong2WarpedLongMax(index, inputType, outputType, filterArg));
                } else {
                    aggList.add(new Max(index, inputType, outputType, filterArg));
                }
                break;
            }
            case MIN: {
                if (inputTypeClass == Byte.class && outputTypeClass == Byte.class) {
                    aggList.add(new Byte2ByteMin(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Short.class && outputTypeClass == Short.class) {
                    aggList.add(new Short2ShortMin(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Integer.class && outputTypeClass == Integer.class) {
                    aggList.add(new Int2IntMin(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Long.class && outputTypeClass == Long.class) {
                    aggList.add(new Long2LongMin(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Decimal.class && outputTypeClass == Decimal.class) {
                    aggList.add(new Decimal2DecimalMin(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Float.class && outputTypeClass == Float.class) {
                    aggList.add(new Float2FloatMin(index, inputType, outputType, filterArg));
                } else if (inputTypeClass == Double.class && outputTypeClass == Double.class) {
                    aggList.add(new Double2DoubleMin(index, inputType, outputType, filterArg));
                } else if ((inputTypeClass == outputTypeClass) && (inputTypeClass == Date.class
                    || inputTypeClass == Time.class || inputTypeClass == Timestamp.class)) {
                    aggList.add(new WrapedLong2WarpedLongMin(index, inputType, outputType, filterArg));
                } else {
                    aggList.add(new Min(index, inputType, outputType, filterArg));
                }
                break;
            }
            case BIT_OR: {
                if (outputType == DataTypes.ULongType) {
                    if (inputTypeClass == Byte.class) {
                        aggList.add(new Byte2UInt64BitOr(index, inputType, filterArg));
                    } else if (inputTypeClass == Short.class) {
                        aggList.add(new Short2UInt64BitOr(index, inputType, filterArg));
                    } else if (inputTypeClass == Integer.class) {
                        aggList.add(new Int2UInt64BitOr(index, inputType, filterArg));
                    } else if (inputTypeClass == Long.class) {
                        aggList.add(new Long2UInt64BitOr(index, inputType, filterArg));
                    } else if (inputTypeClass == Decimal.class) {
                        aggList.add(new Decimal2UInt64BitOr(index, inputType, filterArg));
                    } else {
                        aggList.add(new OtherType2UInt64BitOr(index, inputType, filterArg));
                    }
                } else {
                    aggList.add(new BitOr(index, inputType, outputType, filterArg));
                }
                break;
            }
            case BIT_XOR: {
                if (outputType == DataTypes.ULongType) {
                    if (inputTypeClass == Byte.class) {
                        aggList.add(new Byte2UInt64BitXor(index, inputType, filterArg));
                    } else if (inputTypeClass == Short.class) {
                        aggList.add(new Short2UInt64BitXor(index, inputType, filterArg));
                    } else if (inputTypeClass == Integer.class) {
                        aggList.add(new Int2UInt64BitXor(index, inputType, filterArg));
                    } else if (inputTypeClass == Long.class) {
                        aggList.add(new Long2UInt64BitXor(index, inputType, filterArg));
                    } else if (inputTypeClass == Decimal.class) {
                        aggList.add(new Decimal2UInt64BitXor(index, inputType, filterArg));
                    } else {
                        aggList.add(new OtherType2UInt64BitXor(index, inputType, filterArg));
                    }
                } else {
                    aggList.add(new BitXor(index, inputType, outputType, filterArg));
                }
                break;
            }
            case BIT_AND: {
                if (outputType == DataTypes.ULongType) {
                    if (inputTypeClass == Byte.class) {
                        aggList.add(new Byte2UInt64BitAnd(index, inputType, filterArg));
                    } else if (inputTypeClass == Short.class) {
                        aggList.add(new Short2UInt64BitAnd(index, inputType, filterArg));
                    } else if (inputTypeClass == Integer.class) {
                        aggList.add(new Int2UInt64BitAnd(index, inputType, filterArg));
                    } else if (inputTypeClass == Long.class) {
                        aggList.add(new Long2UInt64BitAnd(index, inputType, filterArg));
                    } else if (inputTypeClass == Decimal.class) {
                        aggList.add(new Decimal2UInt64BitAnd(index, inputType, filterArg));
                    } else {
                        aggList.add(new OtherType2UInt64BitAnd(index, inputType, filterArg));
                    }
                } else {
                    aggList.add(new BitAnd(index, inputType, outputType, filterArg));
                }
                break;
            }
            case GROUP_CONCAT: {
                assert call instanceof GroupConcatAggregateCall;
                GroupConcatAggregateCall groupConcatAggregateCall = (GroupConcatAggregateCall) call;
                int groupConcatMaxLen = 1024;
                if (executionContext.getServerVariables() != null) {
                    Object v = executionContext.getServerVariables()
                        .get(ConnectionProperties.GROUP_CONCAT_MAX_LEN.toLowerCase());
                    if (v != null) {
                        groupConcatMaxLen = (int) v;
                    }
                }

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
                    filterArg,
                    outputType));
                break;
            }
            case __FIRST_VALUE: {
                aggList.add(new InternalFirstValue(index, outputType, filterArg, executionContext));
                break;
            }
            default:
                throw new UnsupportedOperationException(
                    "Unsupported agg function to convert:" + function.name());

            }
        }
        return aggList;
    }

    public static Aggregator convertWindowFunction(AggregateCall call) {
        SqlKind function = call.getAggregation().getKind();
        int filterArg = call.filterArg;
        switch (function) {
        case ROW_NUMBER: {
            return new RowNumber(filterArg);
        }
        case RANK: {
            return new Rank(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg);
        }
        case DENSE_RANK: {
            return new DenseRank(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg);
        }
        case PERCENT_RANK: {
            return new PercentRank(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg);
        }
        case CUME_DIST: {
            return new CumeDist(call.getArgList().stream().mapToInt(Integer::valueOf).toArray(), filterArg);
        }
        case FIRST_VALUE: {
            return new FirstValue(call.getArgList().get(0), filterArg);
        }
        case LAST_VALUE: {
            return new LastValue(call.getArgList().get(0), filterArg);
        }
        case NTH_VALUE: {
            WindowAggregateCall windowAggregateCall = (WindowAggregateCall) call;
            return new NThValue(windowAggregateCall.getArgList().get(0),
                windowAggregateCall.getNTHValueOffset(call.getArgList().get(1)), filterArg);
        }
        case NTILE: {
            WindowAggregateCall windowAggregateCall = (WindowAggregateCall) call;
            return new NTile(windowAggregateCall.getNTileOffset(call.getArgList().get(0)), filterArg);
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
                return new Lag(call.getArgList().get(0), lagLeadOffset, defaultValue, filterArg);
            } else {
                return new Lead(call.getArgList().get(0), lagLeadOffset, defaultValue, filterArg);
            }
        }
        default:
            throw new UnsupportedOperationException(
                "Unsupported window function to convert:" + function.name());
        }
    }

    public static boolean supportSpill(Aggregator aggregator) {
        return !(aggregator instanceof Avg) && !(aggregator instanceof SpecificType2DecimalAvg)
            && !(aggregator instanceof SpecificType2DoubleAvgV2) && !(aggregator instanceof GroupConcat);
    }
}
