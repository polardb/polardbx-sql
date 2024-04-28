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

package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import org.apache.calcite.sql.SqlKind;

public abstract class AccumulatorBuilders {

    public static Accumulator create(Aggregator aggregator, DataType aggValueType, DataType[] inputType, int capacity,
                                     ExecutionContext context) {
        Class clazz = aggValueType.getDataClass();
        if (aggregator.getSqlKind() == SqlKind.COUNT) {
            assert clazz == Long.class;
            if (aggregator.getInputColumnIndexes().length == 0) {
                return new CountRowsAccumulator(capacity);
            } else {
                return new CountAccumulator(capacity);
            }
        } else if (aggregator.getSqlKind() == SqlKind.SUM) {
            if (clazz == Decimal.class) {
                return new DecimalSumAccumulator(capacity, aggValueType);
            } else if (clazz == Long.class) {
                return new LongSumAccumulator(capacity);
            } else if (clazz == Double.class) {
                return new DoubleSumAccumulator(capacity);
            }
        } else if (aggregator.getSqlKind() == SqlKind.SUM0) {
            assert clazz == Long.class;
            return new LongSum0Accumulator(capacity);
        } else if (aggregator.getSqlKind() == SqlKind.MIN || aggregator.getSqlKind() == SqlKind.MAX) {
            final boolean isMin = aggregator.getSqlKind() == SqlKind.MIN;
            if (clazz == Decimal.class) {
                return new DecimalMaxMinAccumulator(capacity, isMin);
            } else if (clazz == Double.class) {
                return new DoubleMaxMinAccumulator(capacity, isMin);
            } else if (clazz == Long.class) {
                return new LongMaxMinAccumulator(capacity, isMin);
            }
        } else if (aggregator.getSqlKind() == SqlKind.AVG) {
            if (clazz == Decimal.class) {
                return new DecimalAvgAccumulator(capacity, context);
            } else if (clazz == Double.class) {
                return new DoubleAvgAccumulator(capacity);
            }
        } else if (aggregator.getSqlKind() == SqlKind.BIT_OR) {
            if (clazz == Long.class) {
                return new LongBitOrAccumulator(capacity);
            }
        } else if (aggregator.getSqlKind() == SqlKind.BIT_XOR) {
            if (clazz == Long.class) {
                return new LongBitXorAccumulator(capacity);
            }
        } else if (aggregator.getSqlKind() == SqlKind.__FIRST_VALUE) {
            return new FirstValueAccumulator(aggValueType, context);
        } else if (aggregator.getSqlKind() == SqlKind.HYPER_LOGLOG) {
            return new HyperLogLogAccumulator(aggregator, inputType, capacity);
        } else if (aggregator.getSqlKind() == SqlKind.PARTIAL_HYPER_LOGLOG) {
            return new PartialHyperLogLogAccumulator(aggregator, inputType, capacity);
        } else if (aggregator.getSqlKind() == SqlKind.FINAL_HYPER_LOGLOG) {
            return new FinalHyperLogLogAccumulator(aggregator, inputType, capacity);
        } else if (aggregator.getSqlKind() == SqlKind.CHECK_SUM) {
            return new CheckSumAccumulator(aggregator, inputType, capacity);
        } else if (aggregator.getSqlKind() == SqlKind.CHECK_SUM_MERGE) {
            return new CheckSumMergeAccumulator(capacity);
        } else if (aggregator.getSqlKind() == SqlKind.CHECK_SUM_V2) {
            return new CheckSumV2Accumulator(aggregator, inputType, capacity);
        } else if (aggregator.getSqlKind() == SqlKind.CHECK_SUM_V2_MERGE) {
            return new CheckSumV2MergeAccumulator(capacity);
        }
        return new WrapAggregatorAccumulator(aggregator, inputType, aggValueType, capacity);
    }
}
