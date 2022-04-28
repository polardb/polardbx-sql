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

package com.alibaba.polardbx.executor.operator.util.minmaxfilter;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.bloomfilter.MinMaxFilterInfo;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.slice.Slice;

import java.sql.Date;
import java.sql.Timestamp;


/**
 * @author chenzilin
 * @date 2021/12/8 14:33
 */
public class MinMaxFilter {
    public static MinMaxFilter create(DataType type) {
        Class clazz = type.getDataClass();
        if (clazz == Integer.class) {
            return new IntegerMinMaxFilter();
        } else if (clazz == Long.class) {
            return new LongMinMaxFilter();
        } else if (clazz == UInt64.class) {
            return new LongMinMaxFilter();
        } else if (clazz == Slice.class) {
            return new StringMinMaxFilter();
        } else if (clazz == Timestamp.class) {
            return new TimestampMinMaxFilter();
        } else if (clazz == Date.class) {
            return new DateMinMaxFilter();
        } else if (clazz == Decimal.class) {
            return new DecimalMinMaxFilter();
        } else if (clazz == Double.class) {
            return new DoubleMinMaxFilter();
        } else if (clazz == Float.class) {
            return new FloatMinMaxFilter();
        } else {
            return new BlackHoleMinMaxFilter();
        }
    }

    public static MinMaxFilter from(MinMaxFilterInfo minMaxFilterInfo) {
        switch (minMaxFilterInfo.getType()) {
            case NULL:
                return new BlackHoleMinMaxFilter();
            case INTEGER:
                return new IntegerMinMaxFilter(
                        minMaxFilterInfo.getMinNumber() == null ? null : minMaxFilterInfo.getMinNumber().intValue(),
                        minMaxFilterInfo.getMaxNumber() == null ? null : minMaxFilterInfo.getMaxNumber().intValue());
            case LONG:
                return new LongMinMaxFilter(
                        minMaxFilterInfo.getMinNumber() == null ? null : minMaxFilterInfo.getMinNumber().longValue(),
                        minMaxFilterInfo.getMaxNumber() == null ? null : minMaxFilterInfo.getMaxNumber().longValue());
            case STRING:
                return new StringMinMaxFilter(
                        minMaxFilterInfo.getMinString() == null ? null : minMaxFilterInfo.getMinString(),
                        minMaxFilterInfo.getMaxString() == null ? null : minMaxFilterInfo.getMaxString());
            case TIMESTAMP:
                return new TimestampMinMaxFilter(
                        minMaxFilterInfo.getMinNumber() == null ? null : minMaxFilterInfo.getMinNumber().longValue(),
                        minMaxFilterInfo.getMaxNumber() == null ? null : minMaxFilterInfo.getMaxNumber().longValue());
            case DATE:
                return new DateMinMaxFilter(
                        minMaxFilterInfo.getMinNumber() == null ? null : minMaxFilterInfo.getMinNumber().longValue(),
                        minMaxFilterInfo.getMaxNumber() == null ? null : minMaxFilterInfo.getMaxNumber().longValue());
            case DECIMAL:
                return new DecimalMinMaxFilter(
                        minMaxFilterInfo.getMinString() == null ? null : Decimal.fromString(minMaxFilterInfo.getMinString()),
                        minMaxFilterInfo.getMaxString() == null ? null : Decimal.fromString(minMaxFilterInfo.getMaxString()));
            case DOUBLE:
                return new DoubleMinMaxFilter(
                        minMaxFilterInfo.getMinDouble() == null ? null : minMaxFilterInfo.getMinDouble(),
                        minMaxFilterInfo.getMaxDouble() == null ? null : minMaxFilterInfo.getMaxDouble());
            case FLOAT:
                return new FloatMinMaxFilter(
                        minMaxFilterInfo.getMinFloat() == null ? null : minMaxFilterInfo.getMinFloat(),
                        minMaxFilterInfo.getMaxFloat() == null ? null : minMaxFilterInfo.getMaxFloat());
            default:
                throw new UnsupportedOperationException();
        }
    }

    public void put(Block block, int pos) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public MinMaxFilterInfo toMinMaxFilterInfo() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public Number getMinNumber() {
        return null;
    }

    public Number getMaxNumber() {
        return null;
    }

    public String getMinString() {
        return null;
    }

    public String getMaxString() {
        return null;
    }
}
