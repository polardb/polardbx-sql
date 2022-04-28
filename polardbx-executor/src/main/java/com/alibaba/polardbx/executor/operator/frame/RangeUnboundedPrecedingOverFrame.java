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

package com.alibaba.polardbx.executor.operator.frame;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.executor.calc.Aggregator;

import java.math.BigInteger;
import java.util.List;

/**
 * The range unboundedFollowing window frame calculates frames with the following SQL form:
 * ... RANGE BETWEEN [window frame preceding] AND UNBOUNDED FOLLOWING
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING.
 */
public class RangeUnboundedPrecedingOverFrame extends AbstractOverWindowFrame {

    private final int rightBound;
    private int rightIndex = 0;
    private int leftIndex = 0;
    private int orderByColIndex;
    private int isAscOrder;
    private int nullRowsLeft;
    private int nullRowsRight;
    private DataType dataType;
    private int currentIndex;
    //avoid zero for left = 0 and right = 0
    private int lastProcessedLeft = -1;
    private int lastProcessedRight = -1;
    private Object lastProcessedValue;

    public RangeUnboundedPrecedingOverFrame(
        List<Aggregator> aggregator,
        int rightBound,
        int orderByColIndex,
        boolean isAscOrder,
        DataType dataType) {
        super(aggregator);
        this.rightBound = rightBound;
        this.orderByColIndex = orderByColIndex;
        this.dataType = dataType;
        if (isAscOrder) {
            this.isAscOrder = 1;
        } else {
            this.isAscOrder = -1;
        }
    }

    @Override
    public void updateIndex(int leftIndex, int rightIndex) {//[]
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex = rightIndex - 1;
        lastProcessedValue = null;
        if (isAscOrder > 0 && chunksIndex.rowAt(leftIndex).getObject(orderByColIndex) == null) {
            updateNullRows(leftIndex);
        }
        if (isAscOrder < 0 && chunksIndex.rowAt(this.rightIndex).getObject(orderByColIndex) == null) {
            updateNullRows(rightIndex);
        }
        currentIndex = leftIndex;
        if (isAscOrder > 0) {
            aggregators.forEach(t -> t.resetToInitValue(0));
        }
    }

    @Override
    public void processData(int index) {
        Object currentValue = chunksIndex.rowAt(index).getObject(orderByColIndex);
        int[] indexes = new int[2];
        if (lastProcessedValue != null && lastProcessedValue.equals(currentValue)) {
            return;
        }
        indexes[0] = leftIndex;
        if (currentValue == null) {
            indexes[1] = nullRowsRight;
        } else {
            int otherSize = getBound(index);
            indexes[1] = otherSize;
        }
        lastProcessedValue = currentValue;
        process(indexes[0], indexes[1]);
    }

    private void process(int leftIndex, int rightIndex) {
        // 升序时，不停的添加结果
        if (isAscOrder > 0) {
            while (currentIndex <= rightIndex) {
                Chunk.ChunkRow row = chunksIndex.rowAt(currentIndex++);
                aggregators.forEach(t -> t.accumulate(0, row.getChunk(), row.getPosition()));
            }
            return;
        }
        if (lastProcessedLeft == leftIndex && lastProcessedRight == rightIndex) {
            return;
        }
        // 降序时，根据范围重新进行计算，如果实现了部分函数的retract方法，则可减少该部分函数的计算量
        aggregators.forEach(t -> {
            t.resetToInitValue(0);
            for (int i = leftIndex; i <= rightIndex; i++) {
                Chunk.ChunkRow row = chunksIndex.rowAt(i);
                t.accumulate(0, row.getChunk(), row.getPosition());
            }
        });
        lastProcessedRight = rightIndex;
        lastProcessedLeft = leftIndex;
    }

    private void updateNullRows(int index) {
        int nullRowsAnotherSide = index;
        while (nullRowsAnotherSide >= leftIndex && nullRowsAnotherSide <= rightIndex
            && chunksIndex.rowAt(nullRowsAnotherSide).getObject(orderByColIndex) == null) {
            nullRowsAnotherSide += isAscOrder;
        }
        nullRowsAnotherSide -= isAscOrder;
        if (isAscOrder > 0) {
            nullRowsLeft = leftIndex;
            nullRowsRight = nullRowsAnotherSide;
        } else {
            nullRowsLeft = nullRowsAnotherSide;
            nullRowsRight = rightIndex;
        }
    }

    private int getBound(int index) {
        Object currentValue = chunksIndex.rowAt(index).getObject(orderByColIndex);
        int other = index;
        while (other <= rightIndex && other >= leftIndex
            && chunksIndex.rowAt(other).getObject(orderByColIndex) != null) {
            //asc need <= , desc need the next value <= current value - right bound 
            boolean compare = compare(chunksIndex.rowAt(other).getObject(orderByColIndex), currentValue, rightBound);
            //current + bound >= next value
            if (isAscOrder > 0 && compare(chunksIndex.rowAt(other).getObject(orderByColIndex), currentValue,
                rightBound)) {
                other += 1;
            } else if (isAscOrder < 0 && compare(currentValue, chunksIndex.rowAt(other).getObject(orderByColIndex),
                rightBound)) {
                //current - right bounded <= next value[get(Other)] => current <= next value + right bounded
                other += 1;
            } else {
                break;
            }

        }
        if (other != index) {
            other -= 1;
        }
        return other;
    }

    boolean compare(Object v1, Object v2, int range) {
        if (DataTypeUtil.equalsSemantically(dataType, DataTypes.DecimalType)) {
            return ((Decimal) v1).compareTo(((Decimal) v2).add(new Decimal(range, 0))) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.IntegerType)) {
            return ((Integer) v1).compareTo(((Integer) v2) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.ByteType)) {
            return ((Byte) v1).intValue() <= ((Byte) v1).intValue() + range;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.ShortType)) {
            return ((Short) v1).intValue() <= ((Short) v1).intValue() + range;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.LongType)) {
            return ((Long) v1).compareTo(((Long) v2) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.DoubleType)) {
            return ((Double) v1).compareTo(((Double) v2) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.FloatType)) {
            return ((Float) v1).compareTo(((Float) v2) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.ULongType)) {
            return ((BigInteger) v1).compareTo(((BigInteger) v2).add(new BigInteger(String.valueOf(range))))
                <= 0;
        }
        return false;
    }
}
