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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The range unboundedFollowing window frame calculates frames with the following SQL form:
 * ... RANGE BETWEEN [window frame preceding] AND UNBOUNDED FOLLOWING
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING.
 */
public class RangeUnboundedFollowingOverFrame extends UnboundedFollowingOverFrame {

    private int rightIndex = 0;
    private int leftIndex = 0;
    private int leftBound;
    private int orderByColIndex;
    private int isAscOrder;
    private DataType dataType;
    private Object lastProcessedValue;
    private int currentIndex;
    private int lastProcessedLeft;
    private int lastProcessedRight;
    private int nullRowsLeft;
    private int nullRowsRight;

    public RangeUnboundedFollowingOverFrame(
        List<Aggregator> aggregator,
        int lbound,
        int orderByColIndex,
        boolean isAscOrder,
        DataType dataType) {
        super(aggregator);
        this.leftBound = lbound;
        this.orderByColIndex = orderByColIndex;
        this.dataType = dataType;
        if (isAscOrder) {
            this.isAscOrder = 1;
        } else {
            this.isAscOrder = -1;
        }
    }

    @Override
    public void updateIndex(int leftIndex, int rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex = rightIndex - 1;
        lastProcessedValue = null;
        // 直接把值为null的行计算出来
        if (isAscOrder > 0 && chunksIndex.rowAt(leftIndex).getObject(orderByColIndex) == null) {
            updateNullRows(leftIndex);
        }
        if (isAscOrder < 0 && chunksIndex.rowAt(this.rightIndex).getObject(orderByColIndex) == null) {
            updateNullRows(rightIndex);
        }
        lastProcessedLeft = -1;
        lastProcessedRight = -1;
        currentIndex = leftIndex;
        // 升序与降序的逻辑不一样
        if (isAscOrder < 0) {
            aggregators = aggregators.stream().map(aggregator -> aggregator.getNew()).collect(Collectors.toList());
        }
    }

    private List<Object> process(int leftIndex, int rightIndex) {
        if (lastProcessedLeft == leftIndex && lastProcessedRight == rightIndex) {
            return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
        }
        aggregators = aggregators.stream().map(aggregator -> aggregator.getNew()).collect(Collectors.toList());
        for (int i = leftIndex; i <= rightIndex; i++) {
            final int l = i;
            aggregators.forEach(aggregator -> aggregator.aggregate(chunksIndex.rowAt(l)));
        }
        lastProcessedLeft = leftIndex;
        lastProcessedRight = rightIndex;
        return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
    }

    @Override
    public List<Object> processData(int index) {
        Object currentValue = chunksIndex.rowAt(index).getObject(orderByColIndex);
        int[] indexes;
        if (currentValue == null) {
            indexes = new int[] {nullRowsLeft, rightIndex};
        } else {
            indexes = getBound(index);
        }
        lastProcessedValue = currentValue;
        return process(indexes[0], indexes[1]);
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

    private int[] getBound(int index) {
        Object currentValue = chunksIndex.rowAt(index).getObject(orderByColIndex);
        int other = index;
        while (other <= rightIndex && other >= leftIndex
            && chunksIndex.rowAt(other).getObject(orderByColIndex) != null
        ) {
            //ASC index value >= current value - left bounded => current <= left bounded + index value
            //DESC index value <= current value + leftBounded
            if (isAscOrder > 0 && compare(currentValue, chunksIndex.rowAt(other).getObject(orderByColIndex),
                leftBound)) {
                other -= 1;
            } else if (isAscOrder < 0 && compare(chunksIndex.rowAt(other).getObject(orderByColIndex), currentValue,
                leftBound)) {
                other -= 1;
            } else {
                break;
            }
        }
        if (other != index) {
            other += 1;
        }
        return new int[] {other, rightIndex};
    }

    boolean compare(Object v, Object currentValue, int range) {
        if (DataTypeUtil.equalsSemantically(dataType, DataTypes.DecimalType)) {
            return ((Decimal) v).compareTo(((Decimal) currentValue).add(new Decimal(range, 0))) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.IntegerType)) {
            return ((Integer) v).compareTo(((Integer) currentValue) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.ByteType)) {
            return ((Byte) v).intValue() <= ((Byte) v).intValue() + range;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.ShortType)) {
            return ((Short) v).intValue() <= ((Short) v).intValue() + range;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.LongType)) {
            return ((Long) v).compareTo(((Long) currentValue) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.DoubleType)) {
            return ((Double) v).compareTo(((Double) currentValue) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.FloatType)) {
            return ((Float) v).compareTo(((Float) currentValue) + (range)) <= 0;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.ULongType)) {
            return ((BigInteger) v).compareTo(((BigInteger) currentValue).add(new BigInteger(String.valueOf(range))))
                <= 0;
        }
        return false;
    }
}