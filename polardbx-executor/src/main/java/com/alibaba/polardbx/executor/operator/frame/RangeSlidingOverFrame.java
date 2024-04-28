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
 * The range sliding window frame calculates frames with the following SQL form:
 * ... RANGE BETWEEN [window frame preceding] AND [window frame following]
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 * [window frame following] ::= [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING.
 */

// 目前仅支持数值类型的window frame，不支持日期类型的range frame
// 比较函数用表达式替换之后，逻辑都是一样的
public class RangeSlidingOverFrame extends SlidingOverFrame {
    private final int leftBound;
    private final int rightBound;
    private int rightIndex = 0;
    private int leftIndex = 0;
    private Object lastProcessedValue;

    // 给出order by的column，range frame的window必须又order by列，并且order by的列只能有一个
    // 因为需要以该值为基准，进行上下滑动
    private int orderByColIndex;

    // 是否为升序排列
    // 是=1，否=-1，便于计算行的索引，否则需要分情况讨论
    private int isAscOrder;

    // 如果当前值为null，sliding window的大小为所有的null行，避免重复计算
    private boolean isNullVisit;

    // 值为null的行的边界index
//    private int nullRowsLeft;
//    private int nullRowsRight;

    // order by列的类型，TODO 将比较函数替换为IExpression后就不需要这个参数了
    private DataType dataType;

    public RangeSlidingOverFrame(
        List<Aggregator> aggregator,
        int leftBound,
        int rightBound,
        int orderByColIndex,
        boolean isAscOrder,
        DataType dataType) {
        super(aggregator);
        this.leftBound = leftBound;
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
    public void updateIndex(int leftIndex, int rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex - 1;
        this.isNullVisit = false;
        prevLeftIndex = -1;
        prevRightIndex = -1;
        lastProcessedValue = null;
    }

    @Override
    public List<Object> processData(int index) {
        Object currentValue = chunksIndex.rowAt(index).getObject(orderByColIndex);
        if (currentValue == null) {
            if (!isNullVisit) {
                return process(index, getNullRowsRight(index));
            }
            return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
        }
        if (lastProcessedValue != null && lastProcessedValue.equals(currentValue)) {
            return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
        } else {
            int[] indexes = getBound(index);
            lastProcessedValue = currentValue;
            return process(indexes[0], indexes[1]);
        }
    }

    private int getNullRowsRight(int index) {
        int nullRowsRight = index;
        while (nullRowsRight <= rightIndex && chunksIndex.rowAt(nullRowsRight).getObject(orderByColIndex) == null) {
            nullRowsRight++;
        }
        isNullVisit = true;
        return nullRowsRight - 1;
    }

    private int[] getBound(int index) {
        Object currentValue = chunksIndex.rowAt(index).getObject(orderByColIndex);
        int highIndex = index, lowIndex = index;
        while (highIndex <= rightIndex && highIndex >= leftIndex
            && chunksIndex.rowAt(highIndex).getObject(orderByColIndex) != null && compare(
            chunksIndex.rowAt(highIndex).getObject(orderByColIndex), currentValue, rightBound)) {
            highIndex += isAscOrder;
        }
        highIndex -= isAscOrder;
        while (lowIndex <= rightIndex && lowIndex >= leftIndex
            && chunksIndex.rowAt(lowIndex).getObject(orderByColIndex) != null && compare(currentValue,
            chunksIndex.rowAt(lowIndex).getObject(orderByColIndex), leftBound)) {
            lowIndex -= isAscOrder;
        }
        lowIndex += isAscOrder;
        if (isAscOrder > 0) {
            return new int[] {lowIndex, highIndex};
        } else {
            return new int[] {highIndex, lowIndex};
        }
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