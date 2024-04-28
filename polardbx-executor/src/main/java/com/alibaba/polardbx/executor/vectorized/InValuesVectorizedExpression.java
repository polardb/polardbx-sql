package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Representation of in values
 */
public class InValuesVectorizedExpression extends AbstractVectorizedExpression {

    private final int operandCount;
    private final InValueSet inValueSet;
    private boolean hasNull;

    public InValuesVectorizedExpression(DataType<?> dataType, List<RexNode> rexLiteralList, int outputIndex) {
        super(dataType, outputIndex, new VectorizedExpression[0]);
        this.operandCount = rexLiteralList.size() - 1;
        this.inValueSet = new InValueSet(dataType, operandCount);
        this.hasNull = false;

        for (int operandIndex = 1; operandIndex <= operandCount; operandIndex++) {
            Object value = ((RexLiteral) rexLiteralList.get(operandIndex)).getValue3();
            Object convertedValue = dataType.convertFrom(value);
            if (convertedValue == null) {
                hasNull = true;
            } else {
                inValueSet.add(convertedValue);
            }
        }
    }

    /**
     * @param rexLiteralList start from index1
     */
    public static InValuesVectorizedExpression from(List<RexNode> rexLiteralList, int outputIndex) {
        Preconditions.checkArgument(rexLiteralList.size() > 1,
            "Illegal in values, list size: " + rexLiteralList.size());
        RexLiteral rexLiteral = (RexLiteral) rexLiteralList.get(1);
        Field field = new Field(rexLiteral.getType());
        return new InValuesVectorizedExpression(field.getDataType(), rexLiteralList, outputIndex);
    }

    public int getOperandCount() {
        return operandCount;
    }

    public InValueSet getInValueSet() {
        return inValueSet;
    }

    public boolean hasNull() {
        return hasNull;
    }

    @Override
    public void eval(EvaluationContext ctx) {

    }

    public static class InValueSet {
        private final DataType dataType;
        private final SetType setType;
        private Set<Object> set = null;
        private IntOpenHashSet intHashSet = null;
        private LongOpenHashSet longHashSet = null;

        public InValueSet(DataType<?> dataType, int capacity) {
            this.dataType = dataType;
            this.setType = getSetType(dataType);
            switch (setType) {
            case INT:
                this.intHashSet = new IntOpenHashSet(capacity);
                break;
            case LONG:
                this.longHashSet = new LongOpenHashSet(capacity);
                break;
            case OTHERS:
                this.set = new HashSet<>(capacity);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        private SetType getSetType(DataType<?> dataType) {
            if (dataType == DataTypes.LongType) {
                return SetType.LONG;
            }
            if (dataType == DataTypes.IntegerType) {
                return SetType.INT;
            }
            return SetType.OTHERS;
        }

        /**
         * skip type check
         */
        public void add(Object value) {
            switch (setType) {
            case INT:
                intHashSet.add((int) value);
                break;
            case LONG:
                longHashSet.add((long) value);
                break;
            case OTHERS:
                set.add(value);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        public boolean contains(int value) {
            switch (setType) {
            case INT:
                return intHashSet.contains(value);
            case LONG:
                return longHashSet.contains(value);
            case OTHERS:
                return set.contains(dataType.convertFrom(value));
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        public boolean contains(long value) {
            switch (setType) {
            case INT:
                if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                    return intHashSet.contains((int) value);
                } else {
                    return false;
                }
            case LONG:
                return longHashSet.contains(value);
            case OTHERS:
                return set.contains(dataType.convertFrom(value));
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        public boolean contains(Object value) {
            if (value == null) {
                return false;
            }
            switch (setType) {
            case INT:
                return intHashSet.contains(dataType.convertFrom(value));
            case LONG:
                return longHashSet.contains(dataType.convertFrom(value));
            case OTHERS:
                return set.contains(dataType.convertFrom(value));
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        enum SetType {
            INT,
            LONG,
            OTHERS
        }
    }
}
