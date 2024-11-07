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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
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

    private static final Logger logger = LoggerFactory.getLogger(InValuesVectorizedExpression.class);

    private final int operandCount;
    private InValueSet inValueSet;
    private boolean hasNull;
    private boolean allNull;

    public InValuesVectorizedExpression(DataType colDataType, DataType<?> inDataType,
                                        List<RexNode> rexLiteralList, int outputIndex, boolean autoTypeConvert) {
        super(inDataType, outputIndex, new VectorizedExpression[0]);
        this.operandCount = rexLiteralList.size() - 1;
        InValueSet inValueSet = null;
        this.hasNull = false;
        this.allNull = true;

        if (autoTypeConvert && shouldBeConverted(inDataType, colDataType)) {
            try {
                // try converting string into number for better performance
                inValueSet = new InValueSet(colDataType, operandCount);
                for (int operandIndex = 1; operandIndex <= operandCount; operandIndex++) {
                    Object value = ((RexLiteral) rexLiteralList.get(operandIndex)).getValue3();
                    Object convertedValue = colDataType.convertFrom(value);
                    if (convertedValue == null) {
                        this.hasNull = true;
                    } else {
                        this.allNull = false;
                        inValueSet.add(convertedValue);
                    }
                }
                this.inValueSet = inValueSet;
                return;
            } catch (Exception e) {
                logger.warn(e.getMessage());
            }
        }

        inValueSet = new InValueSet(inDataType, operandCount);
        for (int operandIndex = 1; operandIndex <= operandCount; operandIndex++) {
            Object value = ((RexLiteral) rexLiteralList.get(operandIndex)).getValue3();
            Object convertedValue = inDataType.convertFrom(value);
            if (convertedValue == null) {
                this.hasNull = true;
            } else {
                this.allNull = false;
                inValueSet.add(convertedValue);
            }
        }

        this.inValueSet = inValueSet;
    }

    private boolean shouldBeConverted(DataType<?> inDataType, DataType colDataType) {
        return DataTypeUtil.anyMatchSemantically(inDataType, DataTypes.CharType, DataTypes.VarcharType) &&
            DataTypeUtil.anyMatchSemantically(colDataType, DataTypes.IntegerType, DataTypes.LongType);
    }

    public static InValuesVectorizedExpression from(List<RexNode> rexLiteralList, int outputIndex) {
        return from(rexLiteralList, outputIndex, false);
    }

    /**
     * @param rexLiteralList start from index:1
     */
    public static InValuesVectorizedExpression from(List<RexNode> rexLiteralList, int outputIndex, boolean autoTypeConvert) {
        Preconditions.checkArgument(rexLiteralList.size() > 1,
            "Illegal in values, list size: " + rexLiteralList.size());
        DataType colDataType = new Field(rexLiteralList.get(0).getType()).getDataType();
        RexLiteral rexLiteral = (RexLiteral) rexLiteralList.get(1);
        Field field = new Field(rexLiteral.getType());
        return new InValuesVectorizedExpression(colDataType, field.getDataType(), rexLiteralList, outputIndex, autoTypeConvert);
    }

    public int getOperandCount() {
        return operandCount;
    }

    public InValueSet getInValueSet() {
        return inValueSet;
    }

    public boolean hasNull() {
        return this.hasNull;
    }

    public boolean allNull() {
        return this.allNull;
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

        static SetType getSetType(DataType<?> dataType) {
            if (dataType == DataTypes.IntegerType) {
                return SetType.INT;
            }
            if (dataType == DataTypes.LongType) {
                return SetType.LONG;
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

        public DataType getDataType() {
            return dataType;
        }

        enum SetType {
            INT,
            LONG,
            OTHERS
        }
    }
}
