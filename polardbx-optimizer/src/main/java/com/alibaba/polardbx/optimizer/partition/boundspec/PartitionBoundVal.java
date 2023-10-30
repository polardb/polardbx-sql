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

package com.alibaba.polardbx.optimizer.partition.boundspec;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;

/**
 * The definition the const value of one partition bound
 *
 * @author chenghui.lch
 */
public class PartitionBoundVal {

    protected static PartitionBoundVal MIN_VAL = createMinValueInner();
    protected static PartitionBoundVal MAX_VAL = createMaxValueInner();

    protected static PartitionBoundVal DEFAULT_VAL = createDefaultValueInner();

    protected static PartitionBoundVal HASH_BOUND_MAX_VAL = createHashBoundMaxValueInner();
    /**
     * all hashcode must be less than Long.MAX_VALUE, so
     * the maxvalue of partSpec like
     * "partition p1 values less than (maxvalue)"
     * is Long.MAX_VALUE
     */
    public static final long HASH_BOUND_MAX_VAL_LONG = Long.MAX_VALUE;
    public static final long HASH_BOUND_MIN_VAL_LONG = Long.MIN_VALUE;

    /**
     * The computed actual value of raw expression of create tbl ddl definition
     */
    protected PartitionField value;

    /**
     * label if the value is null
     */
    protected boolean isNullValue = false;

    /**
     * The kind of bound value, e.g. min_val/max_val/normal_val
     */
    protected PartitionBoundValueKind valueKind = PartitionBoundValueKind.DATUM_NORMAL_VALUE;

    public static PartitionBoundVal createPartitionBoundVal(PartitionField value,
                                                            PartitionBoundValueKind valueKind) {
        boolean isNullVal = false;
        PartitionField finalValue = value;
        if (valueKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
            if (value == null) {
                isNullVal = true;
                PartitionField nullFld = PartitionFieldBuilder.createField(DataTypes.LongType);
                nullFld.setNull(true);
                finalValue = nullFld;
            } else {
                isNullVal = value.isNull();
            }

        }
        PartitionBoundVal bndVal = new PartitionBoundVal(finalValue, isNullVal, valueKind);
        return bndVal;
    }

    public static PartitionBoundVal createPartitionBoundVal(PartitionField value,
                                                            PartitionBoundValueKind valueKind,
                                                            boolean isAlwaysNull) {
        boolean isNullVal = isAlwaysNull;
        if (valueKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE && !isAlwaysNull) {
            if (value == null) {
                value = PartitionFieldBuilder.createField(DataTypes.LongType);
                value.setNull(true);
                isNullVal = true;
            } else {
                isNullVal = value.isNull();
            }
        }
        return new PartitionBoundVal(value, isNullVal, valueKind);
    }

    public static PartitionBoundVal createNormalValue(PartitionField value) {
        return createPartitionBoundVal(value, PartitionBoundValueKind.DATUM_NORMAL_VALUE);
    }

    public static PartitionBoundVal createMinValue() {
        return PartitionBoundVal.MIN_VAL;
    }

    public static PartitionBoundVal createMaxValue() {
        return PartitionBoundVal.MAX_VAL;
    }

    public static PartitionBoundVal createDefaultValue() {
        return PartitionBoundVal.DEFAULT_VAL;
    }

    public static PartitionBoundVal createHashBoundMaxValue() {
        return PartitionBoundVal.HASH_BOUND_MAX_VAL;
    }

    private PartitionBoundVal() {
    }

    public PartitionBoundVal(PartitionField value, boolean isNullValue, PartitionBoundValueKind valueKind) {
        /**
         * When the value kind of value is min_val or max_val, this.value is null
         */
        this.value = value;
        this.isNullValue = isNullValue;
        this.valueKind = valueKind;
    }

    protected static PartitionBoundVal createMinValueInner() {
        return new PartitionBoundVal(null, false, PartitionBoundValueKind.DATUM_MIN_VALUE);
    }

    protected static PartitionBoundVal createMaxValueInner() {
        return new PartitionBoundVal(null, false, PartitionBoundValueKind.DATUM_MAX_VALUE);
    }

    protected static PartitionBoundVal createDefaultValueInner() {

        return new PartitionBoundVal(null, false, PartitionBoundValueKind.DATUM_DEFAULT_VALUE);
    }

    protected static PartitionBoundVal createHashBoundMaxValueInner() {
        PartitionField bndFld = PartitionFieldBuilder.createField(DataTypes.LongType);
        bndFld.store(Long.MAX_VALUE, DataTypes.LongType);
        return new PartitionBoundVal(bndFld, false, PartitionBoundValueKind.DATUM_NORMAL_VALUE);
    }

    public PartitionField getValue() {
        return value;
    }

    public void setValue(PartitionField value) {
        this.value = value;
    }

    public boolean isNullValue() {
        return isNullValue;
    }

    public void setNullValue(boolean nullValue) {
        isNullValue = nullValue;
    }

    public PartitionBoundVal copy() {
        PartitionBoundVal val = new PartitionBoundVal();
        val.setValue(this.value);
        val.setNullValue(this.isNullValue);
        val.setValueKind(this.valueKind);
        return val;
    }

    @Override
    public int hashCode() {

        int hashCodeVal = valueKind.hashCode();
        if (valueKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
            if (isNullValue) {
                hashCodeVal |= 0x01;
            } else {
                hashCodeVal ^= value.hashCode();
            }
            return hashCodeVal;
        } else {
            return hashCodeVal;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj != null && obj.getClass() == this.getClass()) {

            PartitionBoundVal objBnd = (PartitionBoundVal) obj;

            if (objBnd.getValueKind() != this.valueKind) {
                return false;
            }

            if (objBnd.isNullValue() != objBnd.isNullValue) {
                return false;
            }

            if (objBnd.getValue() == null && this.value == null) {
                return true;
            }

            if (((PartitionBoundVal) obj).getValue().stringValue().equals(value.stringValue())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return PartitionBoundVal.printPartBndVal(this);
    }

    public static String printPartBndVal(PartitionBoundVal bndVal) {
        StringBuilder sb = new StringBuilder();

        PartitionField partField = bndVal.getValue();
        if (bndVal.getValueKind() == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
            boolean needWrappWithQuotationMarks = false;
            if (!bndVal.isNullValue() && !(DataTypeUtil.isNumberSqlType(partField.dataType()))) {
                needWrappWithQuotationMarks = true;
            }
            if (needWrappWithQuotationMarks) {
                sb.append("'");
            }
            if (bndVal.isNullValue()) {
                sb.append("null");
            } else {
                sb.append(partField.stringValue().toStringUtf8());
            }

            if (needWrappWithQuotationMarks) {
                sb.append("'");
            }
        } else if (bndVal.getValueKind() == PartitionBoundValueKind.DATUM_MAX_VALUE) {
            sb.append("MAXVALUE");
        } else if (bndVal.getValueKind() == PartitionBoundValueKind.DATUM_MIN_VALUE) {
            sb.append("MINVALUE");
        } else {
            sb.append("DEFAULT");
        }
        return sb.toString();
    }

    public PartitionBoundValueKind getValueKind() {
        return valueKind;
    }

    public boolean isMinValue() {
        return valueKind.equals(PartitionBoundValueKind.DATUM_MIN_VALUE);
    }

    public boolean isMaxValue() {
        return valueKind.equals(PartitionBoundValueKind.DATUM_MAX_VALUE);
    }

    public boolean isNormalValue() {
        return valueKind.equals(PartitionBoundValueKind.DATUM_NORMAL_VALUE);
    }

    public boolean isDefaultValue() {
        return valueKind.equals(PartitionBoundValueKind.DATUM_DEFAULT_VALUE);
    }

    public void setValueKind(PartitionBoundValueKind valueKind) {
        this.valueKind = valueKind;
    }
}
