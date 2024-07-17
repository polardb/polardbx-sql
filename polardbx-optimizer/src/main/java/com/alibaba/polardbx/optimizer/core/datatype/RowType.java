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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprUtil;

import java.util.List;

import static com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil.getTypeOfObject;

public class RowType extends AbstractDataType<RowValue> {

    private List<DataType> dataTypes;

    public RowType() {
    }

    public RowType(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    @Override
    public Class getDataClass() {
        return RowValue.class;
    }

    @Override
    public ResultGetter getResultGetter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowValue getMaxValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowValue getMinValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Calculator getCalculator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSqlType() {
        return UNDECIDED_SQL_TYPE;
    }

    @Override
    public String getStringSqlType() {
        return "ROW";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        throw new UnsupportedOperationException();
    }

    public Long nullNotSafeEqual(Object o1, Object o2) {
        if (o1 == o2) {
            if (o1 == null || o2 == null) {
                return null;
            } else {
                return 1L;
            }
        }
        RowValue no1 = convertFrom(o1);
        RowValue no2 = convertFrom(o2);
        int num1 = no1.getValues().size();
        int num2 = no2.getValues().size();
        if (num1 != num2) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Operand should contain %s column(s)", num1));
        }
        boolean hasNull = false;
        for (int i = 0; i < num1; i++) {
            Object val1 = no1.getValues().get(i);
            Object val2 = no2.getValues().get(i);
            if (val1 == null || val2 == null) {
                hasNull = true;
                continue;
            } else {
                DataType type = inferCmpType(getTypeOfObject(val1), getTypeOfObject(val2));
                int ret = type.compare(val1, val2);
                if (ret != 0) {
                    return 0L;
                }
            }
        }
        return hasNull ? null : 1L;
    }

    public Integer nullNotSafeCompare(Object o1, Object o2, boolean mustGreater) {
        if (o1 == o2) {
            if (o1 == null || o2 == null) {
                return null;
            } else {
                return mustGreater ? -1 : 0;
            }
        }
        RowValue no1 = convertFrom(o1);
        RowValue no2 = convertFrom(o2);
        int num1 = no1.getValues().size();
        int num2 = no2.getValues().size();
        if (num1 != num2) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Operand should contain %s column(s)", num1));
        }
        for (int i = 0; i < num1; i++) {
            Object val1 = no1.getValues().get(i);
            Object val2 = no2.getValues().get(i);
            if (val1 == null || val2 == null) {
                return null;
            } else {
                DataType type = inferCmpType(getTypeOfObject(val1), getTypeOfObject(val2));
                int ret = type.compare(val1, val2);
                if (ret != 0) {
                    return ret;
                }
            }
        }
        return mustGreater ? -1 : 0;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }
        if ((o1 instanceof RowValue)
            && (o2 instanceof RowValue)) {

            List<Object> o1s = ((RowValue) o1).getValues();
            List<Object> o2s = ((RowValue) o2).getValues();
            if (o1s.size() == o2s.size() && o1s.size() == dataTypes.size()) {
                for (int i = 0; i < o1s.size(); i++) {
                    int ret = dataTypes.get(i).compare(o1s.get(i), o2s.get(i));
                    if (ret != 0) {
                        return ret;
                    }
                }
                return 0;
            } else {
                throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private DataType inferCmpType(DataType leftType, DataType rightType) {
        if (leftType == DataTypes.RowType || rightType == DataTypes.RowType) {
            return DataTypes.RowType;
        } else {
            return Rex2ExprUtil.compareTypeOf(leftType, rightType, true, true);
        }
    }

}

