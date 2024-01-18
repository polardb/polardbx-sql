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

package com.alibaba.polardbx.optimizer.partition.datatype.function;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.SqlOperator;

import java.util.Arrays;
import java.util.List;

import java.util.List;

import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.NON_MONOTONIC;

/**
 * @author chenghui.lch
 */
public class SubStrPartitionFunction extends PartitionIntFunction {

    private int position;

    private int length;

    public SubStrPartitionFunction() {
        super(null, null);
    }

    public SubStrPartitionFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
        this.position = Integer.MIN_VALUE;
        this.length = Integer.MIN_VALUE;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        return NON_MONOTONIC;
    }

    @Override
    public SqlOperator getSqlOperator() {
        return TddlOperatorTable.SUBSTR;
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return null;
    }

    @Override
    public long evalInt(PartitionField partitionField, SessionProperties sessionProperties) {
        throw new NotSupportException();
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                boolean[] endpoints) {
        throw new NotSupportException();
    }
    @Override
    public Object evalEndpoint(List<PartitionField> fullParamFlds,
                               SessionProperties sessionProperties,
                               boolean[] endpoints) {
        return compute(fullParamFlds, sessionProperties, endpoints);
    }

    protected Object compute(List<PartitionField> fullParamFields,
                             SessionProperties sessionProperties,
                             boolean[] endpoints) {
        PartitionField partitionField = fullParamFields.get(0);

        int positionFldVal = Integer.MIN_VALUE;
        int lengthFldVal = Integer.MIN_VALUE;

        if (fullParamFields.size() >= 2) {
            positionFldVal = Long.valueOf(fullParamFields.get(1).longValue()).intValue();
            if (fullParamFields.size() >= 3) {
                lengthFldVal = Long.valueOf(fullParamFields.get(2).longValue()).intValue();
            }
        }

        if (positionFldVal == 0) {
            return "";
        }

        Slice slice = partitionField.stringValue();
        if (slice == null) {
            return null;
        }
        String str = slice.toStringUtf8();

        int pos = positionFldVal;
        if (positionFldVal < 0) {
            pos = str.length() + pos + 1;
        }

        if (lengthFldVal != Integer.MIN_VALUE) {
            if (lengthFldVal < 1) {
                return "";
            } else {
                return TStringUtil.substring(str, pos - 1, pos - 1 + lengthFldVal);
            }
        } else {
            return TStringUtil.substring(str, pos - 1);
        }

    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SubStrPartition"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.VarcharType;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[] {getSqlOperator()});
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof SubStrPartitionFunction)) {
            return false;
        }

        return true;
    }
}
