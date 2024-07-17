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

import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.NON_MONOTONIC;

/**
 * @author chenghui.lch
 */
public class LeftPartitionFunction extends PartitionIntFunction {

    public LeftPartitionFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        return NON_MONOTONIC;
    }

    @Override
    public SqlOperator getSqlOperator() {
        return TddlOperatorTable.LEFT;
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
        PartitionField lengthField = fullParamFields.get(1);
        if (partitionField.isNull() || lengthField.isNull()) {
            return null;
        }

        Slice slice = partitionField.stringValue();
        if (slice == null) {
            return null;
        }
        String str = slice.toStringUtf8();
        Long len = lengthField.longValue();

//        String str = DataTypeUtil.convert(getOperandType(0), DataTypes.StringType, args[0]);
//        Long len = DataTypes.LongType.convertFrom(args[1]);

        int effectLen;
        if (len < 0) {
            return "";
        }
        int length = str.length();
        if (len > length) {
            effectLen = length;
        } else {
            effectLen = len.intValue();
        }
        return str.substring(0, effectLen);
    }

//    @Override
//    public Object compute(Object[] args, ExecutionContext ec) {
//        for (Object arg : args) {
//            if (FunctionUtils.isNull(arg)) {
//                return null;
//            }
//        }
//        String str = DataTypeUtil.convert(getOperandType(0), DataTypes.StringType, args[0]);
//        Long len = DataTypes.LongType.convertFrom(args[1]);
//        int effectLen;
//        if (len < 0) {
//            return "";
//        }
//        int length = str.length();
//        if (len > length) {
//            effectLen = length;
//        } else {
//            effectLen = len.intValue();
//        }
//        return str.substring(0, effectLen);
//    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LeftPartition"};
    }

    @Override
    public DataType getReturnType() {
        if (this.resultField != null) {
            return resultField.getDataType();
        } else {
            return DataTypes.VarcharType;
        }
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

        if (!(obj instanceof LeftPartitionFunction)) {
            return false;
        }

        return true;
    }
}
