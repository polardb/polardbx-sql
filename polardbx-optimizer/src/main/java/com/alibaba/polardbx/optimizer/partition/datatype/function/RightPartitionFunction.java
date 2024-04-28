package com.alibaba.polardbx.optimizer.partition.datatype.function;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.SqlOperator;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.NON_MONOTONIC;

/**
 * @author chenghui.lch
 */
public class RightPartitionFunction extends PartitionIntFunction {

    public RightPartitionFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        return NON_MONOTONIC;
    }

    @Override
    public SqlOperator getSqlOperator() {
        return TddlOperatorTable.RIGHT;
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
        return str.substring(str.length() - effectLen, str.length());
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
//        return str.substring(str.length() - effectLen, str.length());
//
//    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"RightPartition"};
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

        if (!(obj instanceof RightPartitionFunction)) {
            return false;
        }

        return true;
    }
}
