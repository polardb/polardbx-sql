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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.math;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.datatype.UInt64Utils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.google.common.primitives.UnsignedLongs;

import java.math.BigInteger;
import java.util.List;

/**
 * Rounds the argument X to D decimal places. The rounding algorithm depends on
 * the data type of X. D defaults to 0 if not specified. D can be negative to
 * cause D digits left of the decimal point of the value X to become zero.
 */
public class Round extends AbstractScalarFunction {

    private static final long[] LOG_10_INT = {
        1, 10, 100, 1000, 10000L, 100000L, 1000000L, 10000000L,
        100000000L, 1000000000L, 10000000000L, 100000000000L,
        1000000000000L, 10000000000000L, 100000000000000L,
        1000000000000000L, 10000000000000000L, 100000000000000000L,
        1000000000000000000L /*10000000000000000000L*/
    };

    private static final long LOG_10_19 = UInt64.fromString("10000000000000000000").longValue();

    public Round(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (FunctionUtils.isNull(args[0]) || (args.length > 1 && FunctionUtils.isNull(args[1]))) {
            return null;
        }

        // For 1 arg function, use 0 as scale.
        if (args.length == 1) {
            args = new Object[] {args[0], 0};
        }

        if (DataTypeUtil.isIntType(type)) {
            return intOperator(args);
        } else if (DataTypeUtil.isDecimalType(type)) {
            return decimalOperator(args);
        } else if (DataTypeUtil.isRealType(type)) {
            return realOperator(args);
        }
        return null;
    }

    private long intOperator(Object[] args) {
        Object arg0 = args[0];
        Object arg1 = args[1];

        boolean isArg0Unsigned =
            operandTypes.get(0) instanceof ULongType || arg0 instanceof UInt64 || arg0 instanceof BigInteger;
        boolean isArg1Unsigned =
            operandTypes.get(1) instanceof ULongType || arg1 instanceof UInt64 || arg1 instanceof BigInteger;

        Object valueObj = isArg0Unsigned
            ? DataTypes.ULongType.convertFrom(arg0)
            : DataTypes.LongType.convertFrom(arg0);
        long value = valueObj == null ? 0L : ((Number) valueObj).longValue();

        Object decObj = isArg1Unsigned
            ? DataTypes.ULongType.convertFrom(arg1)
            : DataTypes.LongType.convertFrom(arg1);
        long dec = decObj == null ? 0L : ((Number) decObj).longValue();

        if (dec >= 0 || isArg1Unsigned) {
            // integer have not digits after point
            return value;
        }

        long absDec = -dec;
        if (UnsignedLongs.compare(absDec, 20) >= 0) {
            // util now, we don't support round scale > 20
            return 0L;
        }

        long tmp = absDec == 19 ? LOG_10_19 : LOG_10_INT[(int) absDec];
        if (isTruncate()) {
            return isArg0Unsigned ? UInt64Utils.divide(value, tmp) * tmp
                : (value / tmp) * tmp;
        } else {
            return (isArg0Unsigned || value >= 0) ? UInt64Utils.round(value, tmp) :
                -UInt64Utils.round(-value, tmp);
        }
    }

    private Decimal decimalOperator(Object[] args) {
        Object arg0 = args[0];
        Object arg1 = args[1];

        boolean isArg1Unsigned = false;
        if (operandTypes.size() > 1) {
            isArg1Unsigned =
                operandTypes.get(1) instanceof ULongType || arg1 instanceof UInt64 || arg1 instanceof BigInteger;
        }

        Decimal value = DataTypes.DecimalType.convertFrom(arg0);

        Object decObj = isArg1Unsigned
            ? DataTypes.ULongType.convertFrom(arg1)
            : DataTypes.LongType.convertFrom(arg1);
        long dec = decObj == null ? 0L : ((Number) decObj).longValue();

        if (isArg1Unsigned) {
            dec = UInt64Utils.compareUnsigned(dec, DecimalTypeBase.MAX_DECIMAL_SCALE) < 0 ? dec :
                DecimalTypeBase.MAX_DECIMAL_SCALE;
        } else if (dec >= 0) {
            dec = Math.min(dec, DecimalTypeBase.MAX_DECIMAL_SCALE);
        } else if (dec < Integer.MIN_VALUE) {
            dec = Integer.MIN_VALUE;
        }

        // do decimal round
        DecimalStructure decimalStructure = value.getDecimalStructure();
        DecimalStructure to = new DecimalStructure();
        DecimalRoundMod mod = isTruncate() ? DecimalRoundMod.TRUNCATE : DecimalRoundMod.HALF_UP;
        int error = FastDecimalUtils.round(decimalStructure, to, (int) dec, mod);
        if (error > 0) {
            return null;
        } else {
            return new Decimal(to);
        }
    }

    private Double realOperator(Object[] args) {
        Object arg0 = args[0];
        Object arg1 = args[1];

        Double value = DataTypes.DoubleType.convertFrom(arg0);
        boolean isArg1Unsigned =
            operandTypes.get(1) instanceof ULongType || arg1 instanceof UInt64 || arg1 instanceof BigInteger;

        Object decObj = isArg1Unsigned
            ? DataTypes.ULongType.convertFrom(arg1)
            : DataTypes.LongType.convertFrom(arg1);
        long dec = decObj == null ? 0L : ((Number) decObj).longValue();

        boolean isDecNeg = dec < 0 && !isArg1Unsigned;
        long absDec = isDecNeg ? -dec : dec;

        // tmp2 is here to avoid return the value with 80 bit precision
        // This will fix that the test round(0.1,1) = round(0.1,1) is true
        // Tagging with volatile is no guarantee, it may still be optimized away
        double tmp2;
        double tmp = Math.pow(10, absDec);
        double divTmp = value / tmp;
        double mulTmp = value * tmp;
        if (isDecNeg && Double.isInfinite(tmp)) {
            tmp2 = 0.0D;
        } else if (!isDecNeg && (Double.isInfinite(mulTmp) || Double.isNaN(mulTmp))) {
            tmp2 = value;
        } else if (isTruncate()) {
            if (value >= 0.0D) {
                tmp2 = dec < 0 ? Math.floor(divTmp) * tmp : Math.floor(mulTmp) / tmp;
            } else {
                tmp2 = dec < 0 ? Math.ceil(divTmp) * tmp : Math.ceil(mulTmp) / tmp;
            }
        } else {
            tmp2 = dec < 0 ? Math.rint(divTmp) * tmp : Math.rint(mulTmp) / tmp;
        }
        return tmp2;
    }

    protected boolean isTruncate() {
        return false;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ROUND"};
    }
}
