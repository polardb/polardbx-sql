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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

/**
 * <pre>
 * HEX(str), HEX(N)
 *
 * For a string argument str, HEX() returns a hexadecimal string representation of str where each byte of each character in str is converted to two hexadecimal digits. (Multi-byte characters therefore become more than two digits.) The inverse of this operation is performed by the UNHEX() function.
 *
 * For a numeric argument N, HEX() returns a hexadecimal string representation of the value of N treated as a longlong (BIGINT) number. This is equivalent to CONV(N,10,16). The inverse of this operation is performed by CONV(HEX(N),16,10).
 *
 * mysql> SELECT 0x616263, HEX('abc'), UNHEX(HEX('abc'));
 *         -> 'abc', 616263, 'abc'
 * mysql> SELECT HEX(255), CONV(HEX(255),16,10);
 *         -> 'FF', 255
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午3:59:21
 * @since 5.1.0
 */
public class Hex extends AbstractScalarFunction {
    public Hex(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private final static char[] digVecUpper = "0123456789ABCDEF".toCharArray();

    @Override
    public String[] getFunctionNames() {
        return new String[] {"HEX"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        DataType operandType = operandTypes.get(0);
        if (DataTypeUtil.equalsSemantically(DataTypes.BytesType, operandType)) {
            byte[] bytes = DataTypes.BytesType.convertFrom(args[0]);
            return octetToHex(bytes);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.VarcharType, operandType)
            && operandType.getCharsetName() == CharsetName.BINARY) {
            byte[] bytes = ((Slice) args[0]).getBytes();
            return octetToHex(bytes);
        } else if (DataTypeUtil.isStringType(operandType)) {
            Slice utf8Str = DataTypes.VarcharType.convertFrom(args[0]);
            return Optional.ofNullable(operandTypes)
                .map(types -> types.get(0))
                .filter(SliceType.class::isInstance)
                .map(SliceType.class::cast)
                .map(SliceType::getCharsetHandler)
                .map(charsetHandler -> charsetHandler.encodeFromUtf8(utf8Str))
                .map(Slice::getBytes)
                .map(Hex::octetToHex)
                .orElseGet(
                    () -> octetToHex((byte[]) utf8Str.getBase())
                );
        } else {
            BigInteger intVal = (BigInteger) DataTypes.ULongType.convertJavaFrom(args[0]);
            return intVal.toString(16).toUpperCase();
        }
    }

    /**
     * 依照 mysql 5.7 sql/auth/password.cc 函数 char *octet2hex(char *to, const char *str, uint len)
     */
    private static String octetToHex(byte[] input) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < input.length; i++) {
            int unsignedByte = input[i] & 0xFF;
            builder.append(digVecUpper[unsignedByte >> 4]);
            builder.append(digVecUpper[unsignedByte & 0x0F]);
        }
        return builder.toString();
    }
}
