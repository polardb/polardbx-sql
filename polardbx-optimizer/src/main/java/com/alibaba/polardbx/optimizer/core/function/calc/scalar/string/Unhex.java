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

import com.alibaba.polardbx.common.charset.CharsetHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * <pre>
 * UNHEX(str)
 *
 * For a string argument str, UNHEX(str) interprets each pair of characters in the argument as a hexadecimal number and converts it to the byte represented by the number. The return value is a binary string.
 *
 * mysql> SELECT UNHEX('4D7953514C');
 *         -> 'MySQL'
 * mysql> SELECT 0x4D7953514C;
 *         -> 'MySQL'
 * mysql> SELECT UNHEX(HEX('string'));
 *         -> 'string'
 * mysql> SELECT HEX(UNHEX('1267'));
 *         -> '1267'
 *
 * The characters in the argument string must be legal hexadecimal digits: '0' .. '9', 'A' .. 'F', 'a' .. 'f'. If the argument contains any nonhexadecimal digits, the result is NULL:
 *
 * mysql> SELECT UNHEX('GG');
 * +-------------+
 * | UNHEX('GG') |
 * +-------------+
 * | NULL        |
 * +-------------+
 *
 * A NULL result can occur if the argument to UNHEX() is a BINARY column, because values are padded with 0x00 bytes when stored but those bytes are not stripped on retrieval. For example, '41' is stored into a CHAR(3) column as '41 ' and retrieved as '41' (with the trailing pad space stripped), so UNHEX() for the column value returns 'A'. By contrast '41' is stored into a BINARY(3) column as '41\0' and retrieved as '41\0' (with the trailing pad 0x00 byte not stripped). '\0' is not a legal hexadecimal digit, so UNHEX() for the column value returns NULL.
 *
 * For a numeric argument N, the inverse of HEX(N) is not performed by UNHEX(). Use CONV(HEX(N),16,10) instead. See the description of HEX().
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午2:54:57
 * @since 5.1.0
 */
public class Unhex extends AbstractScalarFunction {
    public Unhex(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static String hexString = "0123456789ABCDEF";

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UNHEX"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        String byteStr = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]).toUpperCase();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (int i = 0; i < byteStr.length(); i += 2) {
            int index0 = hexString.indexOf(byteStr.charAt(i));
            if (index0 < 0) {
                return null;
            }
            if (i + 1 < byteStr.length()) {
                int index1 = hexString.indexOf(byteStr.charAt(i + 1));
                if (index1 < 0) {
                    return null;
                }

                baos.write((index0 << 4 | index1));
            } else {
                baos.write((index0));
            }
        }

        if (resultType instanceof SliceType) {
            CharsetHandler charsetHandler = ((SliceType) resultType).getCharsetHandler();
            return charsetHandler.decode(baos.toByteArray());
        }
        return resultType.convertFrom(baos.toByteArray());
    }

}
