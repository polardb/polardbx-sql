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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Computes a cyclic redundancy check value and returns a 32-bit unsigned value.
 * The result is NULL if the argument is NULL. The argument is expected to be a
 * string and (if possible) is treated as one if it is not.
 *
 * <pre>
 * mysql> SELECT CRC32('MySQL');
 *         -> 3259397556
 * mysql> SELECT CRC32('mysql');
 *         -> 2501908538
 * </pre>
 *
 * @author jianghang 2014-4-14 下午10:16:56
 * @since 5.0.7
 */
public class Crc32 extends AbstractScalarFunction {
    public Crc32(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        String d = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        // get bytes from string
        byte bytes[] = d.getBytes();
        Checksum checksum = new CRC32();
        // update the current checksum with the specified array of bytes
        checksum.update(bytes, 0, bytes.length);
        // get the current checksum value
        return resultType.convertFrom(checksum.getValue());
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CRC32"};
    }
}
