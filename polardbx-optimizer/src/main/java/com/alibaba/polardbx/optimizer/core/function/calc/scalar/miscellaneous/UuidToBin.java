/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.miscellaneous;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;

public class UuidToBin extends AbstractScalarFunction {

    public UuidToBin(List<DataType> operandTypes,
                        DataType resultType) {
        super(operandTypes, resultType);
    }

    protected UuidToBin() {
        super(null, null);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UUID_TO_BIN"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length == 0 || args[0] == null) {
            return null;
        }

        String uuidStr = DataTypeUtil.convert(getOperandType(0), DataTypes.StringType, args[0]);
        UUID uuid;
        try {
            uuid = UUID.fromString(uuidStr);
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                "Incorrect string value: '" + args[0] + "' for function uuid_to_bin");
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);

        if (args.length == 1 || (int) args[1] == 0) {
            byteBuffer.putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits());
        } else {
            // 需要交换time-low和time-high
            // 最高有效位 (在标准表示的UUID字符串中是 8-4-4-格式的前两部分)
            long msb = uuid.getMostSignificantBits();

            // 交换time-low和time-high
            // time-low 在 long 的高32位
            // time-high 在 long 的次高16位
            long timeLow = msb >>> 32;
            long timeMid = (msb & 0x00000000FFFF0000L) << 16;
            long timeHigh = (msb & 0x000000000000FFFFL) << 48;

            // 组合交换后的值并放入 ByteBuffer
            msb = timeHigh | timeMid | timeLow;
            byteBuffer.putLong(msb)
                .putLong(uuid.getLeastSignificantBits());
        }

        return byteBuffer.array();
    }
}
