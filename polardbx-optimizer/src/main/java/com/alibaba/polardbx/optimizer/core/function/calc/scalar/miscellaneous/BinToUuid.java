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
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import io.airlift.slice.Slice;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;

public class BinToUuid extends AbstractScalarFunction {
    public BinToUuid(List<DataType> operandTypes,
                        DataType resultType) {
        super(operandTypes, resultType);
    }
    protected BinToUuid() {
        super(null, null);
    }
    @Override
    public String[] getFunctionNames() {
        return new String[] {"BIN_TO_UUID"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length == 0 || args[0] == null) {
            return null;
        }

        if (!(args[0] instanceof Slice || args[0] instanceof byte[])) {
            throw new UnsupportedOperationException(
                "Incorrect string value: '" + args[0] + "' for function bin_to_uuid");
        }

        byte[] uuidBytes = args[0] instanceof byte[] ? (byte[]) args[0] : ((Slice) args[0]).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(uuidBytes);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);

        long mostSignificantBits;
        long leastSignificantBits;

        if (args.length == 1 || (int) args[1] == 0) {
            mostSignificantBits = byteBuffer.getLong();
            leastSignificantBits = byteBuffer.getLong();
        } else {
            // 如果字节进行了交换，需要还原它们的顺序
            // We need to swap time-low and time-high back to their original places
            mostSignificantBits = byteBuffer.getLong();

            long timeLowSwapped = (mostSignificantBits & 0x00000000FFFFFFFFL) << 32;
            long timeMid = (mostSignificantBits & 0x0000FFFF00000000L) >>> 16;
            long timeHighSwapped = mostSignificantBits >>> 48;

            mostSignificantBits = timeLowSwapped | timeMid | timeHighSwapped;

            // 继续读取剩余的部分作为 least significant bits
            leastSignificantBits = byteBuffer.getLong(); // 剩下的8字节作为lsb
        }

        return new UUID(mostSignificantBits, leastSignificantBits).toString();
    }
}
