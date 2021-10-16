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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <pre>
 * Returns a “short” universal identifier as a 64-bit unsigned integer. Values returned by UUID_SHORT() differ from the string-format 128-bit identifiers returned by the UUID() function and have different uniqueness properties. The value of UUID_SHORT() is guaranteed to be unique if the following conditions hold:
 *
 * The server_id value of the current server is between 0 and 255 and is unique among your set of master and slave servers
 *
 * You do not set back the system time for your server host between mysqld restarts
 *
 * You invoke UUID_SHORT() on average fewer than 16 million times per second between mysqld restarts
 *
 * The UUID_SHORT() return value is constructed this way:
 *
 *   (server_id & 255) << 56
 * + (server_startup_time_in_seconds << 24)
 * + incremented_variable++;
 * mysql> SELECT UUID_SHORT();
 *         -> 92395783831158784
 *
 * </pre>
 *
 * @author agapple 2016年5月4日 上午10:28:48
 * @since 5.1.25-2
 */
public class UuidShort extends AbstractScalarFunction {
    public UuidShort(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");
    private static long startupTimeInSecond = System.currentTimeMillis() / 1000;
    private static AtomicLong incremented = new AtomicLong(0);

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UUID_SHORT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        long value = 0;
        value = value + (getServerId() & 255) << 56;
        value = value + (startupTimeInSecond << 24);
        value = value + incremented.incrementAndGet();

        if (value < 0) {
            return BIGINT_MAX_VALUE.add(BigInteger.valueOf(value)).toString();
        } else {
            return BigInteger.valueOf(value);
        }
    }

    private static int getServerId() {
        int server_id = TddlNode.getNodeIndex();
        return server_id % 255;
    }
}
