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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;
import java.util.UUID;

/**
 * <pre>
 * UUID() returns a value that conforms to UUID version 1 as described in RFC 4122. The value is a 128-bit number represented as a utf8 string of five hexadecimal numbers in aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee format:
 *
 * The first three numbers are generated from the low, middle, and high parts of a timestamp. The high part also includes the UUID version number.
 *
 * The fourth number preserves temporal uniqueness in case the timestamp value loses monotonicity (for example, due to daylight saving time).
 *
 * The fifth number is an IEEE 802 node number that provides spatial uniqueness. A random number is substituted if the latter is not available (for example, because the host device has no Ethernet card, or it is unknown how to find the hardware address of an interface on the host operating system). In this case, spatial uniqueness cannot be guaranteed. Nevertheless, a collision should have very low probability.
 *
 * The MAC address of an interface is taken into account only on FreeBSD and Linux. On other operating systems, MySQL uses a randomly generated 48-bit number.
 *
 * mysql> SELECT UUID();
 *         -> '6ccd780c-baba-1026-9564-0040f4311e29'
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午4:07:06
 * @since 5.1.0
 */
public class Uuid extends AbstractScalarFunction {
    public Uuid(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UUID"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return UUID.randomUUID().toString();
    }
}
