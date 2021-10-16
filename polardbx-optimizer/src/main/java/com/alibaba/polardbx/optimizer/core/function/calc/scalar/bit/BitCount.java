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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.bit;

import java.math.BigInteger;
import java.util.List;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.util.BitString;

/**
 * BIT_COUNT(N)
 *
 * <pre>
 * Returns the number of bits that are set in the argument N.
 *
 * mysql> SELECT BIT_COUNT(29), BIT_COUNT(b'101010');
 *         -> 4, 3
 * </pre>
 *
 * @author jianghang 2014-4-14 下午11:22:02
 * @since 5.0.7
 */
public class BitCount extends AbstractScalarFunction {
    public BitCount(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        if (args[0] instanceof ByteString) {
            byte[] bytes = ((ByteString) args[0]).getBytes();
            int count = 0;
            String bitString = BitString.createFromBytes(bytes).toBitString();
            for (int i = 0; i < bitString.length(); i++) {
                if (bitString.charAt(i) == '1') {
                    count++;
                }
            }
            return count;
        }

        Object t = resultType.convertFrom(args[0]);
        if (t instanceof BigInteger) {
            return ((BigInteger) t).bitCount();
        } else if (t instanceof UInt64) {
            return ((UInt64) t).toBigInteger().bitCount();
        } else {
            return Long.bitCount((Long) t);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"BIT_COUNT"};
    }
}
