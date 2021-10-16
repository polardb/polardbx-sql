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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.encryption;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;
import java.util.Random;

/**
 * 可用于生成aes加密的初始向量
 * https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_random-bytes
 */
public class RandomBytes extends AbstractScalarFunction {
    public RandomBytes(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static final Random random = new Random(System.currentTimeMillis());

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        int len = DataTypeUtil.convert(operandTypes.get(0), DataTypes.IntegerType, args[0]);
        if (len <= 0 || len > 1024) {
            throw GeneralUtil.nestedException("length value is out of range in 'random_bytes'");
        }
        byte[] res = new byte[len];
        random.nextBytes(res);
        return res;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"RANDOM_BYTES"};
    }
}
