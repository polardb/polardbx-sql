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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.security.MessageDigest;
import java.util.List;

/**
 * SHA2族散列算法
 */
public class SHA2 extends AbstractScalarFunction {
    public SHA2(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static final List<Integer> SHA2_HASH_LENGTH =
        ImmutableList.of(224, 256, 384, 512);

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0]) || FunctionUtils.isNull(args[1])) {
            return null;
        }
        int hashLen = DataTypeUtil.convert(operandTypes.get(1), DataTypes.IntegerType, args[1]);
        // SHA2-0 等价于 SHA2-256
        hashLen = (hashLen == 0) ? 256 : hashLen;
        if (!SHA2_HASH_LENGTH.contains(hashLen)) {
            return null;
        }
        byte[] input = DataTypeUtil.convert(operandTypes.get(0), DataTypes.BinaryType, args[0]);

        String sha2str = "";
        try {
            MessageDigest sha2 = MessageDigest.getInstance("SHA-" + hashLen);
            byte[] buff = sha2.digest(input);
            sha2str = SecurityUtil.byte2HexStr(buff);
        } catch (Exception e) {
            logger.warn("Calculate SHA2 failed", e);
        }
        return sha2str;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SHA2"};
    }
}
