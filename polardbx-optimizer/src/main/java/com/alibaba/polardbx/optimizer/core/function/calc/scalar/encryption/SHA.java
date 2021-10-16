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
 * SHA等价于SHA1散列算法, 结果为160位
 *
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_sha">SHA</a>
 */
public class SHA extends AbstractScalarFunction {
    public SHA(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        byte[] input = DataTypeUtil.convert(operandTypes.get(0), DataTypes.BinaryType, args[0]);
        String sha1str = "";
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA1");
            byte[] buff = sha1.digest(input);
            sha1str = SecurityUtil.byte2HexStr(buff);
        } catch (Exception e) {
            logger.warn("Calculate SHA failed", e);
        }
        return sha1str;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SHA", "SHA1"};
    }
}
