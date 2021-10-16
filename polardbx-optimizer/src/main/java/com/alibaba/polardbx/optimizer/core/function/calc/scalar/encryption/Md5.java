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
 * Created by chuanqin on 17/12/20.
 */
public class Md5 extends AbstractScalarFunction {
    public Md5(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, DataTypes.StringType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object arg = args[0];

        if (FunctionUtils.isNull(arg)) {
            return null;
        }
        byte[] input = DataTypeUtil.convert(operandTypes.get(0), DataTypes.BinaryType, args[0]);
        String md5str = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            // calculate 128 bit message digest
            byte[] buff = md.digest(input);
            // 4 把数组每一字节（一个字节占八位）换成16进制连成md5字符串
            md5str = SecurityUtil.byte2HexStr(buff);

        } catch (Exception e) {
            logger.warn("Calculate MD5 failed", e);
        }
        return md5str;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MD5"};
    }
}
