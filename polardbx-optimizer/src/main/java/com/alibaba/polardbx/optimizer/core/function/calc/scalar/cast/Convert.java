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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.cast;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

/**
 * @author jianghang 2014-7-1 下午1:55:17
 * @since 5.1.7
 */
public class Convert extends Cast {
    public Convert(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        int extraIndex = 2;
        if (args.length == 4) {
            extraIndex = 3;
        }
        boolean isCharsetConvert = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(
            args[extraIndex]));
        if (isCharsetConvert) {
            String arg = DataTypes.StringType.convertFrom(args[0]);
            String charset = DataTypes.StringType.convertFrom(args[1]);
            try {
                return new String(args[0] instanceof byte[] ? (byte[]) args[0] : arg.getBytes(), charset);
            } catch (UnsupportedEncodingException e) {
                return arg;
            }
        } else {
            CastType castType = getType(Arrays.asList(args), 1);
            Object obj = castType.type.convertFrom(args[0]);
            return computeInner(castType, obj);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CONVERT"};
    }

}
