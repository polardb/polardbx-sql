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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.json;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.List;

/**
 * 返回JSON二进制形式的字节大小
 */
public class JsonStorageSize extends JsonExtraFunction {
    public JsonStorageSize(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_STORAGE_SIZE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != 1) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }

        if (args[0] == null) {
            return null;
        }

        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        Object target;

        try {
            target = JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        return JsonUtil.getStorageSize(target, jsonDoc);
    }
}
