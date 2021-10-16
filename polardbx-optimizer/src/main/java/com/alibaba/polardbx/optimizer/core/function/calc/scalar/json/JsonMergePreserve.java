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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JsonMergeProcessor;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * JSON_MERGE_PRESERVE(json_doc, json_doc[, json_doc] ...)
 * <p>合并遵循如下规则:
 * <p><ul>
 * <li> 相邻两个数组合并为一个数组, 两个队形合并为一个对象
 * <li> 标量会被视作单元素数组
 * <li> 对象和数组合并, 对象会被视作单元素数组
 * </ul>
 */
public class JsonMergePreserve extends JsonExtraFunction {
    public JsonMergePreserve(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_MERGE_PRESERVE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }
        if (FunctionUtils.atLeastOneElementsNull(args)) {
            return null;
        }

        Object[] mergeObjs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            String jsonArg = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            Object jsonTarget;
            try {
                jsonTarget = JSON.parse(jsonArg);
            } catch (Exception e) {
                throw JsonParserException.invalidArgInFunc(i + 1, getFunctionNames()[0]);
            }
            mergeObjs[i] = jsonTarget;
        }

        return JsonMergeProcessor.mergePreserve(mergeObjs);
    }
}
