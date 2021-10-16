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
import com.alibaba.polardbx.optimizer.json.JsonContainProcessor;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @author wuheng.zxy 2016-5-21 上午11:27:28
 * JSON_CONTAINS(target, candidate[, path])
 * <p>
 * target中指定path处是否包含指定数据candidate, 是返回1, 否返回0. 规则如下:
 * <p><ul>
 * <li> 当target和candidate均为标量, 只有它们是可比较并且相等时, 才返回1
 * <li> 当target和candidate均为数组, 只有candidate的每个元素被target中某个元素包含时, 才返回1
 * <li> 当target为数组而candidate非数组, 只有candidate被target中某个元素包含时, 才返回1
 * <li> 当target和candidate均为对象, 只有当candidate每个键在target的键集合中存在, 且该键对应的值被包含于target键对应的值时, 才返回1
 * </ul>
 * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#function_json-contains">
 * json-contains</a>
 */
public class JsonContains extends JsonExtraFunction {
    public JsonContains(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_CONTAINS"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != 2 && args.length != 3) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Object target, candidate;
        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        try {
            target = JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        String candidateDoc = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
        try {
            candidate = JSON.parse(candidateDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(2, "JSON_CONTAINS");
        }

        if (args.length == 3) {
            String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);
            try {
                JsonPathExprStatement jsonPathStmt = parseJsonPathExpr(jsonPathExpr);
                target = JsonDocProcessor.extract(jsonDoc, jsonPathStmt);
            } catch (Exception e) {
                throw new JsonParserException(e, "Invalid JSON path expression.");
            }
        }

        return JsonContainProcessor.contains(target, candidate);
    }
}
