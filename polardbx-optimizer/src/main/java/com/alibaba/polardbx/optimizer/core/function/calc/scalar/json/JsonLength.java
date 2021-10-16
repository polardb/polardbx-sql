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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.List;

/**
 * @author wuheng.zxy 2016-5-21 上午11:40:01
 * JSON_LENGTH(json_doc[, path])
 * <p><ul>
 * <li> 标量长度为1
 * <li> 数组长度为元素个数
 * <li> 对象长度为成员个数
 * <li> 长度不嵌套计算
 * </ul>
 */
public class JsonLength extends JsonExtraFunction {
    public JsonLength(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_LENGTH"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != 1 && args.length != 2) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }
        Object target;
        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        try {
            target = JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        if (args.length == 2) {
            String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
            try {
                JsonPathExprStatement jsonPathStmt = parseJsonPathExpr(jsonPathExpr);
                target = JsonDocProcessor.extract(jsonDoc, jsonPathStmt);
            } catch (Exception e) {
                throw new JsonParserException(e, "Invalid JSON path expression.");
            }
        }

        if (target instanceof JSONArray) {
            return ((JSONArray) target).size();
        }
        if (target instanceof JSONObject) {
            return ((JSONObject) target).size();
        }
        return 1;
    }
}
