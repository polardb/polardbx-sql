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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JSONConstants;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @author wuheng.zxy 2016-5-21 上午11:23:55
 * <p>
 * JSON_ARRAY_APPEND(json_doc, path, val[, path, val] ...)
 * <p>
 * 在json_doc指定path处（如果是object，会被视为array）追加val
 * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-array-append">
 * JSON_ARRAY_APPEND</a>
 */
public class JsonArrayAppend extends JsonExtraFunction {
    public JsonArrayAppend(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_ARRAY_APPEND"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 3 || args.length % 2 != 1) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }
        if (FunctionUtils.atLeastOneElementsNull(args)) {
            return null;
        }

        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        try {
            JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        List<Pair<JsonPathExprStatement, Object>> pathValPairList = Lists.newArrayList();
        JsonPathExprStatement nextStmt;

        for (int i = 1; i < args.length; i += 2) {
            String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            if (jsonPathExpr.contains(JSONConstants.WILDCARD)) {
                throw JsonParserException.illegalAsterisk();
            }
            try {
                nextStmt = parseJsonPathExpr(jsonPathExpr);
            } catch (Exception e) {
                throw new JsonParserException(e, "Invalid JSON path expression.");
            }

            Object nextVal = null;
            try {
                nextVal = JSON.parse(DataTypeUtil.convert(operandTypes.get(i + 1), DataTypes.StringType, args[i + 1]));
            } catch (Exception e) {
                // Ignore this error due to the value of json could be a string
            }
            if (nextVal == null) {
                nextVal = args[i + 1];
            }
            Object resObj = JsonDocProcessor.extract(jsonDoc, nextStmt);
            if (resObj == null) {
                continue;
            }
            JSONArray jsonArray;
            if (JsonUtil.isJsonArray(resObj)) {
                jsonArray = (JSONArray) resObj;
                jsonArray.add(nextVal);
            } else {
                List<Object> list = Lists.newArrayList(resObj, nextVal);
                ;
                jsonArray = new JSONArray(list);
            }
            pathValPairList.add(new Pair(nextStmt, jsonArray));
        }

        return JsonDocProcessor.set(jsonDoc, pathValPairList);
    }
}
