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
import com.alibaba.polardbx.optimizer.json.JSONConstants;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.List;

/**
 * JSON_KEYS(json_doc[, path])
 * <p>
 * Returns the keys from the top-level value of a JSON object as a JSON array, or, if a path argument is given,
 * the top-level keys from the selected path. Returns NULL if any argument is NULL,
 * the json_doc argument is not an object, or path, if given, does not locate an object.
 * An error occurs if the json_doc argument is not a valid JSON document or
 * the path argument is not a valid path expression or contains a * or ** wildcard.
 * <p>
 * The result array is empty if the selected object is empty. If the top-level value has nested subobjects,
 * the return value does not include keys from those subobjects.
 *
 * @author wuheng.zxy 2016-5-21 上午11:37:48
 * @author arnkore 2017-08-15
 */
public class JsonKeys extends JsonExtraFunction {
    public JsonKeys(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_KEYS"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != 1 && args.length != 2) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }

        // Returns NULL if any argument is NULL
        for (Object arg : args) {
            if (arg == null) {
                return null;
            }
        }

        Object jsonObj;
        try {
            String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
            jsonObj = JSON.parse(str);
            // Returns NULL if the json_doc argument is not an object
            if (!JsonUtil.isJsonObject(jsonObj)) {
                return null;
            }
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        JsonPathExprStatement jsonPathStmt = null;
        if (args.length == 2) {
            String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
            if (jsonPathExpr.contains(JSONConstants.WILDCARD)) {
                throw JsonParserException.illegalAsterisk();
            }

            try {
                jsonPathStmt = parseJsonPathExpr(jsonPathExpr);
            } catch (Exception e) {
                throw new JsonParserException(e, "Invalid JSON path expression.");
            }
        }

        return JsonDocProcessor.jsonKeys(jsonObj, jsonPathStmt);
    }
}
