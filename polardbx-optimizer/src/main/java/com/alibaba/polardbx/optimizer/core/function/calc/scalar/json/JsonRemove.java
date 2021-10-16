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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JSONConstants;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.List;

/**
 * JSON_REMOVE(json_doc, path[, path] ...)
 * <p>
 * Removes data from a JSON document and returns the result. Returns NULL if any argument is NULL.
 * An error occurs if the json_doc argument is not a valid JSON document or
 * any path argument is not a valid path expression or is $ or contains a * or ** wildcard.
 * <p>
 * The path arguments are evaluated left to right. The document produced by evaluating one path
 * becomes the new value against which the next path is evaluated.
 * <p>
 * It is not an error if the element to be removed does not exist in the document; in that case,
 * the path does not affect the document.
 *
 * @author wuheng.zxy 2016-5-21 上午11:47:25
 * @author arnkore 2017-07-24
 */
public class JsonRemove extends JsonExtraFunction {
    public JsonRemove(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_REMOVE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        // Returns NULL if any argument is NULL
        for (Object arg : args) {
            if (arg == null) {
                throw new JsonParserException("JSON documents may not contain NULL member names.");
            }
        }

        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        try {
            JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        List<JsonPathExprStatement> jsonPathExprs = Lists.newArrayList();
        // any path argument is not a valid path expression or is $ or contains a * or ** wildcard.
        for (int i = 1; i < args.length; i++) {
            String jsonPathExpr = DataTypes.StringType.convertFrom(args[i]);

            if (jsonPathExpr.equals(JSONConstants.SCOPE_PREFIX)) {
                throw new JsonParserException("The path expression '$' is not allowed in this context.");
            }

            if (jsonPathExpr.contains(JSONConstants.WILDCARD)) {
                throw JsonParserException.illegalAsterisk();
            }

            try {
                jsonPathExprs.add(parseJsonPathExpr(jsonPathExpr));
            } catch (Exception e) {
                throw new JsonParserException(e, "Invalid JSON path expression.");
            }
        }

        return JsonDocProcessor.remove(jsonDoc, jsonPathExprs);
    }
}
