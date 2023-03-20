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
import com.alibaba.polardbx.common.utils.Pair;
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
 * JSON_REPLACE(json_doc, path, val[, path, val] ...)
 * <p>
 * Replaces existing values in a JSON document and returns the result. Returns NULL if any argument is NULL.
 * An error occurs if the json_doc argument is not a valid JSON document or any path argument is not a valid path expression
 * or contains a * or ** wildcard.
 * <p>
 * The path-value pairs are evaluated left to right. The document produced by evaluating one pair
 * becomes the new value against which the next pair is evaluated.
 * <p>
 * A path-value pair for an existing path in the document overwrites the existing document value with the new value.
 * A path-value pair for a nonexisting path in the document is ignored and has no effect.
 *
 * @author wuheng.zxy 2016-5-21 上午11:48:58
 * @author arnkore 2017-07-20
 */
public class JsonReplace extends JsonExtraFunction {
    public JsonReplace(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_REPLACE"};
    }

    /**
     * JSON_REPLACE(json_doc, path, val[, path, val] ...)
     */
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        // 如果参数不是奇数个，报错。
        if (args.length % 2 != 1 || args.length == 1) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }

        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        if (jsonDoc == null) {
            return null;
        }
        try {
            JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        List<Pair<JsonPathExprStatement, Object>> pathValPairList = Lists.newArrayList();
        JsonPathExprStatement nextStmt = null;
        Object nextVal = null;

        // any path argument is not a valid path expression or contains a * or ** wildcard.
        for (int i = 1; i < args.length; i++) {
            if (i % 2 == 1) { // 奇数参数是json path expression
                String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
                if (jsonPathExpr == null) {
                    return null;
                }
                if (jsonPathExpr.contains(JSONConstants.WILDCARD)) {
                    throw JsonParserException.illegalAsterisk();
                }

                try {
                    nextStmt = parseJsonPathExpr(jsonPathExpr);
                } catch (Exception e) {
                    throw new JsonParserException(e, "Invalid JSON path expression.");
                }
            } else {
                nextVal = args[i];
                pathValPairList.add(new Pair<>(nextStmt, nextVal));
            }
        }

        return JsonDocProcessor.replace(jsonDoc, pathValPairList);
    }
}
