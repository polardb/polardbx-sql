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
import io.airlift.slice.Slice;

import java.util.List;

/**
 * JSON_SET(json_doc, path, val[, path, val] ...)
 * <p>
 * Inserts or updates data in a JSON document and returns the result.
 * Returns NULL if any argument is NULL or path, if given, does not locate an object.
 * An error occurs if the json_doc argument is not a valid JSON document
 * or any path argument is not a valid path expression or contains a * or ** wildcard.
 * <p>
 * The path-value pairs are evaluated left to right. The document produced by evaluating one pair
 * becomes the new value against which the next pair is evaluated.
 * <p>
 * A path-value pair for an existing path in the document overwrites the existing document value with the new value.
 * A path-value pair for a nonexisting path in the document adds the value to the document
 * if the path identifies one of these types of values:
 * <p>
 * A member not present in an existing object. The member is added to the object and associated with the new value.
 * <p>
 * A position past the end of an existing array. The array is extended with the new value.
 * If the existing value is not an array, it is autowrapped as an array, then extended with the new value.
 * <p>
 * Otherwise, a path-value pair for a nonexisting path in the document is ignored and has no effect.
 * <p>
 * The JSON_SET(), JSON_INSERT(), and JSON_REPLACE() functions are related:
 * <p>
 * JSON_SET() replaces existing values and adds nonexisting values.
 * <p>
 * JSON_INSERT() inserts values without replacing existing values.
 * <p>
 * JSON_REPLACE() replaces only existing values.
 *
 * @author wuheng.zxy 2016-5-21 上午11:54:04
 * @author arnkore 2017-07-19 16:53
 */
public class JsonSet extends JsonExtraFunction {
    public JsonSet(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_SET"};
    }

    /**
     * JSON_SET(json_doc, path, val[, path, val] ...)
     */
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        // Returns NULL if any argument is NULL
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        // 如果参数不是奇数个，报错。
        if (args.length % 2 != 1) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }

        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);

        try {
            JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        List<Pair<JsonPathExprStatement, Object>> pathValPairList = Lists.newArrayList();
        JsonPathExprStatement nextStmt = null;

        // any path argument is not a valid path expression or contains a * or ** wildcard.
        for (int i = 1; i < args.length; i++) {
            // 奇数参数是json path expression
            if (i % 2 == 1) {
                String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
                if (jsonPathExpr.contains(JSONConstants.WILDCARD)) {
                    throw JsonParserException.illegalAsterisk();
                }

                try {
                    nextStmt = parseJsonPathExpr(jsonPathExpr);
                } catch (Exception e) {
                    throw new JsonParserException(e, "Invalid JSON path expression.");
                }
            } else {
                Object nextVal = null;
                try {
                    // The value is a json
                    nextVal = JSON.parse(DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]));
                } catch (Exception e) {
                    // Ignore this error due to the value of json could be a string
                    if (args[i] instanceof Slice){
                        nextVal = DataTypeUtil.convert(getOperandType(i), DataTypes.StringType, args[i]);
                    }
                }
                if (nextVal == null) {
                    nextVal = args[i];
                }
                pathValPairList.add(new Pair(nextStmt, nextVal));
            }
        }

        return JsonDocProcessor.set(jsonDoc, pathValPairList);
    }
}
