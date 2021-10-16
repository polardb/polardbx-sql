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
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JSONConstants;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.List;

/**
 * JSON_EXTRACT(json_doc, path[, path] ...)
 * <p>
 * Returns data from a JSON document, selected from the parts of the document matched by the path arguments.
 * Returns NULL if any argument is NULL or no paths locate a value in the document.
 * An error occurs if the json_doc argument is not a valid JSON document or any path argument is not a valid path expression.
 * <p>
 * The return value consists of all values matched by the path arguments.
 * If it is possible that those arguments could return multiple values, the matched values are autowrapped as an array,
 * in the order corresponding to the paths that produced them. Otherwise, the return value is the single matched value.
 *
 * @author wuheng.zxy 2016-5-21 上午11:33:21
 * @author arnkore 2017-07-18 18:09
 */
public class JsonExtract extends JsonExtraFunction {
    public JsonExtract(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_EXTRACT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ConfigDataMode.isFastMock()) {
            return null;
        }
        if (args.length < 2) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }
        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);

        int pathCount = args.length - 1;
        Object[] resObjs = new Object[pathCount];
        for (int i = 0; i < pathCount; i++) {
            String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(i + 1), DataTypes.StringType, args[i + 1]);
            if (null == jsonPathExpr) {
                return null;
            }
            JsonPathExprStatement jsonPathStmt = parseJsonPathExpr(jsonPathExpr);
            if (jsonPathStmt == null) {
                return JSONConstants.NULL_VALUE;
            }
            Object resObj = JsonDocProcessor.extract(jsonDoc, jsonPathStmt);
            if (resObj == null) {
                return JSONConstants.NULL_VALUE;
            }
            resObjs[i] = resObj;
        }

        return pathCount == 1 ? JSON.toJSONString(resObjs[0]) : JSON.toJSONString(resObjs);
    }
}
