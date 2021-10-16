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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.AbstractPathLeg;
import com.alibaba.polardbx.optimizer.json.ArrayLocation;
import com.alibaba.polardbx.optimizer.json.JSONConstants;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @author wuheng.zxy 2016-5-21 上午11:26:04
 * JSON_INSERT(json_doc, path, val[, path, val] ...)
 * <p>
 * 在Array型的json_doc指定path处, 插入val; 若path不存在, 则忽略.
 * <p>
 * 如果path超出Array范围，默认在最后插入.
 */
public class JsonArrayInsert extends JsonExtraFunction {
    public JsonArrayInsert(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_ARRAY_INSERT"};
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
        Object jsonObj;
        try {
            jsonObj = JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

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
            List<AbstractPathLeg> pathLegList = nextStmt.getPathLegs();
            if (!(pathLegList.get(pathLegList.size() - 1) instanceof ArrayLocation)) {
                throw new JsonParserException("A path expression is not a path to a cell in an array.");
            }
            int insertPos = ((ArrayLocation) pathLegList.get(pathLegList.size() - 1)).getArrayIndex();
            pathLegList.remove(pathLegList.size() - 1);
            Object resObj = JsonDocProcessor.extract(jsonObj, nextStmt);
            if (!JsonUtil.isJsonArray(resObj)) {
                continue;
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
            JSONArray jsonArray = (JSONArray) resObj;
            if (insertPos <= jsonArray.size()) {
                jsonArray.add(insertPos, nextVal);
            } else {
                jsonArray.add(nextVal);
            }

            jsonObj = JsonDocProcessor.set(jsonObj, nextStmt, jsonArray);
        }

        return JSON.toJSONString(jsonObj);
    }
}
