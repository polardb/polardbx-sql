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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JsonContainProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @author wuheng.zxy 2016-5-21 上午11:29:22
 * JSON_CONTAINS_PATH(json_doc, one_or_all, path[, path] ...)
 * <p>
 * 查询json_doc是否存在指定路径path, 是返回1, 否返回0.
 * <p>参数one_or_all的取值:
 * <p><ul>
 * <li>'one': path中只要有一个存在就返回1
 * <li>'all': 所有的path都存在才返回1
 * </ul>
 */
public class JsonContainsPath extends JsonExtraFunction {
    public JsonContainsPath(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_CONTAINS_PATH"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 3) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }
        if (FunctionUtils.atLeastOneElementsNull(args)) {
            return null;
        }

        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);

        try {
            JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw new JsonParserException("Invalid JSON text in argument 1 to function JSON_ARRAY_INSERT.");
        }

        String oneOrAll = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
        SearchMode searchMode = parseSearchMode(oneOrAll);

        JsonPathExprStatement nextStmt;
        boolean contains = false;
        for (int i = 2; i < args.length; i++) {
            String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);

            try {
                nextStmt = parseJsonPathExpr(jsonPathExpr);
            } catch (Exception e) {
                throw new JsonParserException(e, "Invalid JSON path expression.");
            }

            boolean containsCurPath = JsonContainProcessor.containsPath(jsonDoc, nextStmt);
            if (searchMode == SearchMode.ONE && containsCurPath) {
                return true;
            }
            if (searchMode == SearchMode.ALL) {
                if (!containsCurPath) {
                    return false;
                } else {
                    contains = true;
                }
            }
        }

        return contains;
    }
}
