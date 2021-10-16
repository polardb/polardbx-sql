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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wuheng.zxy 2016-5-21 上午11:49:55
 * JSON_SEARCH(json_doc, one_or_all, search_str[, escape_char[, path] ...])
 * <p>
 * 查询json_doc中匹配search_str的路径表达式
 * <p>参数one_or_all的取值:
 * <p><ul>
 * <li>'one': 返回第一个匹配的路径(具体哪个是第一个是不定的)
 * <li>'all': 返回所有匹配的路径
 * </ul>
 */
public class JsonSearch extends JsonExtraFunction {
    public JsonSearch(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_SEARCH"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 3) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }

        String jsonDoc = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        Object target;
        try {
            target = JsonUtil.parse(jsonDoc);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }

        String oneOrAll = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
        SearchMode searchMode = parseSearchMode(oneOrAll);

        String searchStr = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);

        char escapeChar = '\\';
        if (args.length > 3 && args[3] != null) {
            String arg3 = DataTypeUtil.convert(operandTypes.get(3), DataTypes.StringType, args[3]);
            if (arg3.length() > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_TARGET_STMT_UNEXPECTED_PARAM,
                    "Incorrect arguments to ESCAPE");
            } else if (arg3.length() == 1) {
                escapeChar = arg3.charAt(0);
            }
        }
        if (args.length < 5) {
            return JsonDocProcessor.search(target, searchMode, searchStr, escapeChar);
        }

        List<JsonPathExprStatement> pathStmts = new ArrayList<>(args.length - 4);
        for (int i = 4; i < args.length; i++) {
            String jsonPathExpr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            try {
                pathStmts.add(parseJsonPathExpr(jsonPathExpr));
            } catch (Exception e) {
                throw new JsonParserException(e, "Invalid JSON path expression.");
            }
        }

        return JsonDocProcessor.search(target, searchMode, searchStr,
            escapeChar, pathStmts);
    }
}
