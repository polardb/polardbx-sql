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

import java.util.List;

/**
 * JSON_QUOTE(string)
 * <p>
 * Quotes a string as a JSON value by wrapping it with double quote characters and escaping interior quote and other characters,
 * then returning the result as a utf8mb4 string. Returns NULL if the argument is NULL.
 * <p>
 * This function is typically used to produce a valid JSON string literal for inclusion within a JSON document.
 * <p>
 * Certain special characters are escaped with backslashes per the escape sequences shown in Table 12.21,
 * “JSON_UNQUOTE() Special Character Escape Sequences”.
 *
 * @author wuheng.zxy 2016-5-21 上午11:43:55
 * @author arnkore 2017-08-15
 */
public class JsonQuote extends JsonExtraFunction {
    public JsonQuote(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_QUOTE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object unquotedStr = args[0];

        if (unquotedStr == null || unquotedStr == JSONConstants.NULL_VALUE) {
            return JSONConstants.NULL_VALUE;
        }

        return JSON.toJSONString(DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]));
    }
}
