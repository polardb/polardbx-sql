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
import com.alibaba.polardbx.optimizer.json.JSONConstants;
import com.alibaba.polardbx.optimizer.json.JsonUtil;

import java.util.List;

/**
 * JSON_UNQUOTE(json_val)
 * <p>
 * Unquotes JSON value and returns the result as a utf8mb4 string. Returns NULL if the argument is NULL.
 * An error occurs if the value starts and ends with double quotes but is not a valid JSON string literal.
 * <p>
 * Within a string, certain sequences have special meaning unless the NO_BACKSLASH_ESCAPES SQL mode is enabled.
 * Each of these sequences begins with a backslash (\), known as the escape character.
 * MySQL recognizes the escape sequences shown in Table 12.21, “JSON_UNQUOTE() Special Character Escape Sequences”.
 * For all other escape sequences, backslash is ignored. That is, the escaped character is interpreted as if it was not escaped.
 * For example, \x is just x. These sequences are case sensitive. For example, \b is interpreted as a backspace,
 * but \B is interpreted as B.
 * <p>
 * Table 12.21 JSON_UNQUOTE() Special Character Escape Sequences
 * <p>
 * ---------------------------------------------------------
 * │ Escape Sequence │ Character Represented by Sequence   │
 * ---------------------------------------------------------
 * │       \"	     │  A double quote (") character       │
 * │       \b  	     │  A backspace character              │
 * │       \f	     │  A formfeed character               │
 * │       \n	     │  A newline (linefeed) character     │
 * │       \r	     │  A carriage return character        │
 * │       \t	     │  A tab character                    │
 * │       \\	     │  A backslash (\) character          │
 * |  \u0000-\uFFFF  |  UTF-8 bytes for Unicode value XXXX |
 * ---------------------------------------------------------
 *
 * @author wuheng.zxy 2016-5-21 上午11:59:18
 * @author arnkore 2017-07-18 18:09
 */
public class JsonUnquote extends JsonExtraFunction {
    public JsonUnquote(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_UNQUOTE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object quotedStr = args[0];

        if (quotedStr == null) {
            return null;
        }

        String unquotedStr =
            JsonUtil.unquote(DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, quotedStr));

        return unquotedStr;
    }
}
