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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Returns a format string. This function is useful in combination with the
 * DATE_FORMAT() and the STR_TO_DATE() functions. The possible values for the
 * first and second arguments result in several possible format strings (for the
 * specifiers used, see the table in the DATE_FORMAT() function description).
 * ISO format refers to ISO 9075, not ISO 8601.
 *
 * @author jianghang 2014-4-17 上午11:03:40
 * @since 5.0.7
 */
public class GetFormat extends AbstractScalarFunction {
    public GetFormat(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public static Map<String, String> formats = new HashMap<String, String>();

    static {
        formats.put("DATE_USA", "%m.%d.%Y");
        formats.put("DATE_JIS", "%Y-%m-%d");
        formats.put("DATE_ISO", "%Y-%m-%d");
        formats.put("DATE_EUR", "%d.%m.%Y");
        formats.put("DATE_INTERNAL", "%Y%m%d");
        formats.put("TIMESTAMP_USA", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_JIS", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_ISO", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_EUR", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_INTERNAL", "%Y%m%d%H%i%s");
        formats.put("TIME_USA", "%h:%i:%s %p");
        formats.put("TIME_JIS", "%H:%i:%s");
        formats.put("TIME_ISO", "%H:%i:%s");
        formats.put("TIME_EUR", "%H:%i:%s");
        formats.put("TIME_INTERNAL", "%H%i%s");
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        String type = DataTypes.StringType.convertFrom(args[0]);
        String format = DataTypes.StringType.convertFrom(args[1]);
        return formats.get(type + "_" + format);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"GET_FORMAT"};
    }
}
