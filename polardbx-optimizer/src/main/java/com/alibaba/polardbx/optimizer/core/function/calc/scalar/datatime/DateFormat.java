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

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import io.airlift.slice.Slices;

import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

/**
 * DATE_FORMAT(date,format)
 * Formats the date value according to the format string.
 * The specifiers shown in the following table may be used in the format string. The % character
 * is required before format specifier characters. The specifiers apply to other functions as well:
 * STR_TO_DATE(), TIME_FORMAT(), UNIX_TIMESTAMP().
 */
public class DateFormat extends AbstractScalarFunction {
    public DateFormat(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private SimpleDateFormat cachedFormat;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Object timeObj = args[0];
        MysqlDateTime t =
            DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_FUZZY_DATE);

        String format = DataTypes.StringType.convertFrom(args[1]);
        if (t == null || format == null || format.length() == 0) {
            return null;
        }

        byte[] res = StringTimeParser.makeFormat(t, format.getBytes());
        return res == null ? null : Slices.wrappedBuffer(res);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DATE_FORMAT"};
    }
}
