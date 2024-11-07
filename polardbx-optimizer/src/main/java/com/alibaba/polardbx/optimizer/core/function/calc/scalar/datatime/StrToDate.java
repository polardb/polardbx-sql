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

import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @author jianghang 2014-4-17 上午12:49:10
 * @since 5.0.7
 */
public class StrToDate extends AbstractScalarFunction {
    public StrToDate(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public StrToDate() {
        super(null, null);
    }

    private final static long dayMs = 1000 * 60 * 60 * 24;

    private int cachedFlags = 0;
    private byte[] formatAsBytes;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        // parse sql mode
        if (cachedFlags == 0) {
            cachedFlags |= TimeParserFlags.FLAG_TIME_FUZZY_DATE;

            String sqlMode = ec.getSqlMode();
            if (sqlMode != null && !sqlMode.isEmpty()) {
                if (sqlMode.contains(SQLMode.NO_ZERO_IN_DATE.name())) {
                    cachedFlags |= TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE;
                }
                if (sqlMode.contains(SQLMode.NO_ZERO_DATE.name())) {
                    cachedFlags |= TimeParserFlags.FLAG_TIME_NO_ZERO_DATE;
                }
                if (sqlMode.contains(SQLMode.ALLOW_INVALID_DATES.name())) {
                    cachedFlags |= TimeParserFlags.FLAG_TIME_NO_ZERO_DATE;
                }
            }
        }
        String timeObj = DataTypes.StringType.convertFrom(args[0]);
        if (formatAsBytes == null) {
            String formatObj = DataTypes.StringType.convertFrom(args[1]);
            formatAsBytes = formatObj.getBytes();
        }

        // parse format
        MysqlDateTime t = StringTimeParser.extractFormat(timeObj.getBytes(), formatAsBytes);
        if (t == null
            || ((TimeParserFlags.check(cachedFlags, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE))
            && (t.getYear() == 0L || t.getMonth() == 0L || t.getDay() == 0L))) {
            return null;
        }
        if (DataTypeUtil.equalsSemantically(resultType, DataTypes.TimeType) && t.getDay() != 0) {
            // we should add hours from day part to hour part to keep valid time value.
            long hour = t.getHour() + t.getDay() * 24;
            t.setHour(hour);
            t.setDay(0);
        }
        return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.DEFAULT_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"STR_TO_DATE"};
    }
}
