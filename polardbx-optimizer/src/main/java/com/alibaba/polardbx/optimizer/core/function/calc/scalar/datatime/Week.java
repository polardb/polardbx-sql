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

import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.List;

/**
 * WEEK(date[,mode])
 * This function returns the week number for date. The two-argument form of WEEK() enables you
 * to specify whether the week starts on Sunday or Monday and whether the return value should
 * be in the range from 0 to 53 or from 1 to 53. If the mode argument is omitted, the value of the
 * default_week_format system variable is used.
 * mode = 0 ~ 7
 */
public class Week extends AbstractScalarFunction {
    public Week(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"WEEK"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        // parse datetime
        Object timeObj = args[0];
        MysqlDateTime t = DataTypeUtil.toMySQLDatetimeByFlags(
            timeObj,
            Types.TIMESTAMP,
            TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);
        if (t == null) {
            return null;
        }

        // get week mode
        int weekMode;
        if (args.length == 1) {
            weekMode = defaultWeekMode() > 0 ? defaultWeekMode() : 0;
        } else {
            weekMode = DataTypes.IntegerType.convertFrom(args[1]);
        }
        final int mode = MySQLTimeConverter.weekMode(weekMode);

        // calc week
        long[] ret = MySQLTimeConverter.datetimeToWeek(t, mode);
        if (ret == null) {
            return null;
        } else {
            return ret[0];
        }
    }

    protected int defaultWeekMode() {
        return -1;
    }
}
