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

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.List;

/**
 * ADDTIME() adds expr2 to expr1 and returns the result. expr1 is a time or
 * datetime expression, and expr2 is a time expression.
 */
public class AddTime extends AbstractScalarFunction {
    public AddTime(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        // get mysql datetime
        MysqlDateTime t1;
        MysqlDateTime t2;

        boolean isTime = false;
        if (DataTypeUtil.equalsSemantically(resultType, DataTypes.DatetimeType)) {
            t1 = DataTypeUtil.toMySQLDatetime(args[0]);
            t2 = DataTypeUtil.toMySQLDatetime(args[1], Types.TIME);
            // for datetime
            if (t1 == null || t2 == null
                || t1.getSqlType() == Types.TIME
                || t2.getSqlType() != Types.TIME) {
                return null;
            }
        } else {
            t1 = DataTypeUtil.toMySQLDatetime(args[0], Types.TIME);
            t2 = DataTypeUtil.toMySQLDatetime(args[1], Types.TIME);
            if (t1 == null || t2 == null
                || t2.getSqlType() == Types.TIMESTAMP
                || t2.getSqlType() == MySQLTimeTypeUtil.DATETIME_SQL_TYPE) {
                return null;
            }
            isTime = t1.getSqlType() == Types.TIME;
        }
        // decide to sub or add.
        int sign = getInitialSign();
        if (t1.isNeg() != t2.isNeg()) {
            sign = -sign;
        }

        MysqlDateTime ret = new MysqlDateTime();
        MysqlDateTime diff = MySQLTimeCalculator.calTimeDiff(t1, t2, -sign < 0);
        long diffSec = diff.getSecond();
        long diffMicro = diff.getSecondPart() / 1000;
        ret.setNeg(diff.isNeg());

        //   If first argument was negative and diff between arguments
        //   is non-zero we need to swap sign to get proper result.
        if (t1.isNeg() && (diffSec != 0 || diffMicro != 0)) {
            // Swap sign of result
            ret.setNeg(!ret.isNeg());
        }

        if (!isTime && ret.isNeg()) {
            return null;
        }

        // from second to time value
        long days = diffSec / (3600 * 24L);
        long sec = diffSec % (3600 * 24L);
        ret.setSqlType(Types.TIME);
        ret.setYear(0);
        ret.setMonth(0);
        ret.setDay(0);
        ret.setHour(sec / 3600L);
        long tSec = sec % 3600L;
        ret.setMinute(tSec / 60L);
        ret.setSecond(tSec % 60L);
        ret.setSecondPart(diffMicro * 1000);

        if (!isTime) {
            // day number to year / month / day
            MySQLTimeCalculator.getDateFromDayNumber(days, ret);
            ret.setSqlType(Types.TIMESTAMP);

            if (MySQLTimeTypeUtil.isDatetimeRangeInvalid(ret)) {
                // Value is out of range, cannot use our printing functions to output it.
                return null;
            }
            if (ret.getDay() != 0) {
                return DataTypeUtil.fromMySQLDatetime(resultType, ret, InternalTimeZone.DEFAULT_TIME_ZONE);
            }
            return null;
        }

        ret.setSqlType(Types.TIME);
        ret.setHour(ret.getHour() + days * 24);
        if (!MySQLTimeTypeUtil.checkTimeRangeQuick(ret)) {
            ret.setDay(0);
            ret.setSecondPart(0);
            ret.setHour(MySQLTimeTypeUtil.TIME_MAX_HOUR);
            ret.setMinute(59);
            ret.setSecond(59);
        }
        // convert mysql datetime to proper type.
        return DataTypeUtil.fromMySQLDatetime(resultType, ret, InternalTimeZone.DEFAULT_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ADDTIME"};
    }

    /**
     * The sign of function, 1 means add and -1 means sub.
     */
    protected int getInitialSign() {
        return 1;
    }
}
