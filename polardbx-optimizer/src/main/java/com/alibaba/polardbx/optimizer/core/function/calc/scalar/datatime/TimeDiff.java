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
 * TIMEDIFF(expr1,expr2)
 * TIMEDIFF() returns expr1 − expr2 expressed as a time value. expr1 and expr2 are time or
 * date-and-time expressions, but both must be of the same type.
 * The result returned by TIMEDIFF() is limited to the range allowed for TIME values. Alternatively,
 * you can use either of the functions TIMESTAMPDIFF() and UNIX_TIMESTAMP(), both of which
 * return integers.
 */
public class TimeDiff extends AbstractScalarFunction {
    public TimeDiff(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        Object timeObj1 = args[0];
        Object timeObj2 = args[1];

        if (DataTypeUtil.isTemporalTypeWithDate(operandTypes.get(0))
            && DataTypeUtil.equalsSemantically(operandTypes.get(1), DataTypes.TimeType)) {
            return null;
        }

        if (DataTypeUtil.isTemporalTypeWithDate(operandTypes.get(1))
            && DataTypeUtil.equalsSemantically(operandTypes.get(0), DataTypes.TimeType)) {
            return null;
        }

        MysqlDateTime t1, t2;
        if (DataTypeUtil.isTemporalTypeWithDate(operandTypes.get(0)) || DataTypeUtil
            .isTemporalTypeWithDate(operandTypes.get(1))) {
            // date or timestamp?
            t1 = DataTypeUtil.toMySQLDatetime(timeObj1);
            t2 = DataTypeUtil.toMySQLDatetime(timeObj2);
        } else {
            t1 = DataTypeUtil.toMySQLDatetime(timeObj1, Types.TIME);
            t2 = DataTypeUtil.toMySQLDatetime(timeObj2, Types.TIME);
        }
        if (t1 == null || t2 == null) {
            return null;
        }

        // Incompatible types
        if (t1.getSqlType() != t2.getSqlType()) {
            return null;
        }

        int sign = 1;
        if (t1.isNeg() != t2.isNeg()) {
            sign = -sign;
        }
        MysqlDateTime ret = new MysqlDateTime();
        MysqlDateTime diff = MySQLTimeCalculator.calTimeDiff(t1, t2, sign < 0);
        long diffSec = diff.getSecond();
        long diffMicro = diff.getSecondPart() / 1000;
        ret.setNeg(diff.isNeg());

        //  For MYSQL_TIMESTAMP_TIME only:
        //  If first argument was negative and diff between arguments
        //  is non-zero we need to swap sign to get proper result.
        if (t1.isNeg() && (diffSec != 0 || diffMicro != 0)) {
            // Swap sign of result
            ret.setNeg(!ret.isNeg());
        }

        // from second to time value
        ret.setSqlType(Types.TIME);
        ret.setYear(0);
        ret.setMonth(0);
        ret.setDay(0);
        ret.setHour(diffSec / 3600L);
        long tSec = diffSec % 3600L;
        ret.setMinute(tSec / 60L);
        ret.setSecond(tSec % 60L);
        ret.setSecondPart(diffMicro * 1000);

        // adjust time range
        if (!MySQLTimeTypeUtil.checkTimeRangeQuick(ret)) {
            ret.setDay(0);
            ret.setSecondPart(0);
            ret.setHour(MySQLTimeTypeUtil.TIME_MAX_HOUR);
            ret.setMinute(59);
            ret.setSecond(59);
        }

        return DataTypeUtil.fromMySQLDatetime(resultType, ret, InternalTimeZone.DEFAULT_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TIMEDIFF"};
    }
}
