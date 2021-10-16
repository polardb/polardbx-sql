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

import com.alibaba.polardbx.common.datatype.DivStructure;
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
 * Returns a time value calculated from the hour, minute, and second arguments.
 * As of MySQL 5.6.4, the second argument can have a fractional part.
 * <p>
 * MAKETIME(h,m,s) is a time function that calculates a time value
 * from the total number of hours, minutes, and seconds.
 * Result: Time value
 */
public class MakeTime extends AbstractScalarFunction {
    public MakeTime(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Long hour = DataTypes.LongType.convertFrom(args[0]);
        Long minute = DataTypes.LongType.convertFrom(args[1]);
        Double second = DataTypes.DoubleType.convertFrom(args[2]);

        if (hour == null || minute == null || second == null || minute < 0 || minute > 59) {
            return null;
        }

        DivStructure divStructure = DivStructure.fromDouble(second);
        long[] div = divStructure == null ? null : divStructure.getDiv();
        if (div == null || div[0] < 0 || div[0] > 59 || div[1] < 0) {
            return null;
        }

        MysqlDateTime t = new MysqlDateTime();
        t.setSqlType(Types.TIME);
        // todo can't handle overflows
        if (hour < 0) {
            t.setNeg(true);
        }

        t.setHour(hour < 0 ? -hour : hour);
        t.setMinute(minute);
        t.setSecond(div[0]);
        t.setSecondPart(div[1] / 1000 * 1000);

        // handle max time value
        if (!MySQLTimeTypeUtil.checkTimeRangeQuick(t)) {
            t.setHour(MySQLTimeTypeUtil.TIME_MAX_HOUR);
            t.setMinute(59);
            t.setSecond(59);
            t.setDay(0);
            t.setSecondPart(0);
        }

        t = MySQLTimeCalculator.timeAddNanoWithRound(t, (int) (div[1] % 1000));

        // handle max time value
        if (!MySQLTimeTypeUtil.checkTimeRangeQuick(t)) {
            t.setHour(MySQLTimeTypeUtil.TIME_MAX_HOUR);
            t.setMinute(59);
            t.setSecond(59);
        }

        return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.DEFAULT_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MAKETIME"};
    }
}
