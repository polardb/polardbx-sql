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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * UTC_TIMESTAMP, UTC_TIMESTAMP([fsp])
 * Returns the current UTC date and time as a value in 'YYYY-MM-DD hh:mm:ss' or
 * YYYYMMDDhhmmss format, depending on whether the function is used in string or numeric context.
 * If the fsp argument is given to specify a fractional seconds precision from 0 to 6, the return value
 * includes a fractional seconds part of that many digits.
 */
public class UtcTimestamp extends AbstractScalarFunction {
    public UtcTimestamp(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        FunctionUtils.checkFsp(args);

        // get scale
        int scale = resultType.getScale();

        // get zoned now datetime
        ZonedDateTime zonedDateTime = ZonedDateTime.now(InternalTimeZone.UTC_ZONE_ID);

        // round to scale.
        MysqlDateTime t = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);
        t = MySQLTimeCalculator.roundDatetime(t, scale);
        return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.UTC_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UTC_TIMESTAMP"};
    }
}
