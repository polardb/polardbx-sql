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
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

/**
 * CONVERT_TZ(dt,from_tz,to_tz)
 * CONVERT_TZ() converts a datetime value dt from the time zone given by from_tz to the time
 * zone given by to_tz and returns the resulting value.
 */
public class ConvertTz extends AbstractScalarFunction {
    public ConvertTz(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private ZoneId cachedFromZoneId;
    private ZoneId cachedToZoneId;
    private TimeZone cachedToTimezone;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }

        }
        if (cachedFromZoneId == null || cachedToZoneId == null) {
            String fromTimezoneId = DataTypes.StringType.convertFrom(args[1]);
            String toTimezoneId = DataTypes.StringType.convertFrom(args[2]);

            cachedFromZoneId = TimeZoneUtils.zoneIdOf(fromTimezoneId);
            cachedToZoneId = TimeZoneUtils.zoneIdOf(toTimezoneId);

            if (cachedFromZoneId == null || cachedToZoneId == null) {
                return null;
            }
            cachedToTimezone = Optional.of(cachedToZoneId)
                .map(ZoneId::getId)
                .map(TimeZoneUtils::convertFromMySqlTZ)
                .map(InternalTimeZone::getTimeZone)
                .orElse(InternalTimeZone.DEFAULT_TIME_ZONE);
        }
        ZoneId fromTz = cachedFromZoneId;
        ZoneId toTz = cachedToZoneId;

        Object timeObj = args[0];
        MysqlDateTime t =
            DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);

        // interpret the datetime to From_Timezone.
        ZonedDateTime zonedDateTime = MySQLTimeTypeUtil.toZonedDatetime(t, fromTz);
        if (zonedDateTime == null) {
            return null;
        }

        // convert to To_Timezone
        zonedDateTime = zonedDateTime.withZoneSameInstant(toTz);
        MysqlDateTime ret = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);

        return DataTypeUtil.fromMySQLDatetime(resultType, ret, cachedToTimezone);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CONVERT_TZ"};
    }
}
