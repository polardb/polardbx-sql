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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;

import java.sql.Types;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * UNIX_TIMESTAMP([date])
 * If UNIX_TIMESTAMP() is called with no date argument, it returns a Unix timestamp representing
 * seconds since '1970-01-01 00:00:00' UTC.
 * If UNIX_TIMESTAMP() is called with a date argument, it returns the value of the argument as
 * seconds since '1970-01-01 00:00:00' UTC. The server interprets date as a value in the
 * session time zone and converts it to an internal Unix timestamp value in UTC.
 */
public class UnixTimestamp extends AbstractScalarFunction {
    public UnixTimestamp(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        ZonedDateTime zonedDateTime;
        if (args.length == 0) {
            zonedDateTime = ZonedDateTime.now(InternalTimeZone.UTC_ZONE_ID);
            // get the epoch millis
            long epochSec = zonedDateTime.toInstant().getEpochSecond();

            return epochSec;
        } else {
            // parse mysql datetime
            Object timeObj = args[0];
            MysqlDateTime t =
                DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (t == null) {
                return 0L;
            }

            boolean isDateValid = MySQLTimeTypeUtil.isDateInvalid(
                t, t.getYear() != 0 || t.getMonth() != 0 || t.getDay() != 0, TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE);

            boolean isTimestampInvalid =! MySQLTimeTypeUtil.checkTimestampRange(t);

            if (isDateValid || isTimestampInvalid) {
                return 0L;
            }

            // get session time zone
            ZoneId zoneId;
            if (ec.getTimeZone() != null) {
                String id = ec.getTimeZone().getId();
                zoneId = ZoneId.of(id);
            } else {
                zoneId = ZoneId.systemDefault();
            }

            //  interprets date as a value in the session time zone
            //  and converts it to an internal Unix timestamp value in UTC.
            zonedDateTime = MySQLTimeTypeUtil.toZonedDatetime(t, zoneId);
            if (zonedDateTime == null) {
                return 0L;
            }

            long epochSec = zonedDateTime.toInstant().getEpochSecond();
            DecimalStructure dec = new DecimalStructure();
            DecimalConverter.longToDecimal(epochSec, dec);

            if (t.getSecondPart() > 0) {
                // second part <= 1000_000_000
                int rem = (int) t.getSecondPart();
                int bufPos = DecimalTypeBase.roundUp(dec.getIntegers());
                dec.setBuffValAt(bufPos, rem);
                dec.setFractions(6);
            }

            if (t.isNeg()) {
                dec.setNeg(true);
            }
            return new Decimal(dec);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UNIX_TIMESTAMP"};
    }
}
