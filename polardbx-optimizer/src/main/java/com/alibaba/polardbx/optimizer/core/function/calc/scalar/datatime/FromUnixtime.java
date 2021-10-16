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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Returns a representation of the unix_timestamp argument as a value in
 * 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS format, depending on whether the
 * function is used in a string or numeric context. The value is expressed in
 * the current time zone. unix_timestamp is an internal timestamp value such as
 * is produced by the UNIX_TIMESTAMP() function. If format is given, the result
 * is formatted according to the format string, which is used the same way as
 * listed in the entry for the DATE_FORMAT() function.
 *
 * <pre>
 * mysql> SELECT FROM_UNIXTIME(1196440219);
 *         -> '2007-11-30 10:30:19'
 * mysql> SELECT FROM_UNIXTIME(1196440219) + 0;
 *         -> 20071130103019.000000
 * mysql> SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(),
 *     ->                      '%Y %D %M %h:%i:%s %x');
 *         -> '2007 30th November 10:30:59 2007'
 * </pre>
 * <p>
 * Note: If you use UNIX_TIMESTAMP() and FROM_UNIXTIME() to convert between
 * TIMESTAMP values and Unix timestamp values, the conversion is lossy because
 * the mapping is not one-to-one in both directions. For details, see the
 * description of the UNIX_TIMESTAMP() function.
 *
 * @author jianghang 2014-4-17 上午12:24:11
 * @since 5.0.7
 */
public class FromUnixtime extends AbstractScalarFunction {
    public FromUnixtime(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1) {
            GeneralUtil.nestedException("FromUnixtime must have at least one argument");
        }
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Long time = DataTypes.LongType.convertFrom(args[0]);
        java.sql.Timestamp timestamp = new java.sql.Timestamp(time * 1000);

        TimeZone timeZone = null;
        if (ec.getTimeZone() != null) {
            timeZone = ec.getTimeZone().getTimeZone();
        }
        if (args.length >= 2) {
            String format = DataTypes.StringType.convertFrom(args[1]);
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat(DateFormat
                    .convertToJavaDataFormat(DateFormat.predealing(DateFormat.computNotSuppotted(timestamp, format))),
                    Locale.ENGLISH);
                if (ec.getTimeZone() != null) {
                    dateFormat.setTimeZone(timeZone);
                }
                return dateFormat.format(timestamp);
            } catch (IllegalArgumentException e) {
                return format;
            }
        } else {
            return resultType.convertFrom(TimeZoneUtils.convertToDateTime(timestamp, timeZone));
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"FROM_UNIXTIME"};
    }
}
