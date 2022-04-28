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

package com.alibaba.polardbx.common.utils.timezone;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class TimestampUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampUtils.class);

    public static final String TYPE_NAME_TIMESTAMP = "timestamp";
    public static final String PATTERN_TIMESTAMP = "yyyy-MM-dd HH:mm:ss";
    public static final String FIXED_GMT_IN_META = "GMT+8:00";

    public static final Pattern PATTERN_ZERO_VALUE = Pattern.compile("^0000-00-00 00:00:00\\.?0*$");

    public static final Set<String> FIXED_GMT_FORMATS_SUPPORTED = new HashSet<>();

    static {
        FIXED_GMT_FORMATS_SUPPORTED.add("+8:00");
        FIXED_GMT_FORMATS_SUPPORTED.add("+08:00");
        FIXED_GMT_FORMATS_SUPPORTED.add("GMT+8:00");
        FIXED_GMT_FORMATS_SUPPORTED.add("GMT+08:00");
    }

    public static boolean needTimeZoneConversion(TimeZone timeZone) {
        return timeZone != null && needTimeZoneConversion(timeZone.getID());
    }

    public static boolean needTimeZoneConversion(String timeZoneId) {
        return TStringUtil.isNotBlank(timeZoneId) &&
            !FIXED_GMT_FORMATS_SUPPORTED.contains(timeZoneId.toUpperCase());
    }

    public static boolean isZeroValue(String timestampString) {
        // A zero value doesn't need to be converted.
        return PATTERN_ZERO_VALUE.matcher(timestampString).matches();
    }

    public static String convertFromGMT8(String timestampString, TimeZone toTimeZone, long precision) {
        return convertBetweenTimeZones(timestampString, null, toTimeZone, precision);
    }

    public static String convertToGMT8(String timestampString, TimeZone fromTimeZone, long precision) {
        return convertBetweenTimeZones(timestampString, fromTimeZone, null, precision);
    }

    private static String convertBetweenTimeZones(String timestampString, TimeZone fromTimeZone, TimeZone toTimeZone,
                                                  long precision) {
        boolean needTimeZoneConversion = needTimeZoneConversion(fromTimeZone) || needTimeZoneConversion(toTimeZone);
        if (!needTimeZoneConversion ||
            TStringUtil.isBlank(timestampString) ||
            isZeroValue(timestampString)) {
            // Don't need conversion.
            return timestampString;
        }

        MysqlDateTime mysqlDateTime = StringTimeParser.parseDatetime(timestampString.getBytes());
        if (mysqlDateTime == null) {
            // Failed to parse for some reason, so return original timestamp string.
            return timestampString;
        }

        TimeZone fixedTimeZone = TimeZone.getTimeZone(FIXED_GMT_IN_META);

        if (fromTimeZone == null) {
            fromTimeZone = fixedTimeZone;
        }

        if (toTimeZone == null) {
            toTimeZone = fixedTimeZone;
        }

        try {
            return convertBetweenTimeZones(mysqlDateTime, fromTimeZone, toTimeZone, precision);
        } catch (Exception e) {
            LOGGER.error(String.format("Failed to convert %s from %s to %s. Caused by: %s",
                timestampString, fromTimeZone.getID(), toTimeZone.getID(), e.getMessage()), e);
            return timestampString;
        }
    }

    /**
     * Use a timestamp string without fractional part for time zone conversion,
     * i.e. only keep yyyy-MM-dd HH:mm:ss, because JDBC and MySQL interpret the
     * fractional part differently: milliseconds in JDBC vs. microseconds in MySQL
     * For example, the fractional part of xx:xx:xx.123000 is 123000:
     * JDBC interprets it as 123000 milliseconds, i.e. 123 seconds, while
     * MySQL interprets it as 12300 microseconds, i.e. 123 milliseconds.
     * Therefore, we have to handle the fractional part separately during time zone conversion.
     */
    private static String convertBetweenTimeZones(MysqlDateTime mysqlDateTime, TimeZone fromTimeZone,
                                                  TimeZone toTimeZone, long precision)
        throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(PATTERN_TIMESTAMP);

        // Truncate the timestamp and leave the nanoseconds for use later.
        long nanoseconds = mysqlDateTime.getSecondPart();
        mysqlDateTime.setSecondPart(0);
        String timestampTruncated = mysqlDateTime.toDatetimeString(0);

        // Convert the timestamp string with the source time zone.
        if (fromTimeZone != null) {
            sdf.setTimeZone(fromTimeZone);
        }
        Date timestampConverted = sdf.parse(timestampTruncated);

        // Convert the timestamp to the target time zone.
        if (toTimeZone != null) {
            sdf.setTimeZone(toTimeZone);
        }
        String timestampStringConverted = sdf.format(timestampConverted);

        // Add the nanoseconds back.
        mysqlDateTime = StringTimeParser.parseDatetime(timestampStringConverted.getBytes());
        mysqlDateTime.setSecondPart(nanoseconds);

        // Fully converted timestamp string.
        return mysqlDateTime.toDatetimeString((int) precision);
    }

}
