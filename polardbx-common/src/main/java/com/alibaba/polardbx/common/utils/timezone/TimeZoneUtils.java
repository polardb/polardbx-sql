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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TimeZoneUtils {

    private static final Set<String> availableTimeZones = new HashSet<>();
    private static final Pattern timeZoneOffsetPattern = Pattern.compile("^([\\+|-])([0-1]?[0-9]):([0|3])0$");

    static {
        for (String id : TimeZone.getAvailableIDs()) {
            availableTimeZones.add(id.toUpperCase());
        }
    }

    public static InternalTimeZone convertFromMySqlTZ(String timeZoneStr) {
        if (timeZoneStr == null) {
            return null;
        }
        String timeZoneId = timeZoneStr.toUpperCase();
        Matcher m = timeZoneOffsetPattern.matcher(timeZoneId);
        if ("SYSTEM".equalsIgnoreCase(timeZoneId)) {
            return InternalTimeZone.defaultTimeZone;
        }
        if ("NULL".equalsIgnoreCase(timeZoneId)) {
            return null;
        } else if (m.find()) {
            String offsetStr = m.group(1) + m.group(2);
            int hourOffset = Integer.parseInt(offsetStr);
            int minOffset = Integer.parseInt(m.group(3)) * 10 + hourOffset * 60;

            if (minOffset >= -12 * 60 - 30 && minOffset <= 13 * 60 + 30) {
                timeZoneId = "GMT" + timeZoneStr;
                return InternalTimeZone.createTimeZone(timeZoneStr, TimeZone.getTimeZone(timeZoneId));
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TZ, timeZoneId);
            }
        } else if (availableTimeZones.contains(timeZoneId)) {
            return InternalTimeZone.createTimeZone(timeZoneStr, TimeZone.getTimeZone(timeZoneId));
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TZ, timeZoneId);
        }
    }

    public static String convertToDateTimeWithMills(Timestamp timestamp, TimeZone timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
        if (timeZone != null) {
            sdf.setTimeZone(timeZone);
        }
        return sdf.format(timestamp);
    }

    public static String convertToDateTime(Timestamp timestamp, TimeZone timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (timeZone != null) {
            sdf.setTimeZone(timeZone);
        }
        return sdf.format(timestamp);
    }

    public static String convertToDateTimeWithMilliseconds(Timestamp timestamp, TimeZone timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        if (timeZone != null) {
            sdf.setTimeZone(timeZone);
        }
        return sdf.format(timestamp);
    }

    public static String convertToDate(Timestamp timestamp, TimeZone timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        if (timeZone != null) {
            sdf.setTimeZone(timeZone);
        }
        return sdf.format(timestamp);
    }

    public static String convertToTime(Timestamp timestamp, TimeZone timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        if (timeZone != null) {
            sdf.setTimeZone(timeZone);
        }
        return sdf.format(timestamp);
    }

    public static String convertBetweenTimeZone(String dateTime, TimeZone fromTz, TimeZone toTz) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (fromTz != null) {
            sdf.setTimeZone(fromTz);
        }
        long timestamp = sdf.parse(dateTime).getTime();
        Timestamp actualTimestamp = new Timestamp(timestamp);
        return convertToDateTime(actualTimestamp, toTz);
    }

    public static Timestamp convertTimeZoneFromDate(String dateTime, TimeZone toTz) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (toTz != null) {
            sdf.setTimeZone(toTz);
        }
        long timestamp = sdf.parse(dateTime).getTime();
        Timestamp actualTimestamp = new Timestamp(timestamp);
        return actualTimestamp;
    }

    public static String convertBetweenTimeZone(Timestamp unrealTimestamp, TimeZone fromTz, TimeZone toTz)
        throws ParseException {

        TimeZone sysDefaultTimeZone = Calendar.getInstance().getTimeZone();
        long sysOffset = sysDefaultTimeZone.getRawOffset();
        long fromOffset = fromTz.getRawOffset();
        long deltaTimeMillis = sysOffset - fromOffset;
        long realTimestampInMills = unrealTimestamp.getTime() + deltaTimeMillis;
        Timestamp actualTimestamp = new Timestamp(realTimestampInMills);
        return convertToDateTime(actualTimestamp, toTz);
    }

    public static ZoneId zoneIdOf(String id) {
        if (id == null) {
            return null;
        }

        if ("SYSTEM".equalsIgnoreCase(id)) {
            return ZoneId.systemDefault();
        } else if ("CST".equalsIgnoreCase(id)) {
            return ZoneId.of("GMT+08:00");
        } else if ((id.charAt(0) == '+' || id.charAt(0) == '-') && id.length() != 6) {
            String toParse = id.charAt(0) == '+' ? id.substring(1) : id;
            MysqlDateTime time = StringTimeParser.parseString(toParse.getBytes(), Types.TIME);
            if (time == null) {
                return null;
            }
            String normalized = time.toTimeString(0);
            id = id.charAt(0) == '+' ? ("+" + normalized) : normalized;
        }

        ZoneId zoneId;
        try {
            zoneId = ZoneId.of(id);
        } catch (Throwable t) {

            try {

                InternalTimeZone internalTimeZone = convertFromMySqlTZ(id);
                String internalTzId = internalTimeZone.getId();
                zoneId = ZoneId.of(internalTzId);
            } catch (Throwable error) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TZ, id);
            }
        }
        return zoneId;
    }

}
