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

package com.alibaba.polardbx.common.utils.convertor;

import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.old.DateUtils;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class StringAndDateConvertor {

    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String TIMESTAMP3_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String TIMESTAMP6_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final String TIME3_FORMAT = "HH:mm:ss.SSS";


    public static class StringToDate extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src)) {
                return DateUtils.str_to_time((String) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }


    public static class StringToSqlDate extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (src instanceof String) {
                if (Timestamp.class == destClass) {
                    try {
                        return MySQLTimeTypeUtil.bytesToDatetime(
                            ((String) src).getBytes(),
                            Types.TIMESTAMP,
                            false);
                    } catch (IllegalArgumentException ignore) {
                    }
                }

                Date date = null;
                String val = DateUtils.strToDateString((String) src);
                String format = val.indexOf(
                    "-") <= 2 ? DateUtils.MYSQL_DATETIME_DEFAULT_FORMAT2 : DateUtils.MYSQL_DATETIME_DEFAULT_FORMAT1;
                date = DateUtils.extractDateTime(val, format);

                if (date == null) {
                    throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
                }
                return ConvertorHelper.dateToSql.convert(date, destClass);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }


    public static class StringToSqlTime extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src)) {
                String str = (String) src;
                str = str.trim();
                Date date = null;
                Calendar cal = Calendar.getInstance();
                cal.clear();
                str = DateUtils.strToTimeString(str);

                if (!str.isEmpty() && str.charAt(0) == '-') {
                    date = DateUtils.extractDateTime(str.substring(1), DateUtils.MYSQL_TIME_FORMAT);
                    if (date == null) {
                        return null;
                    }

                    Calendar myCal = Calendar.getInstance();
                    DateUtils.resetCalendar(myCal);
                    long diff = date.getTime() - myCal.getTime().getTime();
                    date.setTime(cal.getTime().getTime() + diff);
                    DateUtils.normalizeTime(date);
                    return new java.sql.Time(cal.getTime().getTime() - date.getTime());
                } else {

                    boolean dateOrTime = (str.indexOf(' ') == -1);
                    if (dateOrTime) {
                        date = DateUtils.extractDateTime(str, DateUtils.MYSQL_TIME_FORMAT);
                    } else {
                        String val = DateUtils.strToDateString(str);
                        String format = val.indexOf(
                            "-") <= 2 ? DateUtils.MYSQL_DATETIME_DEFAULT_FORMAT2 :
                            DateUtils.MYSQL_DATETIME_DEFAULT_FORMAT1;
                        date = DateUtils.extractDateTime(val, format);

                        if (date == null) {
                            return null;
                        }

                        date = DateUtils.extractDateTime(str.substring(str.indexOf(' ')).trim(),
                            DateUtils.MYSQL_TIME_FORMAT);
                    }
                    if (date == null) {
                        return null;
                    }
                    Calendar myCal = Calendar.getInstance();
                    DateUtils.resetCalendar(myCal);
                    long diff = date.getTime() - myCal.getTime().getTime();
                    date.setTime(cal.getTime().getTime() + diff);
                    DateUtils.normalizeTime(date);
                    return new java.sql.Time(cal.getTime().getTime() + date.getTime());
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class StringToCalendar extends StringToDate {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src)) {
                Date dest = (Date) super.convert(src, Date.class);
                Calendar result = new GregorianCalendar();
                result.setTime(dest);
                return result;
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class SqlDateToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (ZeroDate.class.isInstance(src)) {
                return ZeroDate.instance.toString();
            }
            if (Date.class.isInstance(src)) {
                return new SimpleDateFormat(DATE_FORMAT).format((Date) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class SqlTimeToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {

            if (ZeroTime.class.isInstance(src)) {
                return ZeroTime.instance.toString();
            }

            if (Date.class.isInstance(src)) {
                final long timeInMillis = ((Date) src).getTime();
                Calendar cal = Calendar.getInstance();
                cal.clear();

                return formatSqlTime(timeInMillis - cal.getTimeInMillis());
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

        public String formatSqlTime(long timeInMillis) {
            StringBuilder builder = new StringBuilder(16);

            final boolean minus = (timeInMillis < 0);
            if (minus) {
                timeInMillis = -timeInMillis;
            }
            final int millisecond = (int) (timeInMillis % 1000);
            timeInMillis = timeInMillis / 1000;
            final int second = (int) (timeInMillis % 60);
            timeInMillis = timeInMillis / 60;
            final int minute = (int) (timeInMillis % 60);
            timeInMillis = timeInMillis / 60;
            final int hour = (int) (timeInMillis);

            if (minus) {
                builder.append('-');
            }
            if (hour < 10) {
                builder.append('0');
            }
            builder.append(hour);
            builder.append(':');
            if (minute < 10) {
                builder.append('0');
            }
            builder.append(minute);
            builder.append(':');
            if (second < 10) {
                builder.append('0');
            }
            builder.append(second);
            if (millisecond > 0) {
                builder.append('.');
                builder.append(millisecond < 10 ? "00" : millisecond < 100 ? "0" : "");
                builder.append(millisecond);
            }

            return builder.toString();
        }
    }

    public static class SqlTimestampToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (ZeroTimestamp.class.isInstance(src)) {
                return ZeroTimestamp.instance.toString();
            }

            if (src instanceof Timestamp) {
                return src.toString();
            }

            if (Date.class.isInstance(src)) {
                Date date = (Date) src;

                if ((date.getTime() % 1000) != 0) {
                    return getTimeStampFormatDateString(date);
                }
                return new SimpleDateFormat(TIMESTAMP_FORMAT).format(date);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

        private String getTimeStampFormatDateString(Date src) {
            if (src == null) {
                return null;
            }
            if (src instanceof Timestamp) {
                final int nanos = ((Timestamp) src).getNanos();
                if (nanos == 0) {
                    return new SimpleDateFormat(TIMESTAMP_FORMAT).format((Timestamp) src);
                }

                String str = String.valueOf(nanos);
                int pos = -1;
                int preZero = 0;
                if (str.length() < 9) {
                    preZero = 9 - str.length();
                }
                for (int i = str.length() - 1; i >= 0; i--) {
                    if (str.toCharArray()[i] == '0') {
                        continue;
                    }
                    pos = i;
                    break;
                }
                if (preZero + pos + 1 == 0) {
                    return new SimpleDateFormat(TIMESTAMP_FORMAT).format((Timestamp) src);
                }
                StringBuilder sb = new StringBuilder(".");
                for (int i = 0; i < preZero + (pos + 1); i++) {
                    if (i < preZero) {
                        sb.append('0');
                    } else {
                        sb.append(str.toCharArray()[i - preZero]);
                    }
                }
                return new SimpleDateFormat(TIMESTAMP_FORMAT).format((Timestamp) src) + sb.toString();
            }
            return new SimpleDateFormat(TIMESTAMP3_FORMAT).format(src);
        }

    }

    public static class CalendarToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Calendar.class.isInstance(src)) {
                Date date = ((Calendar) src).getTime();

                if ((date.getTime() % 1000) != 0) {
                    return new SimpleDateFormat(TIMESTAMP3_FORMAT).format(date);
                }
                return new SimpleDateFormat(TIMESTAMP_FORMAT).format(date);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

    }

    public static class LocalDateTimeToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (src instanceof LocalDateTime) {
                LocalDateTime time = (LocalDateTime) src;
                if (time.getNano() / 1000 != 0) {

                    return DateTimeFormatter.ofPattern(TIMESTAMP6_FORMAT).format(time);
                } else {

                    return DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT).format(time);
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

    }

}
