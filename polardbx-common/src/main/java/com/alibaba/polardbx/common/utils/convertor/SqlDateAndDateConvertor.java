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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;


public class SqlDateAndDateConvertor {

    public static class SqlDateToDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Date.class != destClass) {
                throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
            }

            if (src instanceof java.sql.Date) {
                return new Date(((java.sql.Date) src).getTime());
            }
            if (src instanceof java.sql.Timestamp) {
                return new Date(((java.sql.Timestamp) src).getTime());
            }
            if (src instanceof java.sql.Time) {
                return convertSqlTimeToDate((java.sql.Time) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class DateToSqlDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Date.class.isInstance(src)) {
                Date date = (Date) src;

                if (destClass.equals(java.sql.Date.class)) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    cal.set(Calendar.HOUR_OF_DAY, 0);
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    return new java.sql.Date(cal.getTimeInMillis());
                }

                if (destClass.equals(java.sql.Time.class)) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    cal.set(1970, 0, 1);
                    return new java.sql.Time(cal.getTimeInMillis());
                }


                if (destClass.equals(java.sql.Timestamp.class)) {
                    return new java.sql.Timestamp(date.getTime());
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class CalendarToDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Calendar.class.isInstance(src)) {
                Calendar cal = (Calendar) src;


                if (destClass.equals(Date.class)) {
                    return cal.getTime();
                }

                if (destClass.equals(java.sql.Date.class)) {
                    cal.set(Calendar.HOUR_OF_DAY, 0);
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    return new java.sql.Date(cal.getTimeInMillis());
                }

                if (destClass.equals(java.sql.Time.class)) {
                    cal.set(1970, 0, 1);
                    return new java.sql.Time(cal.getTimeInMillis());
                }

                if (destClass.equals(java.sql.Timestamp.class)) {
                    return new java.sql.Timestamp(cal.getTimeInMillis());
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class LocalDateTimeToSqlDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {

            if (src instanceof LocalDateTime) {
                LocalDateTime localDateTime = (LocalDateTime) src;
                java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                        .format(localDateTime));

                if (destClass.equals(java.sql.Date.class)) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(timestamp);
                    cal.set(Calendar.HOUR_OF_DAY, 0);
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    return new java.sql.Date(cal.getTimeInMillis());
                }

                if (destClass.equals(java.sql.Time.class)) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(timestamp);
                    cal.set(1970, 0, 1);
                    return new java.sql.Time(cal.getTimeInMillis());
                }

                if (destClass.equals(java.sql.Timestamp.class)) {
                    return timestamp;
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class SqlTimeToSqlDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (java.sql.Time.class.isInstance(src)) {

                Date date = convertSqlTimeToDate((java.sql.Time) src);

                if (destClass.equals(java.sql.Date.class)) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    cal.set(Calendar.HOUR_OF_DAY, 0);
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    return new java.sql.Date(cal.getTimeInMillis());
                }

                if (destClass.equals(java.sql.Timestamp.class)) {
                    return new java.sql.Timestamp(date.getTime());
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class LocalDateTimeToDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {

            if (src instanceof LocalDateTime) {
                LocalDateTime localDateTime = (LocalDateTime) src;
                java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                        .format(localDateTime));

                if (destClass.equals(Date.class)) {
                    return new Date(timestamp.getTime());
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static Date convertSqlTimeToDate(java.sql.Time time) {
        Calendar cal = Calendar.getInstance();

        int gapOfYear = cal.get(Calendar.YEAR) - 1970;
        int gapOfDay = cal.get(Calendar.DAY_OF_YEAR) - 1;

        cal.setTime(time);
        cal.add(Calendar.YEAR, gapOfYear);
        cal.add(Calendar.DAY_OF_YEAR, gapOfDay);

        return cal.getTime();
    }
}
