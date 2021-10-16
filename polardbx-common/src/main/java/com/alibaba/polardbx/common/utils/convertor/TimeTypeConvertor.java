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

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.time.old.DateUtils;
import com.alibaba.polardbx.common.datatype.Decimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeTypeConvertor {

    public static class TimeTypeToNumber extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            Object converted = null;
            if (src == null) {
                converted = null;
            } else {
                if (src instanceof java.sql.Date) {
                    String date = src.toString().replaceAll("-", "");
                    if (destClass.equals(BigInteger.class)) {
                        converted = new BigInteger(date);
                    }
                    if (destClass.equals(Long.class)) {
                        converted = Long.parseLong(date);
                    }
                    if (destClass.equals(Integer.class)) {
                        converted = Integer.parseUnsignedInt(date);
                    }
                    if (destClass.equals(Decimal.class)) {
                        converted = Decimal.fromString(date);
                    }
                    if (destClass.equals(Double.class)) {
                        converted = Double.parseDouble(date);
                    }

                } else if (src instanceof java.sql.Timestamp) {
                    long second = ((Timestamp) src).getTime();
                    if (destClass.equals(BigInteger.class)) {
                        String str = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                            .format(((Timestamp) src).toLocalDateTime());
                        converted = new BigInteger(str);
                    }
                    if (destClass.equals(Long.class)) {
                        converted = second;
                    }
                    if (destClass.equals(Integer.class)) {
                        converted = (int) second;
                    }
                    if (destClass.equals(Decimal.class)) {
                        String str = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSSSSS")
                            .format(((Timestamp) src).toLocalDateTime());
                        converted = Decimal.fromString(str);
                    }
                    if (destClass.equals(Double.class)) {
                        converted = (double) second;
                    }
                } else if (src instanceof java.sql.Time) {
                    String time = src.toString().replaceAll(":", "");
                    if (destClass.equals(BigInteger.class)) {
                        converted = new BigInteger(time);
                    }
                    if (destClass.equals(Long.class)) {
                        converted = Long.parseLong(time);
                    }
                    if (destClass.equals(Integer.class)) {
                        converted = Integer.parseUnsignedInt(time);
                    }
                    if (destClass.equals(Decimal.class)) {
                        converted = Decimal.fromString(time);
                    }
                    if (destClass.equals(Double.class)) {
                        converted = Double.parseDouble(time);
                    }
                } else if (src instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime) src;
                    java.sql.Timestamp timestamp = java.sql.Timestamp
                        .valueOf(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").format(localDateTime));

                    if (destClass.equals(BigInteger.class)) {
                        String str = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(localDateTime);
                        converted = new BigInteger(str);
                    }
                    if (destClass.equals(Long.class)) {
                        converted = timestamp.getTime();
                    }
                    if (destClass.equals(Integer.class)) {
                        converted = (int) timestamp.getTime();
                    }
                    if (destClass.equals(Decimal.class)) {
                        String str = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSSSSS").format(localDateTime);
                        converted = Decimal.fromString(str);
                    }
                    if (destClass.equals(Double.class)) {
                        converted = (double) timestamp.getTime();
                    }
                } else {
                    throw new ConvertorException("Illegal Convert, " + src.getClass() + " to " + destClass);
                }
            }
            return converted;
        }
    }

    public static class NumberToTimeType extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            Object converted = null;
            if (src == null) {
                converted = null;
            } else {
                if (destClass.equals(Timestamp.class)) {

                    if (src instanceof BigInteger) {
                        converted = new Timestamp(((BigInteger) src).longValueExact() * 1000);
                    }
                    if (src instanceof UInt64) {
                        converted = new Timestamp((((UInt64) src).toBigInteger()).longValueExact() * 1000);
                    }
                    if (src instanceof Decimal) {
                        converted = new Timestamp(((Decimal) src).longValue() * 1000);
                    }
                    if (src instanceof BigDecimal) {
                        converted = new Timestamp(((BigDecimal) src).longValueExact() * 1000);
                    }
                    if (src instanceof Long) {
                        converted = new Timestamp((long) src * 1000);
                    }
                    if (src instanceof Double) {
                        converted = new Timestamp(((Double) src).longValue() * 1000);
                    }
                    if (src instanceof Integer) {
                        converted = new Timestamp((int) src * 1000);
                    }
                    if (src instanceof Short) {
                        converted = new Timestamp((short) src * 1000);
                    }

                } else if (destClass.equals(Date.class)) {
                    long timestamp = 0;
                    if (src instanceof BigInteger) {
                        timestamp = ((BigInteger) src).longValue();
                    }
                    if (src instanceof UInt64) {
                        timestamp = (((UInt64) src).toBigInteger()).longValue();
                    }
                    if (src instanceof Decimal) {
                        timestamp = ((Decimal) src).longValue();
                    }
                    if (src instanceof BigDecimal) {
                        timestamp = ((BigDecimal) src).longValue();
                    }
                    if (src instanceof Long) {
                        timestamp = (Long) src;
                    }
                    if (src instanceof Double) {
                        timestamp = ((Double) src).longValue();
                    }
                    if (src instanceof Integer) {
                        timestamp = ((Integer) src).longValue();
                    }
                    if (src instanceof Short) {
                        timestamp = ((Short) src).longValue();
                    }
                    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                    try {
                        java.util.Date date = format.parse(String.valueOf(timestamp));
                        return new Date(date.getTime());
                    } catch (ParseException ignore) {
                    }
                    String d = format.format(timestamp);
                    return Date.valueOf(d);

                } else if (destClass.equals(Time.class)) {

                    long timestamp = 0;
                    if (src instanceof BigInteger) {
                        timestamp = ((BigInteger) src).longValue() * 1000;
                    }
                    if (src instanceof UInt64) {
                        timestamp = (((UInt64) src).toBigInteger()).longValue() * 1000;
                    }
                    if (src instanceof Decimal) {
                        timestamp = ((Decimal) src).multiply(Decimal.fromLong(1000)).longValue();
                    }
                    if (src instanceof BigDecimal) {
                        timestamp = ((BigDecimal) src).multiply(new BigDecimal(1000)).longValue();
                    }
                    if (src instanceof Long) {
                        timestamp = (Long) src * 1000;
                    }
                    if (src instanceof Double) {
                        timestamp = ((Double) (((Double) src) * 1000)).longValue();
                    }
                    if (src instanceof Integer) {
                        timestamp = ((Integer) src).longValue() * 1000;
                    }
                    if (src instanceof Short) {
                        timestamp = ((Short) src).longValue() * 1000;
                    }
                    if (timestamp / 1000 > (DateUtils.MAX_HOUR + 1) * 10000
                        || timestamp / 1000 < (DateUtils.MIN_HOUR - 1) * 10000) {
                        return null;
                    } else {
                        StringAndDateConvertor.StringToSqlTime strToTime = new StringAndDateConvertor.StringToSqlTime();
                        double val = ((double) timestamp) / 1000.0;
                        return strToTime.convert(Double.toString(val), destClass);
                    }

                } else {
                    throw new ConvertorException("Illegal Convert, " + src.getClass() + " to " + destClass);
                }
            }
            return converted;
        }
    }

}
