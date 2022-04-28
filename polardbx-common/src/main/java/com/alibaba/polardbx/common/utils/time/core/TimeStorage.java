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

package com.alibaba.polardbx.common.utils.time.core;

import com.google.common.base.Preconditions;
import com.google.common.io.LittleEndianDataOutputStream;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Optional;


public class TimeStorage {

    public static long packTime(Time time) {
        if (time == null) {
            return 0L;
        }
        if (time instanceof OriginalTime) {
            return TimeStorage.writeTime(((OriginalTime) time).getMysqlDateTime());
        } else {
            return Optional.ofNullable(time)
                .map(MySQLTimeTypeUtil::toMysqlTime)
                .map(TimeStorage::writeTime)
                .orElse(0L);
        }
    }

    public static long packDatetime(Timestamp t) {
        if (t == null) {
            return 0L;
        }
        if (t instanceof OriginalTimestamp) {
            return TimeStorage.writeTimestamp(((OriginalTimestamp) t).getMysqlDateTime());
        } else {
            return Optional.ofNullable(t)
                .map(MySQLTimeTypeUtil::toMysqlDateTime)
                .map(TimeStorage::writeTimestamp)
                .orElse(0L);
        }
    }

    public static long packDate(Date date) {
        if (date == null) {
            return 0L;
        }
        if (date instanceof OriginalDate) {
            return TimeStorage.writeDate(((OriginalDate) date).getMysqlDateTime());
        } else {
            return Optional.ofNullable(date)
                .map(MySQLTimeTypeUtil::toMysqlDate)
                .map(TimeStorage::writeDate)
                .orElse(0L);
        }
    }

    public static long writeTime(MysqlDateTime t) {

        long l1 = ((t.getMonth() != 0 ? 0 : t.getDay() * 24) + t.getHour()) << 12;


        long l2 = l1 | (t.getMinute() << 6) | t.getSecond();


        long l3 = (l2 << 24) + (t.getSecondPart() / 1000);


        return t.isNeg() ? -l3 : l3;
    }

    public static long writeTime(long hour, long minute, long second, long secondPart, boolean isNeg) {
        //  If month is 0, we mix day with hours: "1 00:10:10" -> "24:00:10"
        long l1 = hour << 12;
        // for minute & second
        long l2 = l1 | (minute << 6) | second;
        // for nano
        long l3 = (l2 << 24) + (secondPart / 1000);
        // for sign
        return isNeg ? -l3 : l3;
    }


    public static MysqlDateTime readTime(long l) {
        MysqlDateTime t = new MysqlDateTime();

        t.setNeg(l < 0);
        t.setYear(0);
        t.setMonth(0);
        t.setDay(0);
        l = Math.abs(l);

        long hms = l >> 24;

        t.setHour((hms >> 12) % (1L << 10));


        t.setMinute((hms >> 6) % (1L << 6));


        t.setSecond(hms % (1L << 6));

        t.setSecondPart((l % (1L << 24)) * 1000);
        t.setSqlType(Types.TIME);
        return t;
    }

    public static long writeTimestamp(MysqlDateTime t) {

        long ymd = ((t.getYear() * 13 + t.getMonth()) << 5) | t.getDay();
        long hms = (t.getHour() << 12) | (t.getMinute() << 6) | t.getSecond();
        long l = (((ymd << 17) | hms) << 24) + (t.getSecondPart() / 1000);

        return t.isNeg() ? -l : l;
    }

    public static long writeTimestamp(long year, long month, long day, long hour, long minute, long second, long secondPart, boolean isNeg) {
        // | 64 - 42 | 41 - 25 | 24 - 1 |
        // |   ymd   |   hms   |  nano  |
        long ymd = ((year * 13 + month) << 5) | day;
        long hms = (hour << 12) | (minute << 6) | second;
        long l = (((ymd << 17) | hms) << 24) + (secondPart / 1000);
        return isNeg ? -l : l;
    }


    public static MysqlDateTime readTimestamp(long l) {
        MysqlDateTime t = new MysqlDateTime();
        t.setNeg(l < 0);
        l = Math.abs(l);

        t.setSecondPart((l % (1L << 24)) * 1000L);

        long l2 = l >> 24;
        long ymd = l2 >> 17;
        long ym = ymd >> 5;
        t.setDay(ymd % (1L << 5));
        t.setMonth(ym % 13);
        t.setYear(ym / 13);

        long hms = l2 % (1L << 17);
        t.setSecond(hms % (1L << 6));
        t.setMinute((hms >> 6) % (1L << 6));
        t.setHour(hms >> 12);

        t.setSqlType(Types.TIMESTAMP);
        return t;
    }

    public static long writeDate(MysqlDateTime t) {
        long ymd = ((t.getYear() * 13 + t.getMonth()) << 5) | t.getDay();
        return ymd << (24 + 17);
    }

    public static long writeDate(long year, long month, long day) {
        long ymd = ((year * 13 + month) << 5) | day;
        return ymd << (24 + 17);
    }

    public static MysqlDateTime readDate(long l) {
        MysqlDateTime t = readTimestamp(l);
        t.setSqlType(Types.DATE);
        return t;
    }

    public static byte[] storeAsBinary(long l, int decimal) {

        Preconditions.checkArgument(decimal <= 6);

        Preconditions.checkArgument((l % (1L << 24))
            % StringTimeParser.LOG_10[6 - decimal] == 0);

        long l1 = 0x800000L + (l >> 24);
        byte[] bytes;
        switch (decimal) {
        case 0:
        default:

            bytes = new byte[3];
            bytes[2] = (byte) (l1 & 0xFF);
            bytes[1] = (byte) ((l1 >> 8) & 0xFF);
            bytes[0] = (byte) ((l1 >> 16) & 0xFF);
            break;
        case 1:
        case 2:

            bytes = new byte[4];
            bytes[2] = (byte) (l1 & 0xFF);
            bytes[1] = (byte) ((l1 >> 8) & 0xFF);
            bytes[0] = (byte) ((l1 >> 16) & 0xFF);

            bytes[3] = (byte) (((l % (1L << 24)) / 10000) & 0xFF);
            break;
        case 3:
        case 4:

            bytes = new byte[5];
            bytes[2] = (byte) (l1 & 0xFF);
            bytes[1] = (byte) ((l1 >> 8) & 0xFF);
            bytes[0] = (byte) ((l1 >> 16) & 0xFF);

            long l2 = (l % (1L << 24)) / 100;

            bytes[4] = (byte) (l2 & 0xFF);
            bytes[3] = (byte) ((l2 >> 8) & 0xFF);
            break;
        case 5:
        case 6:
            long l3 = l + 0x800000000000L;
            bytes = new byte[6];
            bytes[5] = (byte) (l3 & 0xFF);
            bytes[4] = (byte) ((l3 >> 8) & 0xFF);
            bytes[3] = (byte) ((l3 >> 16) & 0xFF);
            bytes[2] = (byte) ((l3 >> 24) & 0xFF);
            bytes[1] = (byte) ((l3 >> 32) & 0xFF);
            bytes[0] = (byte) ((l3 >> 40) & 0xFF);
            break;
        }
        return bytes;
    }

    public static byte[] convertDate4(Date v) {
        ByteArrayOutputStream bb = new ByteArrayOutputStream(4 + 1);

        LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(bb);
        try {
            if (v instanceof OriginalDate) {
                MysqlDateTime t = MySQLTimeTypeUtil.toMysqlDate(v);

                if (t.getYear() == 0 && t.getMonth() == 0 && t.getDay() == 0) {
                    return new byte[] {0};
                }

                out.writeByte(4);
                out.writeShort((int) t.getYear());
                out.writeByte((int) t.getMonth());
                out.writeByte((int) t.getDay());
            } else {

                if (v.getYear() == 0 && v.getMonth() == 0 && v.getDate() == 0) {
                    return new byte[] {0};
                }

                out.writeByte(4);
                out.writeShort(v.getYear() + 1900);
                out.writeByte(v.getMonth() + 1);
                out.writeByte(v.getDate());
            }

        } catch (IOException e) {
            return null;
        } finally {
            IOUtils.closeQuietly(out);
        }

        return bb.toByteArray();
    }

    public static byte[] convertDate11(Timestamp v) {
        ByteArrayOutputStream bb = new ByteArrayOutputStream(12 + 1);

        LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(bb);
        try {

            if (isZero(v)) {
                return new byte[] {0};
            }

            if (v instanceof OriginalTimestamp) {
                MysqlDateTime t = ((OriginalTimestamp) v).getMysqlDateTime();
                out.writeByte(11);
                out.writeShort((int) t.getYear());
                out.writeByte((int) t.getMonth());
                out.writeByte((int) t.getDay());
                out.writeByte((int) t.getHour());
                out.writeByte((int) t.getMinute());
                out.writeByte((int) t.getSecond());
                out.writeInt((int) t.getSecondPart() / 1000);
            } else {
                out.writeByte(11);
                out.writeShort(v.getYear() + 1900);
                out.writeByte(v.getMonth() + 1);
                out.writeByte(v.getDate());
                out.writeByte(v.getHours());
                out.writeByte(v.getMinutes());
                out.writeByte(v.getSeconds());
                out.writeInt(v.getNanos() / 1000);
            }
        } catch (IOException e) {
            return null;
        } finally {
            IOUtils.closeQuietly(out);
        }

        return bb.toByteArray();
    }

    private static boolean isZero(Timestamp v) {
        if (v instanceof OriginalTimestamp) {
            MysqlDateTime t = ((OriginalTimestamp) v).getMysqlDateTime();
            return t.getYear() == 0 && t.getMonth() == 0 && t.getDay() == 0
                && t.getHour() == 0 && t.getMinute() == 0 && t.getSecond() == 0
                && t.getSecondPart() == 0;
        } else {
            return v.getYear() == 0 && v.getMonth() == 0 && v.getDate() == 0
                && v.getHours() == 0 && v.getMinutes() == 0 && v.getSeconds() == 0
                && v.getNanos() == 0;
        }
    }

    public static byte[] convertDate2(Date v) {
        byte[] result = new byte[2];

        short year;
        if (v instanceof OriginalDate) {
            MysqlDateTime t = ((OriginalDate) v).getMysqlDateTime();
            year = (short) ((int) t.getYear());
        } else {
            year = (short) (v.getYear() + 1900);
        }

        result[0] = (byte) (year & 0xff);
        result[1] = (byte) (year >> 8);

        return result;
    }
}