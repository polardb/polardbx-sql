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

package com.alibaba.polardbx.common.utils.time.parser;

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;

import java.sql.Types;
import java.util.Arrays;
import java.util.stream.Collectors;


public class StringTimeParser extends MySQLTimeParserBase {
    private static final byte[][] WEEK_DAYS =
        Arrays.stream(new String[] {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"})
            .map(String::getBytes)
            .collect(Collectors.toList())
            .toArray(new byte[][] {});

    private static final byte[][] WEEK_DAYS_ABBR =
        Arrays.stream(new String[] {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"})
            .map(String::getBytes)
            .collect(Collectors.toList())
            .toArray(new byte[][] {});

    private static final byte[][] MONTHS = Arrays.stream(new String[] {
        "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November",
        "December"})
        .map(String::getBytes)
        .collect(Collectors.toList())
        .toArray(new byte[][] {});

    private static final byte[][] MONTHS_ABBR =
        Arrays.stream(new String[] {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"})
            .map(String::getBytes)
            .collect(Collectors.toList())
            .toArray(new byte[][] {});

    private static final byte[] R_REPLACE = "%I:%i:%S %p".getBytes();

    private static final byte[] T_REPLACE = "%H:%i:%S".getBytes();


    public static MysqlDateTime parseString(byte[] timestampAsBytes, int sqlType) {
        switch (sqlType) {

        case Types.TIME:
            return parseTime(timestampAsBytes);
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        case Types.TIMESTAMP:
        case Types.DATE:
        default:
            return parseDatetime(timestampAsBytes);
        }
    }


    public static MysqlDateTime parseString(byte[] timestampAsBytes, int sqlType, int flags) {
        switch (sqlType) {

        case Types.TIME:
            return parseTime(timestampAsBytes, flags);
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        case Types.TIMESTAMP:
        case Types.DATE:
        default:
            return parseDatetime(timestampAsBytes, flags);
        }
    }


    public static MysqlDateTime parseDatetime(byte[] timestampAsBytes) {
        return parseDatetime(timestampAsBytes, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
    }


    public static MysqlDateTime parseDatetime(byte[] timestampAsBytes, int flags) {
        return parseDatetime(timestampAsBytes, flags, null);
    }


    public static MysqlDateTime parseDatetime(byte[] timestampAsBytes, int flags, TimeParseStatus status) {
        int pos = 0;
        int lastPos = pos;
        final int len = timestampAsBytes.length;


        int[] dateLen = new int[8];
        int[] date = new int[8];


        while (pos < len && isSpace(timestampAsBytes[pos])) {
            pos++;
        }

        if (pos == len || !isDigit(timestampAsBytes[pos])) {
            if (status != null) {
                status.addWarning(TimeParserFlags.FLAG_TIME_WARN_TRUNCATED);
                status.setStatus(TimeParseStatus.StatusType.NONE);
            }
            return null;
        }

        int digits = 0;
        for (int i = pos;
             i < len && (isDigit(timestampAsBytes[i]) || timestampAsBytes[i] == 'T');
             i++) {
            digits++;
        }

        dateLen[0] = 0;
        int yearLen = 0;
        int fieldLen = 0;
        boolean isInternalFormat = false;

        if (digits == len || timestampAsBytes[digits] == '.') {

            yearLen = (digits == 4 || digits == 8 || digits >= 14) ? 4 : 2;
            fieldLen = yearLen;
            isInternalFormat = true;
        } else {

            fieldLen = 4;
        }

        int allowSpace = (1 << 2) | (1 << 6);
        allowSpace &= (1 | 2 | 4 | 8 | 64);
        boolean notZeroDate = false;
        boolean foundDelimiter = false;
        boolean foundSpace = false;
        int fieldNumber = 0;
        for (;
             fieldNumber < 7 && pos < len && isDigit(timestampAsBytes[pos]);
             fieldNumber++) {
            int start = pos;
            int value = timestampAsBytes[pos] - '0';
            pos++;

            boolean scanUtilDelimiter = !isInternalFormat && fieldNumber != 6;

            int f = fieldLen;
            while (pos < len
                && isDigit(timestampAsBytes[pos])
                && (scanUtilDelimiter || --f > 0)) {
                value = value * 10 + (timestampAsBytes[pos] - '0');
                pos++;
            }
            dateLen[fieldNumber] = pos - start;
            if (value > 999999) {
                if (status != null) {
                    status.addWarning(TimeParserFlags.FLAG_TIME_WARN_TRUNCATED);
                    status.setStatus(TimeParseStatus.StatusType.NONE);
                }

                return null;
            }
            date[fieldNumber] = value;
            notZeroDate |= (value != 0);

            fieldLen = 2;

            if ((lastPos = pos) == len) {

                fieldNumber++;
                break;
            }

            if (fieldNumber == 2 && timestampAsBytes[pos] == 'T') {
                pos++;
                continue;
            }

            if (fieldNumber == 5) {

                if (timestampAsBytes[pos] == '.') {
                    pos++;

                    lastPos = pos;
                    fieldLen = 6;
                } else if (isDigit(timestampAsBytes[pos])) {

                    fieldNumber++;
                    break;
                }
                continue;
            }

            while (pos < len
                && (isSpace(timestampAsBytes[pos]) || isPunctuation(timestampAsBytes[pos]))) {
                if (isSpace(timestampAsBytes[pos])) {
                    if ((allowSpace & (1 << fieldNumber)) == 0) {
                        if (status != null) {
                            status.addWarning(TimeParserFlags.FLAG_TIME_WARN_TRUNCATED);
                            status.setStatus(TimeParseStatus.StatusType.NONE);
                        }
                        return null;
                    }
                    foundSpace = true;
                }
                pos++;
                foundDelimiter = true;
            }

            if (fieldNumber == 6) {
                fieldNumber++;
            }
            lastPos = pos;
        }

        if (foundDelimiter
            && !foundSpace
            && TimeParserFlags.check(flags, TimeParserFlags.FLAG_TIME_DATETIME_ONLY)) {
            if (status != null) {
                status.addWarning(TimeParserFlags.FLAG_TIME_WARN_TRUNCATED);
                status.setStatus(TimeParseStatus.StatusType.NONE);
            }
            return null;
        }

        pos = lastPos;
        for (int i = fieldNumber; i < 8; i++) {
            dateLen[i] = 0;
            date[i] = 0;
        }

        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int microSecond = 0;
        boolean isNeg = false;
        int sqlType;

        if (!isInternalFormat) {
            yearLen = dateLen[0];
            if (yearLen <= 0) {
                if (status != null) {
                    status.addWarning(TimeParserFlags.FLAG_TIME_WARN_TRUNCATED);
                    status.setStatus(TimeParseStatus.StatusType.NONE);
                }
                return null;
            }
        }

        year = date[0];
        month = date[1];
        day = date[2];
        hour = date[3];
        minute = date[4];
        second = date[5];

        int fractionalLen = dateLen[6];
        if (fractionalLen < MySQLTimeTypeUtil.MAX_NANO_LENGTH) {

            date[6] *= LOG_10[MySQLTimeTypeUtil.MAX_NANO_LENGTH - fractionalLen];
        }
        microSecond = date[6];

        if (yearLen == 2 && notZeroDate) {
            year += (year < 70 ? 2000 : 1900);
        }

        sqlType = fieldNumber <= 3 ? Types.DATE : Types.TIMESTAMP;
        MysqlDateTime ret = new MysqlDateTime();
        ret.setYear(year);
        ret.setMonth(month);
        ret.setDay(day);
        ret.setHour(hour);
        ret.setMinute(minute);
        ret.setSecond(second);
        ret.setSecondPart(microSecond * 1000L);
        ret.setNeg(isNeg);
        ret.setSqlType(sqlType);

        if (fieldNumber < 3 || MySQLTimeTypeUtil.isDatetimeRangeInvalid(ret)) {

            if (!notZeroDate) {
                for (; pos < len; pos++) {
                    if (!isSpace(timestampAsBytes[pos])) {

                        notZeroDate = true;
                        break;
                    }
                }
            }

            if (status != null) {
                status.addWarning(notZeroDate ? TimeParserFlags.FLAG_TIME_WARN_TRUNCATED : TimeParserFlags.FLAG_TIME_WARN_ZERO_DATE);
                status.setStatus(TimeParseStatus.StatusType.ERROR);
            }
            return null;
        }

        if (MySQLTimeTypeUtil.isDateInvalid(ret, notZeroDate, flags, status)) {
            if (status != null) {
                status.setStatus(TimeParseStatus.StatusType.ERROR);
            }
            return null;
        }

        int nano = 0;
        if (fractionalLen == 6 && pos < len) {
            if (isDigit(timestampAsBytes[pos])) {
                nano = 100 * (timestampAsBytes[pos] - '0');
                pos++;
            }
        }

        if (!TimeParserFlags.check(flags, TimeParserFlags.FLAG_TIME_NO_NANO_ROUNDING)) {

            ret = MySQLTimeCalculator.datetimeAddNanoWithRound(ret, nano);
        }

        if (status != null) {
            status.setStatus(TimeParseStatus.StatusType.of(ret.getSqlType()));
        }
        return ret;
    }

    public static MysqlDateTime parseTime(byte[] timestampAsBytes) {
        return parseTime(timestampAsBytes, 0);
    }

    public static MysqlDateTime parseTime(byte[] timestampAsBytes, int flags) {
        return parseTime(timestampAsBytes, flags, null);
    }

    public static MysqlDateTime parseTime(byte[] timestampAsBytes, int flags, TimeParseStatus status) {
        if (timestampAsBytes == null || timestampAsBytes.length == 0) {
            return null;
        }

        long[] date = new long[5];

        int state = 0;

        MysqlDateTime ret = new MysqlDateTime();
        ret.setNeg(false);

        final int len = timestampAsBytes.length;
        int pos = 0;
        int rest = len - pos;

        while (pos < len && isSpace(timestampAsBytes[pos])) {
            pos++;
            rest--;
        }

        if (pos < len && timestampAsBytes[pos] == '-') {
            ret.setNeg(true);
            pos++;
            rest--;
        }
        if (pos == len) {
            return null;
        }

        if (rest >= 12) {

            TimeParseStatus tmpStatus = status == null ? new TimeParseStatus() : status;
            MysqlDateTime datetime = parseDatetime(
                timestampAsBytes,
                (TimeParserFlags.FLAG_TIME_DATETIME_ONLY | TimeParserFlags.FLAG_TIME_FUZZY_DATE),
                tmpStatus);
            if (tmpStatus.getStatus() != TimeParseStatus.StatusType.NONE) {

                return datetime;
            } else if (status != null) {
                status.clear();
            }
        }

        long value = 0;
        while (pos < len && isDigit(timestampAsBytes[pos])) {
            value = value * 10 + (timestampAsBytes[pos] - '0');
            pos++;
        }

        if (value < 0 || value > MAX_UNSIGNED_INTEGER_VALUE) {
            return null;
        }

        int endOfDays = pos;
        while (pos < len && isSpace(timestampAsBytes[pos])) {
            pos++;
            rest--;
        }

        boolean foundDays = false;
        boolean foundHours = false;
        boolean skipToFractional = false;

        if (len - pos > 1
            && pos != endOfDays
            && isDigit(timestampAsBytes[pos])) {

            date[0] = (int) value;

            state = 1;
            foundDays = true;
        } else if (len - pos > 1
            && timestampAsBytes[pos] == ':'
            && isDigit(timestampAsBytes[pos + 1])) {
            date[0] = 0;
            date[1] = (int) value;

            state = 2;
            foundHours = true;

            pos++;
        } else {

            date[0] = 0;
            date[1] = value / 10000;
            date[2] = value / 100 % 100;
            date[3] = value % 100;
            state = 4;
            skipToFractional = true;
        }

        if (!skipToFractional) {

            while (true) {
                value = 0;
                while (pos < len && isDigit(timestampAsBytes[pos])) {
                    value = value * 10 + (timestampAsBytes[pos] - '0');
                    pos++;
                }
                date[state] = (int) value;
                state++;
                if (state == 4
                    || len - pos < 2
                    || timestampAsBytes[pos] != ':'
                    || !isDigit(timestampAsBytes[pos + 1])) {
                    break;
                }

                pos++;
            }

            if (state != 4) {

                if (!foundDays && !foundHours) {

                    for (int i = 0; i < 5; i++) {
                        int j = i + 4 - state;
                        date[i] = j < 5 ? date[j] : 0;
                    }
                } else {

                    for (int i = state; i < 5; i++) {
                        date[i] = 0;
                    }
                }
            }
        }

        int nano = 0;

        if (len - pos >= 2
            && timestampAsBytes[pos] == '.'
            && isDigit(timestampAsBytes[pos + 1])) {
            int fieldLen = 5;
            pos++;
            value = timestampAsBytes[pos] - '0';

            while (++pos < len && isDigit(timestampAsBytes[pos])) {
                if (fieldLen-- > 0) {
                    value = value * 10 + (timestampAsBytes[pos] - '0');
                }
            }

            if (fieldLen > 0) {
                value *= LOG_10[fieldLen];
            } else if (fieldLen != 0) {

                nano = 100 * (timestampAsBytes[pos - 1] - '0');

                while (pos < len && isDigit(timestampAsBytes[pos])) {
                    pos++;
                }
            }
            date[4] = value;
        } else if (len - pos == 1 && timestampAsBytes[pos] == '.') {
            pos++;
            date[4] = 0;
        } else {
            date[4] = 0;
        }

        if (len - pos > 1) {
            boolean checkE = (timestampAsBytes[pos] == 'e' || timestampAsBytes[pos] == 'E');
            boolean checkDigit1 = isDigit(timestampAsBytes[pos + 1]);
            boolean checkDigit2 = (timestampAsBytes[pos + 1] == '-' || timestampAsBytes[pos + 1] == '+')
                && len - pos > 2
                && isDigit(timestampAsBytes[pos + 2]);
            if (checkE && (checkDigit1 || checkDigit2)) {
                return null;
            }
        }

        for (int i = 0; i < 5; i++) {
            if (date[i] < 0 || date[i] > MAX_UNSIGNED_INTEGER_VALUE) {
                return null;
            }
        }

        long hour = date[1] + date[0] * 24;
        int sqlType = Types.TIME;
        ret.setYear(0);
        ret.setMonth(0);
        ret.setDay(0);
        ret.setHour(hour);
        ret.setMinute(date[2]);
        ret.setSecond(date[3]);
        ret.setSecondPart(date[4] * 1000);
        ret.setSqlType(sqlType);

        if (MySQLTimeTypeUtil.isTimeRangeInvalid(ret)) {
            if (status != null) {
                status.addWarning(TimeParserFlags.FLAG_TIME_WARN_OUT_OF_RANGE);
                status.setStatus(TimeParseStatus.StatusType.ERROR);
            }
            return null;
        }

        boolean valid = hour <= MySQLTimeTypeUtil.TIME_MAX_HOUR
            && (hour != MySQLTimeTypeUtil.TIME_MAX_HOUR
            || ret.getMinute() != 59
            || ret.getSecond() != 59
            || ret.getSecondPart() == 0);
        if (!valid) {
            ret.setDay(0);
            ret.setSecondPart(0);
            ret.setHour(MySQLTimeTypeUtil.TIME_MAX_HOUR);
            ret.setMinute(59);
            ret.setSecond(59);
            if (status != null) {
                status.addWarning(TimeParserFlags.FLAG_TIME_WARN_OUT_OF_RANGE);
            }
        }

        if (status != null) {
            status.setStatus(TimeParseStatus.StatusType.of(sqlType));
        }

        if (!TimeParserFlags.check(flags, TimeParserFlags.FLAG_TIME_NO_NANO_ROUNDING)) {

            ret = MySQLTimeCalculator.datetimeAddNanoWithRound(ret, nano);
        }

        return ret;
    }

    public static MysqlDateTime extractFormat(byte[] timestampAsBytes, byte[] formatAsBytes) {
        if (timestampAsBytes == null || formatAsBytes == null) {
            return null;
        }

        boolean needReplace = false;
        for (int i = 0; i < formatAsBytes.length; i++) {
            if (formatAsBytes[i] == '%' && i + 1 < formatAsBytes.length
                && (formatAsBytes[i + 1] == 'T' || formatAsBytes[i + 1] == 'r')) {
                needReplace = true;
                break;
            }

        }

        if (needReplace) {
            SliceOutput sliceOutput = new DynamicSliceOutput(formatAsBytes.length);
            for (int i = 0; i < formatAsBytes.length; i++) {
                if (formatAsBytes[i] == '%' && i + 1 < formatAsBytes.length) {
                    if (formatAsBytes[i + 1] == 'T') {
                        sliceOutput.appendBytes(T_REPLACE);
                        ++i;
                        continue;
                    } else if (formatAsBytes[i + 1] == 'r') {
                        sliceOutput.appendBytes(R_REPLACE);
                        ++i;
                        continue;
                    }
                }
                sliceOutput.appendByte(formatAsBytes[i]);
            }
        }

        MysqlDateTime t = new MysqlDateTime();
        int error = 0;
        int formatPos = 0, formatEnd = formatAsBytes.length;
        int valPos = 0, valEnd = timestampAsBytes.length;
        boolean usaTime = false;

        int dayPart = 0;
        int weekDay = 0, yearDay = 0;
        boolean isSundayFirstDayOfWeek = false, isStrictWeekNumber = false;
        int weekNumber = -1;
        boolean isStrictWeekNumberYearType = false;
        int isStrictWeekNumberYear = -1;
        for (; formatPos < formatEnd && valPos < valEnd; formatPos++) {

            while (valPos < valEnd && timestampAsBytes[valPos] == ' ') {
                valPos++;
            }
            if (valPos == valEnd) {
                break;
            }

            if (formatAsBytes[formatPos] == '%' && formatPos + 1 < formatEnd) {
                int valLen = valEnd - valPos;
                int tmpValPos;

                switch (formatAsBytes[++formatPos]) {

                case 'Y': {

                    tmpValPos = valPos + Math.min(4, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long year = parseResult[0];
                    if (tmpValPos - valPos <= 2) {
                        if ((year += 1900) < 1970) {
                            year += 100;
                        }
                    }
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    t.setYear(year);
                    break;
                }

                case 'y': {

                    tmpValPos = valPos + Math.min(2, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long year = parseResult[0];
                    if ((year += 1900) < 1970) {
                        year += 100;
                    }
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    t.setYear(year);
                    break;
                }

                case 'm':
                case 'c': {
                    tmpValPos = valPos + Math.min(2, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long month = parseResult[0];
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    t.setMonth(month);
                    break;
                }
                case 'M': {
                    int[] checkResult = checkWord(MONTHS, timestampAsBytes, valPos, valEnd);
                    long month = checkResult[0];
                    if (month <= 0) {
                        return null;
                    }
                    valPos = checkResult[1];
                    t.setMonth(month);
                    break;
                }
                case 'b': {
                    int[] checkResult = checkWord(MONTHS_ABBR, timestampAsBytes, valPos, valEnd);
                    long month = checkResult[0];
                    if (month <= 0) {
                        return null;
                    }
                    valPos = checkResult[1];
                    t.setMonth(month);
                    break;
                }

                case 'd':
                case 'e': {
                    tmpValPos = valPos + Math.min(2, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long day = parseResult[0];
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    t.setDay(day);
                    break;
                }
                case 'D': {
                    tmpValPos = valPos + Math.min(2, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long day = parseResult[0];

                    valPos = (int) parseResult[1] + Math.min(valEnd - tmpValPos, 2);
                    error = (int) parseResult[2];
                    t.setDay(day);
                    break;
                }

                case 'h':
                case 'I':
                case 'l':
                    usaTime = true;
                case 'k':
                case 'H': {
                    tmpValPos = valPos + Math.min(2, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long hour = parseResult[0];
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    t.setHour(hour);
                    break;
                }

                case 'i': {
                    tmpValPos = valPos + Math.min(2, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long minute = parseResult[0];
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    t.setMinute(minute);
                    break;
                }

                case 's':
                case 'S': {
                    tmpValPos = valPos + Math.min(2, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    long second = parseResult[0];
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    t.setSecond(second);
                    break;
                }

                case 'f': {
                    tmpValPos = valEnd;
                    long secondPart;
                    if (tmpValPos - valPos > 6) {
                        tmpValPos = valPos + 6;
                    }
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    secondPart = parseResult[0];
                    int fracPart = 6 - (tmpValPos - valPos);
                    if (fracPart > 0) {
                        secondPart *= LOG_10[fracPart];
                    }
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    secondPart *= 1000L;
                    t.setSecondPart(secondPart);
                    break;
                }

                case 'p': {
                    if (valLen < 2 || !usaTime) {
                        return null;
                    }
                    if ((timestampAsBytes[valPos] == 'P' || timestampAsBytes[valPos] == 'p')
                        && (timestampAsBytes[valPos + 1] == 'M' || timestampAsBytes[valPos + 1] == 'm')) {
                        dayPart = 12;
                    } else if ((timestampAsBytes[valPos] != 'P' && timestampAsBytes[valPos] != 'p')
                        || (timestampAsBytes[valPos + 1] != 'M' && timestampAsBytes[valPos + 1] != 'm')) {
                        return null;
                    }
                    valPos += 2;
                    break;
                }

                case 'W': {
                    int[] res = checkWord(WEEK_DAYS, timestampAsBytes, valPos, valEnd);
                    if (res[0] <= 0) {
                        return null;
                    }
                    weekDay = res[0];
                    valPos = res[1];
                    break;
                }
                case 'a': {
                    int[] res = checkWord(WEEK_DAYS_ABBR, timestampAsBytes, valPos, valEnd);
                    if (res[0] <= 0) {
                        return null;
                    }
                    weekDay = res[0];
                    valPos = res[1];
                    break;
                }
                case 'w': {
                    tmpValPos = valPos + 1;
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    weekDay = (int) parseResult[0];
                    if (weekDay < 0 || weekDay >= 7) {
                        return null;
                    }
                    if (weekDay == 0) {
                        weekDay = 7;
                    }
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    break;
                }

                case 'j': {
                    tmpValPos = valPos + Math.min(valLen, 3);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    yearDay = (int) parseResult[0];
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    break;
                }

                case 'V':
                case 'U':
                case 'v':
                case 'u': {
                    isSundayFirstDayOfWeek =
                        formatAsBytes[formatPos] == 'U' || formatAsBytes[formatPos] == 'V';
                    isStrictWeekNumber = formatAsBytes[formatPos] == 'V' || formatAsBytes[formatPos] == 'v';
                    tmpValPos = valPos + Math.min(valLen, 2);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    weekNumber = (int) parseResult[0];
                    if (weekNumber < 0 || (isStrictWeekNumber && weekNumber == 0) || weekNumber > 53) {
                        return null;
                    }
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    break;
                }

                case 'X':
                case 'x': {
                    isStrictWeekNumberYearType = formatAsBytes[formatPos] == 'X';
                    tmpValPos = valPos + Math.min(4, valLen);
                    long[] parseResult = StringNumericParser.parseString(timestampAsBytes, valPos, tmpValPos);
                    isStrictWeekNumberYear = (int) parseResult[0];
                    valPos = (int) parseResult[1];
                    error = (int) parseResult[2];
                    break;
                }

                case '.': {
                    while (valPos < valEnd && isPunctuation(timestampAsBytes[valPos])) {
                        valPos++;
                    }
                    break;
                }
                case '@': {
                    while (valPos < valEnd && isAlpha(timestampAsBytes[valPos])) {
                        valPos++;
                    }
                    break;
                }
                case '#': {
                    while (valPos < valEnd && isDigit(timestampAsBytes[valPos])) {
                        valPos++;
                    }
                    break;
                }
                default:
                    return null;
                }

                if (error == 1) {
                    return null;
                }
            } else if (!isSpace(formatAsBytes[formatPos])) {

                if (timestampAsBytes[valPos] != formatAsBytes[formatPos]) {
                    return null;
                }
                valPos++;
            }
        }

        if (usaTime) {
            if (t.getHour() > 12 || t.getHour() < 1) {
                return null;
            }
            t.setHour(t.getHour() % 12 + dayPart);
        }

        if (yearDay > 0) {
            long days = MySQLTimeCalculator.calDayNumber(t.getYear(), 1, 1) + yearDay - 1;
            if (days <= 0 || days > MySQLTimeTypeUtil.MAX_DAY_NUMBER) {
                return null;
            }
            MySQLTimeCalculator.getDateFromDayNumber(days, t);
        }

        if (weekNumber >= 0 && weekDay != 0) {
            long days, weekday_b;

            if ((isStrictWeekNumber && (isStrictWeekNumberYear < 0
                || isStrictWeekNumberYearType != isSundayFirstDayOfWeek))
                || (!isStrictWeekNumber)) {
                return null;
            }

            days = MySQLTimeCalculator.calDayNumber((isStrictWeekNumber ? isStrictWeekNumberYear : t.getYear()), 1, 1);

            weekday_b = MySQLTimeCalculator.calWeekDay(days, isSundayFirstDayOfWeek);

            if (isSundayFirstDayOfWeek) {
                days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (weekNumber - 1) * 7 + weekDay % 7;
            } else {
                days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (weekNumber - 1) * 7 + (weekDay - 1);
            }

            if (days <= 0 || days > MySQLTimeTypeUtil.MAX_DAY_NUMBER) {
                return null;
            }
            MySQLTimeCalculator.getDateFromDayNumber(days, t);
        }

        if (t.getMonth() > 12 || t.getDay() > 31 || t.getHour() > 23 || t.getMinute() > 59 || t.getSecond() > 59) {
            return null;
        }

        return t;
    }

    private static int[] checkWord(byte[][] lib, byte[] timestampAsBytes, final int startPos, final int endPos) {
        int[] res = new int[2];
        int alphaEnd;

        for (alphaEnd = startPos; alphaEnd < endPos && isAlpha(timestampAsBytes[alphaEnd]); alphaEnd++) {
        }

        int foundCount = 0, foundPos = 0;

        for (int wordPos = 0; wordPos < lib.length; wordPos++) {
            byte[] word = lib[wordPos];

            int alphaPos = 0, matchPos = 0;
            for (alphaPos = startPos;
                 alphaPos < alphaEnd && toAlphaUpper(timestampAsBytes[alphaPos]) == toAlphaUpper(word[matchPos]);
                 alphaPos++, matchPos++) {
            }
            if (alphaPos == alphaEnd) {
                if (matchPos >= word.length) {

                    res[0] = wordPos + 1;
                    res[1] = alphaEnd;
                    return res;
                }

                foundCount++;
                foundPos = wordPos;
            }
        }
        res[0] = foundCount == 1 ? foundPos : 0;
        res[1] = res[0] > 0 ? alphaEnd : startPos;
        return res;
    }
}
