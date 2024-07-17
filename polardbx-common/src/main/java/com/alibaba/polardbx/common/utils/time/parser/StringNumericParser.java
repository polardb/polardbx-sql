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

import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedLongs;
import io.airlift.slice.Slice;


public class StringNumericParser extends MySQLTimeParserBase {
    private static final long SIGNED_MIN_LONG = -0x7fffffffffffffffL - 1;
    private static final long SIGNED_MAX_LONG = 0x7fffffffffffffffL;
    private static final long UNSIGNED_LONG_MAX = 0xffffffffffffffffL;
    private static final int SIGNED_INT_32_MIN = ~0x7FFFFFFF;
    private static final int DIGITS_IN_UNSIGNED_LONG = 20;
    private static final int INIT_CNT = 9;
    private static final long FACTOR = 1000000000L;
    private static final long FACTOR1 = 10000000000L;
    private static final long FACTOR2 = 100000000000L;

    private static final long[] L_FACTOR = {
        1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L
    };

    private static final long[][] OVERFLOW_THRESHOLD = {
        {
            UnsignedLongs.divide(0x8000000000000000L, FACTOR2),
            UnsignedLongs.divide(0xffffffffffffffffL, FACTOR2)
        },
        {
            UnsignedLongs.divide(UnsignedLongs.remainder(0x8000000000000000L, FACTOR2), 100),
            UnsignedLongs.divide(UnsignedLongs.remainder(0xffffffffffffffffL, FACTOR2), 100)
        },
        {
            UnsignedLongs.remainder(0x8000000000000000L, 100),
            UnsignedLongs.remainder(0xffffffffffffffffL, 100)
        },
    };

    private enum EndState {
        END_I, END_I_J, END_3, END_4
    }

    public static final int NUMERIC_INDEX = 0;
    public static final int POSITION_INDEX = 1;
    public static final int ERROR_INDEX = 2;

    public static final int MY_ERRNO_EDOM = 3;
    public static final int MY_ERRNO_ERANGE = 4;

    public static final long CUT_OFF = UnsignedLongs.divide(UNSIGNED_LONG_MAX, 10);
    public static final long CUT_LIM = UnsignedLongs.remainder(UNSIGNED_LONG_MAX, 10);

    private static final long[] D_10 =
        {
            1,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000,
            10000000000L,
            100000000000L,
            1000000000000L,
            10000000000000L,
            100000000000000L,
            1000000000000000L,
            10000000000000000L,
            100000000000000000L,
            1000000000000000000L,

        };

    public static long[] parseString(byte[] bytes, final int startPos, final int endPos) {
        Preconditions.checkArgument(startPos <= endPos);
        int pos = startPos;
        final int end = endPos;
        long iMax = 0L, jMax = 0L, kMax = 0L;
        long i = 0L, j = 0L, k = 0L;
        long[] result = {0L, startPos, 0L};

        while (pos < end && (bytes[pos] == ' ' || bytes[pos] == '\t')) {
            pos++;
        }
        if (pos == end) {
            result[ERROR_INDEX] = 1L;
            return result;
        }

        boolean isNeg = false;
        if (bytes[pos] == '-') {
            isNeg = true;
            if (++pos == end) {
                result[ERROR_INDEX] = 1L;
                return result;
            }
            iMax = OVERFLOW_THRESHOLD[0][0];
            jMax = OVERFLOW_THRESHOLD[1][0];
            kMax = OVERFLOW_THRESHOLD[2][0];
        } else {
            if (bytes[pos] == '+' && ++pos == end) {
                result[ERROR_INDEX] = 1L;
                return result;
            }
            iMax = OVERFLOW_THRESHOLD[0][1];
            jMax = OVERFLOW_THRESHOLD[1][1];
            kMax = OVERFLOW_THRESHOLD[2][1];
        }

        int numberEnd;
        int value;

        if (bytes[pos] == '0') {
            i = 0;
            while (pos != end && bytes[pos] == '0') {
                if (++pos == end) {
                    result[POSITION_INDEX] = pos;
                    return result;
                }
            }
            numberEnd = pos + INIT_CNT;
        } else {

            if ((value = bytes[pos] - '0') > 9 || value < 0) {
                result[ERROR_INDEX] = 1L;
                return result;
            }
            i = value;
            ++pos;
            numberEnd = pos + INIT_CNT - 1;
        }

        if (numberEnd > end) {
            numberEnd = end;
        }
        for (; pos < numberEnd; pos++) {
            if ((value = bytes[pos] - '0') > 9 || value < 0) {
                end(result, isNeg, i, j, k, startPos, pos, EndState.END_I);
                return result;
            }
            i = i * 10 + value;
        }
        if (pos == end) {
            end(result, isNeg, i, j, k, startPos, pos, EndState.END_I);
            return result;
        }

        j = 0L;
        int newStartPos = pos;
        numberEnd = pos + INIT_CNT;
        if (numberEnd > end) {
            numberEnd = end;
        }
        while (pos < numberEnd) {
            if ((value = bytes[pos] - '0') > 9 || value < 0) {
                end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_I_J);
                return result;
            }
            j = j * 10 + value;
            pos++;
        }
        if (pos == end) {
            if (pos != newStartPos + INIT_CNT) {
                end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_I_J);
            } else {
                end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_3);
            }
            return result;
        }
        if ((value = bytes[pos] - '0') > 9 || value < 0) {
            end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_3);
            return result;
        }

        k = value;
        if (++pos == end || (value = bytes[pos] - '0') > 9 || value < 0) {
            end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_4);
            return result;
        }
        k = 10 * k + value;
        ++pos;
        result[POSITION_INDEX] = pos;

        if (pos != end && (value = (bytes[pos] - '0')) <= 9 && value >= 0) {

            overflowValue(result, isNeg);
            return result;
        }

        if (UnsignedLongs.compare(i, iMax) > 0
            || (UnsignedLongs.compare(i, iMax) == 0 && (UnsignedLongs.compare(j, jMax) > 0
            || ((UnsignedLongs.compare(j, jMax) == 0 && (UnsignedLongs.compare(k, kMax) > 0)))))) {
            overflowValue(result, isNeg);
            return result;
        }

        result[NUMERIC_INDEX] = i * FACTOR2 + j * 100 + k;
        return result;
    }

    public static long[] parseString(Slice slice) {
        if (slice == null) {
            return null;
        }
        int startPos = 0;
        int pos = startPos;
        final int end = slice.length();
        long iMax = 0L, jMax = 0L, kMax = 0L;
        long i = 0L, j = 0L, k = 0L;
        long[] result = {0L, slice.length(), 0L};

        while (pos < end && (slice.getByte(pos) == ' ' || slice.getByte(pos) == '\t')) {
            pos++;
        }
        if (pos == end) {
            result[ERROR_INDEX] = 1L;
            return result;
        }

        boolean isNeg = false;
        if (slice.getByte(pos) == '-') {
            isNeg = true;
            if (++pos == end) {
                result[ERROR_INDEX] = 1L;
                return result;
            }
            iMax = OVERFLOW_THRESHOLD[0][0];
            jMax = OVERFLOW_THRESHOLD[1][0];
            kMax = OVERFLOW_THRESHOLD[2][0];
        } else {
            if (slice.getByte(pos) == '+' && ++pos == end) {
                result[ERROR_INDEX] = 1L;
                return result;
            }
            iMax = OVERFLOW_THRESHOLD[0][1];
            jMax = OVERFLOW_THRESHOLD[1][1];
            kMax = OVERFLOW_THRESHOLD[2][1];
        }

        int numberEnd;
        int value;

        if (slice.getByte(pos) == '0') {
            i = 0;
            while (pos != end && slice.getByte(pos) == '0') {
                if (++pos == end) {
                    result[POSITION_INDEX] = pos;
                    return result;
                }
            }
            numberEnd = pos + INIT_CNT;
        } else {

            if ((value = slice.getByte(pos) - '0') > 9 || value < 0) {
                result[ERROR_INDEX] = 1L;
                return result;
            }
            i = value;
            ++pos;
            numberEnd = pos + INIT_CNT - 1;
        }

        if (numberEnd > end) {
            numberEnd = end;
        }
        for (; pos < numberEnd; pos++) {
            if ((value = slice.getByte(pos) - '0') > 9 || value < 0) {
                end(result, isNeg, i, j, k, startPos, pos, EndState.END_I);
                return result;
            }
            i = i * 10 + value;
        }
        if (pos == end) {
            end(result, isNeg, i, j, k, startPos, pos, EndState.END_I);
            return result;
        }

        j = 0L;
        int newStartPos = pos;
        numberEnd = pos + INIT_CNT;
        if (numberEnd > end) {
            numberEnd = end;
        }
        while (pos < numberEnd) {
            if ((value = slice.getByte(pos) - '0') > 9 || value < 0) {
                end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_I_J);
                return result;
            }
            j = j * 10 + value;
            pos++;
        }
        if (pos == end) {
            if (pos != newStartPos + INIT_CNT) {
                end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_I_J);
            } else {
                end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_3);
            }
            return result;
        }
        if ((value = slice.getByte(pos) - '0') > 9 || value < 0) {
            end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_3);
            return result;
        }

        k = value;
        if (++pos == end || (value = slice.getByte(pos) - '0') > 9 || value < 0) {
            end(result, isNeg, i, j, k, newStartPos, pos, EndState.END_4);
            return result;
        }
        k = 10 * k + value;
        ++pos;
        result[POSITION_INDEX] = pos;

        if (pos != end && (value = (slice.getByte(pos) - '0')) <= 9 && value >= 0) {

            overflowValue(result, isNeg);
            return result;
        }

        if (UnsignedLongs.compare(i, iMax) > 0
            || (UnsignedLongs.compare(i, iMax) == 0 && (UnsignedLongs.compare(j, jMax) > 0
            || ((UnsignedLongs.compare(j, jMax) == 0 && (UnsignedLongs.compare(k, kMax) > 0)))))) {
            overflowValue(result, isNeg);
            return result;
        }

        result[NUMERIC_INDEX] = i * FACTOR2 + j * 100 + k;
        return result;
    }

    public static long[] parseString(byte[] bytes) {
        return parseString(bytes, 0, bytes.length);
    }

    public static void parseStringWithRound(byte[] bytes, final int startPos, final int endPos,
                                            final boolean isUnsigned, long[] results) {
        int pos = startPos;

        while (pos < endPos && (bytes[pos] == ' ' || bytes[pos] == '\t')) {
            pos++;
        }

        if (pos >= endPos) {

            results[NUMERIC_INDEX] = 0;
            results[ERROR_INDEX] = MY_ERRNO_EDOM;
            results[POSITION_INDEX] = pos;
            return;
        }

        boolean isNeg;
        if ((isNeg = (bytes[pos] == '-')) || (bytes[pos] == '+')) {
            if (++pos == endPos) {
                results[NUMERIC_INDEX] = 0;
                results[ERROR_INDEX] = MY_ERRNO_EDOM;
                results[POSITION_INDEX] = pos;
                return;
            }
        }

        int begin = pos;
        int nextEnd = (pos + 9) > endPos ? endPos : (pos + 9);

        int value;
        int intResult;
        long longResult;
        for (intResult = 0; pos < nextEnd && (value = bytes[pos] - '0') < 10 && value >= 0; pos++) {
            intResult = intResult * 10 + value;
        }

        if (pos >= endPos) {
            if (isNeg) {
                if (isUnsigned) {
                    results[NUMERIC_INDEX] = 0;
                    results[ERROR_INDEX] = MY_ERRNO_ERANGE;
                    results[POSITION_INDEX] = pos;
                    return;
                } else {
                    results[NUMERIC_INDEX] = -(long) intResult;
                    results[ERROR_INDEX] = 0;
                    results[POSITION_INDEX] = pos;
                    return;
                }
            } else {
                results[NUMERIC_INDEX] = intResult;
                results[ERROR_INDEX] = 0;
                results[POSITION_INDEX] = pos;
                return;
            }
        }

        int digits = pos - begin;
        int dotPos = 0;
        int addOn = 0;
        int shift = 0;
        boolean needHandleExp = false;
        longResult = intResult;

        for (; pos < endPos; pos++) {
            if ((value = bytes[pos] - '0') < 10 && value >= 0) {
                if (UnsignedLongs.compare(longResult, CUT_OFF) < 0
                    || (UnsignedLongs.compare(longResult, CUT_OFF) == 0
                    && UnsignedLongs.compare(value, CUT_LIM) <= 0)) {
                    longResult = longResult * 10 + value;
                    digits++;
                    continue;
                }

                if (UnsignedLongs.compare(longResult, CUT_OFF) == 0) {
                    longResult = UNSIGNED_LONG_MAX;
                    addOn = 1;
                    pos++;
                } else {
                    addOn = (bytes[pos] - '5') >= 0 ? 1 : 0;
                }
                if (dotPos == 0) {
                    for (; pos < endPos && (value = bytes[pos] - '0') < 10 && value >= 0; shift++, pos++) {
                        if (pos < endPos && Byte.toUnsignedInt(bytes[pos]) == '.') {
                            pos++;
                            while (pos < endPos && (value = bytes[pos] - '0') < 10 && value >= 0) {
                                pos++;
                            }
                        }
                    }
                } else {
                    shift = dotPos - pos;
                    while (pos < endPos && (value = bytes[pos] - '0') < 10 && value >= 0) {
                        pos++;
                    }
                }

                needHandleExp = true;
                break;
            }

            if (bytes[pos] == '.') {
                if (dotPos != 0) {

                    addOn = 0;

                    needHandleExp = true;
                    break;
                } else {
                    dotPos = pos + 1;
                }
                continue;
            }

            break;
        }
        if (!needHandleExp) {

            shift = dotPos != 0 ? (dotPos - pos) : 0;
            addOn = 0;
        }

        if (digits == 0) {
            pos = begin;

            results[NUMERIC_INDEX] = 0;
            results[ERROR_INDEX] = MY_ERRNO_EDOM;
            results[POSITION_INDEX] = pos;
            return;
        }

        if (pos < endPos
            && (bytes[pos] == 'e' || bytes[pos] == '+')) {
            pos++;
            if (pos < endPos) {
                long exponent;
                boolean isNegativeExp = bytes[pos] == '-';
                if (isNegativeExp || bytes[pos] == '+') {
                    if (++pos == endPos) {

                        handleResult(isUnsigned, results, pos, isNeg, longResult);
                        return;
                    }
                }
                for (exponent = 0;
                     pos < endPos && (value = bytes[pos] - '0') < 10 && value >= 0;
                     pos++) {
                    if (exponent <= (SIGNED_MAX_LONG - value) / 10) {
                        exponent = exponent * 10 + value;
                    } else {
                        // too big
                        handleRetTooBig(results, isUnsigned, isNeg, pos);
                        return;
                    }
                }
                shift += isNegativeExp ? -exponent : exponent;
            }
        }

        if (shift == 0) {
            if (addOn != 0) {
                if (UnsignedLongs.compare(longResult, UNSIGNED_LONG_MAX) == 0) {
                    // too big
                    handleRetTooBig(results, isUnsigned, isNeg, pos);
                    return;
                }
                longResult++;
            }

            handleResult(isUnsigned, results, pos, isNeg, longResult);
            return;
        }

        if (shift < 0) {
            long d, r;
            if (shift == SIGNED_INT_32_MIN || -shift >= DIGITS_IN_UNSIGNED_LONG) {

                results[NUMERIC_INDEX] = 0;
                results[ERROR_INDEX] = 0;
                results[POSITION_INDEX] = pos;
                return;
            }
            d = D_10[-shift];
            r = UnsignedLongs.remainder(longResult, d) * 2;
            longResult = UnsignedLongs.divide(longResult, d);
            if (UnsignedLongs.compare(r, d) >= 0) {
                longResult++;
            }

            handleResult(isUnsigned, results, pos, isNeg, longResult);
            return;
        }

        if (shift > DIGITS_IN_UNSIGNED_LONG) {
            if (longResult == 0) {

                handleResult(isUnsigned, results, pos, isNeg, longResult);
                return;
            }
            // too big
            handleRetTooBig(results, isUnsigned, isNeg, pos);
            return;
        }

        for (; shift > 0; shift--, longResult *= 10) {
            if (UnsignedLongs.compare(longResult, CUT_OFF) > 0) {
                // Overflow, number too big
                handleRetTooBig(results, isUnsigned, isNeg, pos);
                return;
            }
        }

        handleResult(isUnsigned, results, pos, isNeg, longResult);

    }

    public static void handleRetTooBig(long[] results, boolean isUnsigned, boolean isNeg, int pos) {
        long result;
        if (InstanceVersion.isMYSQL80()) {
            result = isUnsigned ?
                (isNeg ? 0 : UNSIGNED_LONG_MAX) :
                (isNeg ? SIGNED_MIN_LONG : SIGNED_MAX_LONG);
        } else {
            result = isUnsigned ? UNSIGNED_LONG_MAX :
                (isNeg ? SIGNED_MIN_LONG : SIGNED_MAX_LONG);
        }

        results[NUMERIC_INDEX] = result;
        results[ERROR_INDEX] = MY_ERRNO_ERANGE;
        results[POSITION_INDEX] = pos;
    }

    public static double parseStringToDouble(byte[] bytes, int offset, int len) {
        if (bytes == null) {
            return 0d;
        }

        try {
            return Double.valueOf(new String(bytes, offset, len));
        } catch (Throwable t) {
            DecimalStructure tmp = new DecimalStructure();
            DecimalConverter.parseString(bytes, offset, len, tmp, false);
            return DecimalConverter.decimalToDouble(tmp);
        }
    }

    private static void handleResult(boolean isUnsigned, long[] results, int pos, boolean isNeg, long longResult) {

        results[POSITION_INDEX] = pos;
        if (!isUnsigned) {
            if (isNeg) {
                if (UnsignedLongs.compare(longResult, SIGNED_MIN_LONG) > 0) {
                    results[ERROR_INDEX] = MY_ERRNO_ERANGE;
                    results[NUMERIC_INDEX] = SIGNED_MIN_LONG;
                    return;
                }
                results[ERROR_INDEX] = 0;
                results[NUMERIC_INDEX] = -longResult;
                return;
            } else {
                if (UnsignedLongs.compare(longResult, SIGNED_MAX_LONG) > 0) {
                    results[ERROR_INDEX] = MY_ERRNO_ERANGE;
                    results[NUMERIC_INDEX] = SIGNED_MAX_LONG;
                    return;
                }
                results[ERROR_INDEX] = 0;
                results[NUMERIC_INDEX] = longResult;
                return;
            }
        }

        if (isNeg && longResult != 0) {
            results[ERROR_INDEX] = MY_ERRNO_ERANGE;
            results[NUMERIC_INDEX] = 0;
            return;
        }
        results[ERROR_INDEX] = 0;
        results[NUMERIC_INDEX] = longResult;
    }

    private static void end(long[] result, boolean isNeg, long i, long j, long k, int start, int pos, EndState state) {
        long li;
        switch (state) {
        case END_I:
            result[NUMERIC_INDEX] = (isNeg ? -i : i);
            result[POSITION_INDEX] = pos;
            break;
        case END_I_J:
            li = i * L_FACTOR[pos - start] + j;
            result[NUMERIC_INDEX] = (isNeg ? -li : li);
            result[POSITION_INDEX] = pos;
            break;
        case END_3:
            li = i * FACTOR + j;
            result[NUMERIC_INDEX] = (isNeg ? -li : li);
            result[POSITION_INDEX] = pos;
            break;
        case END_4:
            li = i * FACTOR1 + j * 10 + k;
            result[POSITION_INDEX] = pos;
            if (isNeg && UnsignedLongs.compare(li, Long.MIN_VALUE) > 0) {

                result[NUMERIC_INDEX] = SIGNED_MIN_LONG;
                result[ERROR_INDEX] = 1L;
                break;
            }
            result[NUMERIC_INDEX] = (isNeg ? -li : li);
            break;
        default:
        }
    }

    private static void overflowValue(long[] result, boolean isNeg) {
        result[NUMERIC_INDEX] = isNeg ? SIGNED_MIN_LONG : (Long.MAX_VALUE ^ Long.MIN_VALUE);
        result[ERROR_INDEX] = 1;
    }

    public static Long simplyParseLong(String val) {
        long longValue = 0;
        if (val != null) {
            try {
                longValue = Long.parseLong(val);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return longValue;
    }
}
