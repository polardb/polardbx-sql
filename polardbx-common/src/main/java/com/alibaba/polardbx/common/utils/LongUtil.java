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

package com.alibaba.polardbx.common.utils;

import javax.annotation.Nonnull;

/**
 * @version 1.0
 */
public class LongUtil {

    protected final static byte[] DIGIT_TENS = {
        '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
        '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
        '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
        '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
        '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
        '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
        '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
        '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
        '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
        '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
    };

    protected final static byte[] DIGIT_ONES = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    };

    final static byte[] digits = {
        '0', '1', '2', '3', '4', '5',
        '6', '7', '8', '9', 'a', 'b',
        'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'q', 'r', 's', 't',
        'u', 'v', 'w', 'x', 'y', 'z'
    };

    static void getBytes(long i, int index, byte[] buf) {
        long q;
        int r;
        int charPos = index;
        byte sign = 0;

        if (i < 0) {
            sign = '-';
            i = -i;
        }

        // Get 2 digits/iteration using longs until quotient fits into an int
        while (i > Integer.MAX_VALUE) {
            q = i / 100;
            // really: r = i - (q * 100);
            r = (int) (i - ((q << 6) + (q << 5) + (q << 2)));
            i = q;
            buf[--charPos] = DIGIT_ONES[r];
            buf[--charPos] = DIGIT_TENS[r];
        }

        // Get 2 digits/iteration using ints
        int q2;
        int i2 = (int) i;
        while (i2 >= 65536) {
            q2 = i2 / 100;
            // really: r = i2 - (q * 100);
            r = i2 - ((q2 << 6) + (q2 << 5) + (q2 << 2));
            i2 = q2;
            buf[--charPos] = DIGIT_ONES[r];
            buf[--charPos] = DIGIT_TENS[r];
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i2 <= 65536, i2);
        for (; ; ) {
            q2 = (i2 * 52429) >>> (16 + 3);
            r = i2 - ((q2 << 3) + (q2 << 1));  // r = i2-(q2*10) ...
            buf[--charPos] = digits[r];
            i2 = q2;
            if (i2 == 0) {
                break;
            }
        }
        if (sign != 0) {
            buf[--charPos] = sign;
        }
    }

    // Requires positive x
    static int bytesSize(long x) {
        long p = 10;
        for (int i = 1; i < 19; i++) {
            if (x < p) {
                return i;
            }
            p = 10 * p;
        }
        return 19;
    }

    final static byte[] MinValue = "-9223372036854775808".getBytes();

    public static byte[] toBytes(long i) {
        if (i == Long.MIN_VALUE) {
            return MinValue;
        }
        int size = (i < 0) ? bytesSize(-i) + 1 : bytesSize(i);
        byte[] buf = new byte[size];
        getBytes(i, size, buf);
        return buf;
    }

    public static byte[] toUnsignedBytes(long i) {
        if (i >= 0) {
            return toBytes(i);
        } else {
            long quot = (i >>> 1) / 5;
            long rem = i - quot * 10;
            // TODO: May optimize?
            byte[] first = toBytes(quot);
            byte[] last = toBytes(rem);
            byte[] full = new byte[first.length + last.length];
            System.arraycopy(first, 0, full, 0, first.length);
            System.arraycopy(last, 0, full, first.length, last.length);
            return full;
        }
    }

    /**
     * fast path for year/month/day/hour/minute/second to '0'-padding bytes
     *
     * @param value must be non-negative
     */
    public static byte[] fastGetSmallLongBytesForDate(long value, int expectDigits) {
        int i2, q2, r;

        if (expectDigits == 2) {
            if (value > 99) {
                return Long.toString(value).getBytes();
            }
            byte[] zeroPaddingBytes = new byte[] {'0', '0'};
            if (value == 0L) {
                return zeroPaddingBytes;
            }
            value = -value;
            i2 = (int) value;
            q2 = i2 / 10;
            r = (q2 * 10) - i2;
            zeroPaddingBytes[--expectDigits] = (byte) ('0' + r);
            if (q2 < 0) {
                zeroPaddingBytes[--expectDigits] = (byte) ('0' - q2);
            }
            return zeroPaddingBytes;
        }
        if (expectDigits == 4) {
            if (value > 9999) {
                return Long.toString(value).getBytes();
            }
            byte[] zeroPaddingBytes = new byte[] {'0', '0', '0', '0'};
            if (value == 0L) {
                return zeroPaddingBytes;
            }
            value = -value;
            i2 = (int) value;
            q2 = i2 / 100;
            r = (q2 * 100) - i2;
            i2 = q2;
            zeroPaddingBytes[--expectDigits] = LongUtil.DIGIT_ONES[r];
            zeroPaddingBytes[--expectDigits] = DIGIT_TENS[r];
            q2 = i2 / 10;
            r = (q2 * 10) - i2;
            zeroPaddingBytes[--expectDigits] = (byte) ('0' + r);
            if (q2 < 0) {
                zeroPaddingBytes[--expectDigits] = (byte) ('0' - q2);
            }
            return zeroPaddingBytes;
        }

        return Long.toString(value).getBytes();
    }

    /**
     * WARNING:
     * this is a fast path
     * do not use it in normal cases
     */
    public static int fastStringBytesToInt(@Nonnull byte[] strBytes) {
        return fastStringBytesToInt(strBytes, 0, strBytes.length);
    }

    /**
     * WARNING:
     * this is a fast path
     * do not use it in normal cases (skipped checking some overflow conditions)
     *
     * @param strBytes a string-representation byte array without sign
     * @param start start position
     * @param len string length
     */
    public static int fastStringBytesToInt(@Nonnull byte[] strBytes, int start, int len) {
        if (strBytes.length < start + len) {
            throw new IndexOutOfBoundsException("start: " + start +
                ", len: " + len + "is out of: " + strBytes.length);
        }
        if (len == 0) {
            throw new NumberFormatException("empty number string");
        }
        final int RADIX = 10;
        final int limit = -Integer.MAX_VALUE;
        final int multmin = limit / RADIX;

        int result = 0;
        int i = start;
        int digit;

        while (i < start + len) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            digit = strBytes[i++] - '0';

            if (digit < 0 || digit > 9 || result < multmin) {
                throw new NumberFormatException(new String(strBytes));
            }
            result *= RADIX;
            result -= digit;
        }
        return -result;
    }
}
