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

import org.apache.commons.lang3.math.NumberUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

public class BigDecimalUtil {

    private static final long BITS_PER_DIGIT = 3402;
    private static final int DIGIT_PER_INT = 9;
    private static final int DIGIT_PER_LONG = 18;
    private static final int RADIX_10 = 1000000000;
    private static final BigInteger LONG_RADIX_10 = BigInteger.valueOf(0xde0b6b3a7640000L);

    private static final int MAX_MAG_LENGTH = Integer.MAX_VALUE / Integer.SIZE + 1; // (1 << 26)

    static final long INFLATED = Long.MIN_VALUE;
    static final long LONG_MASK = 0xffffffffL;
    static final byte[] ZERO_BYTE = new byte[] {'0'};
    private static final int SCHOENHAGE_BASE_CONVERSION_THRESHOLD = 20;

    /**
     * for XRowSet fastGetBytes only
     */
    public static byte[] fastGetBigDecimalStringBytes(byte[] decimalStrBytes, int scale) throws IOException {
        int len = decimalStrBytes.length;
        if (len == 0) {
            throw new NumberFormatException("Empty BigDecimal");
        }
        int cursor = 0;
        int numDigits = 0;
        // 1: positive, 0: zero, -1: negative
        int sign = 1;
        int signum;
        int[] mag;
        if (decimalStrBytes[0] == '-') {
            sign = -1;
            cursor = 1;
        } else if (decimalStrBytes[0] == '+') {
            cursor = 1;
        }

        if (cursor >= len) {
            throw new NumberFormatException(
                "Illegal BigDecimal: " + new String(decimalStrBytes) + ", scale: " + scale);
        }
        // Skip leading zeros and compute number of digits in magnitude
        while (cursor < len && decimalStrBytes[cursor] == '0') {
            cursor++;
        }

        if (cursor == len) {
            signum = 0;
            mag = new int[0];
        } else {
            numDigits = len - cursor;
            signum = sign;

            // Pre-allocate array of expected size. May be too large
            // but can never be too small. Typically exact.
            long numBits = ((numDigits * BITS_PER_DIGIT) >>> 10) + 1;
            if (numBits + 31 >= (1L << 32)) {
                throw new ArithmeticException("Overflow supported range");
            }
            int numWords = (int) (numBits + 31) >>> 5;
            int[] magnitude = new int[numWords];

            int firstGroupLen = numDigits % DIGIT_PER_INT;
            if (firstGroupLen == 0) {
                firstGroupLen = DIGIT_PER_INT;
            }
            magnitude[numWords - 1] = LongUtil.fastStringBytesToInt(decimalStrBytes, cursor, firstGroupLen);
            cursor += firstGroupLen;
            if (magnitude[numWords - 1] < 0) {
                throw new NumberFormatException("Illegal digit");
            }

            // Process remaining digit groups
            int groupVal = 0;
            while (cursor < len) {
                groupVal = LongUtil.fastStringBytesToInt(decimalStrBytes, cursor, DIGIT_PER_INT);
                cursor += DIGIT_PER_INT;
                if (groupVal < 0) {
                    throw new NumberFormatException("Illegal digit");
                }
                destructiveMulAdd(magnitude, RADIX_10, groupVal);
            }
            // Required for cases where the array was overallocated.
            mag = trustedStripLeadingZeroInts(magnitude);
            if (mag.length > MAX_MAG_LENGTH || mag.length == MAX_MAG_LENGTH && mag[0] < 0) {
                throw new ArithmeticException("Overflow supported range");
            }
        }

        long intCompact = compactVal(mag, signum);

        if (scale == 0) {
            return (intCompact != INFLATED) ?
                LongUtil.toBytes(intCompact) :
                toBigIntegerBytes(signum, mag, decimalStrBytes);
        }

        if (scale == 2 &&
            intCompact >= 0 && intCompact < Integer.MAX_VALUE) {
            int lowInt = (int) intCompact % 100;
            int highInt = (int) intCompact / 100;

            final int MAX_LENGTH = 12;  // -2147483648 and a decimal point
            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream(MAX_LENGTH);

            byteBuffer.write(LongUtil.toBytes(highInt));
            byteBuffer.write('.');
            byteBuffer.write(LongUtil.DIGIT_TENS[lowInt]);
            byteBuffer.write(LongUtil.DIGIT_ONES[lowInt]);

            return byteBuffer.toByteArray();
        }

        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream(32); // 32 is an approximate length
        byte[] cmpBytes;
        int offset;  // offset is the starting index for coeff array
        // Get the significand as an absolute value
        if (intCompact != INFLATED) {
            // All non-negative longs can be made to fit into 19 character array
            cmpBytes = new byte[19];
            offset = putIntCompact(Math.abs(intCompact), cmpBytes);
        } else {
            offset = 0;
            cmpBytes = toBigIntegerBytes(1, mag, decimalStrBytes);
        }

        if (signum < 0) {
            byteBuffer.write('-');
        }
        int cmpBytesLen = cmpBytes.length - offset;
        long adjusted = -(long) scale + (cmpBytesLen - 1);
        if ((scale >= 0) && (adjusted >= -6)) { // plain number
            int pad = scale - cmpBytesLen;         // count of padding zeros
            if (pad >= 0) {                     // 0.xxx form
                byteBuffer.write('0');
                byteBuffer.write('.');
                for (; pad > 0; pad--) {
                    byteBuffer.write('0');
                }
                byteBuffer.write(cmpBytes, offset, cmpBytesLen);
            } else {                         // xx.xx form
                byteBuffer.write(cmpBytes, offset, -pad);
                byteBuffer.write('.');
                byteBuffer.write(cmpBytes, -pad + offset, scale);
            }
        } else {
            byteBuffer.write(cmpBytes[offset]);   // first character
            if (cmpBytesLen > 1) {          // more to come
                byteBuffer.write('.');
                byteBuffer.write(cmpBytes, offset + 1, cmpBytesLen - 1);
            }
            if (adjusted != 0) {
                byteBuffer.write('E');
                if (adjusted > 0) {
                    byteBuffer.write('+');
                }
                byteBuffer.write(LongUtil.toBytes(adjusted));
            }
        }
        return byteBuffer.toByteArray();
    }

    private static int putIntCompact(long intCompact, byte[] cmpBytes) {
        long q;
        int r;
        // since we start from the least significant digit, charPos points to
        // the last character in cmpCharArray.
        int pos = cmpBytes.length;

        // Get 2 digits/iteration using longs until quotient fits into an int
        while (intCompact > Integer.MAX_VALUE) {
            q = intCompact / 100;
            r = (int) (intCompact - q * 100);
            intCompact = q;
            cmpBytes[--pos] = LongUtil.DIGIT_ONES[r];
            cmpBytes[--pos] = LongUtil.DIGIT_TENS[r];
        }

        // Get 2 digits/iteration using ints when i2 >= 100
        int q2;
        int i2 = (int) intCompact;
        while (i2 >= 100) {
            q2 = i2 / 100;
            r = i2 - q2 * 100;
            i2 = q2;
            cmpBytes[--pos] = LongUtil.DIGIT_ONES[r];
            cmpBytes[--pos] = LongUtil.DIGIT_TENS[r];
        }

        cmpBytes[--pos] = LongUtil.DIGIT_ONES[i2];
        if (i2 >= 10) {
            cmpBytes[--pos] = LongUtil.DIGIT_TENS[i2];
        }

        return pos;
    }

    private static byte[] toBigIntegerBytes(int signum, int[] mag, byte[] decStringBuilder) throws IOException {
        if (signum == 0) {
            return ZERO_BYTE;
        }
        if (mag.length <= SCHOENHAGE_BASE_CONVERSION_THRESHOLD) {
            return smallBigIntegerToBytes(signum, mag, decStringBuilder);
        }

        // too large number, which is uncommon, fall back to BigInteger
        return new BigInteger(new String(decStringBuilder)).toString().getBytes();
    }

    /**
     * BigInteger超出了Long的范围
     */
    private static byte[] smallBigIntegerToBytes(int signum, int[] mag, byte[] decStringBuilder)
        throws IOException {
        if (signum == 0) {
            return ZERO_BYTE;
        }

        // Compute upper bound on number of digit groups and allocate space
        int maxNumDigitGroups = (4 * mag.length + 6) / 7;
        byte[][] bytesDigitGroup = new byte[maxNumDigitGroups][];

        BigInteger tmp = new BigInteger(new String(decStringBuilder));
        int numGroups = 0;
        while (tmp.signum() != 0) {
            BigInteger[] quotientAndRemainder = tmp.divideAndRemainder(LONG_RADIX_10);
            BigInteger quotient, remainder;
            if (tmp.signum() > 0) {
                quotient = quotientAndRemainder[0];
                remainder = quotientAndRemainder[1];
            } else {
                // tmp.signum() < 0
                quotient = quotientAndRemainder[0].negate();
                remainder = quotientAndRemainder[1].negate();
            }

            bytesDigitGroup[numGroups++] = LongUtil.toBytes(remainder.longValue());
            tmp = quotient;
        }

        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream(numGroups * DIGIT_PER_LONG + 1);
        if (signum < 0) {
            byteBuffer.write('-');
        }
        byteBuffer.write(bytesDigitGroup[numGroups - 1]);

        // Append remaining digit groups padded with leading zeros
        for (int i = numGroups - 2; i >= 0; i--) {
            // Prepend (any) leading zeros for this digit group
            int numLeadingZeros = DIGIT_PER_LONG - bytesDigitGroup[i].length;
            for (int j = 0; j < numLeadingZeros; j++) {
                byteBuffer.write('0');
            }
            byteBuffer.write(bytesDigitGroup[i]);
        }
        return byteBuffer.toByteArray();
    }

    private static long compactVal(int[] mag, int signum) {
        int len = mag.length;
        if (len == 0) {
            return 0;
        }
        int d = mag[0];
        if (len > 2 || (len == 2 && d < 0)) {
            return INFLATED;
        }

        long u = (len == 2) ?
            (((long) mag[1] & LONG_MASK) + (((long) d) << 32)) :
            (((long) d) & LONG_MASK);
        return (signum < 0) ? -u : u;
    }

    /**
     * Perform the multiplication word by word
     */
    private static void destructiveMulAdd(int[] x, int y, int z) {
        long ylong = y & LONG_MASK;
        long zlong = z & LONG_MASK;
        int len = x.length;

        long product = 0;
        long carry = 0;
        for (int i = len - 1; i >= 0; i--) {
            product = ylong * (x[i] & LONG_MASK) + carry;
            x[i] = (int) product;
            carry = product >>> 32;
        }

        long sum = (x[len - 1] & LONG_MASK) + zlong;
        x[len - 1] = (int) sum;
        carry = sum >>> 32;
        for (int i = len - 2; i >= 0; i--) {
            sum = (x[i] & LONG_MASK) + carry;
            x[i] = (int) sum;
            carry = sum >>> 32;
        }
    }

    private static int[] trustedStripLeadingZeroInts(int val[]) {
        int vlen = val.length;
        int keep;

        // Find first nonzero byte
        for (keep = 0; keep < vlen && val[keep] == 0; keep++) {

        }
        return keep == 0 ? val : Arrays.copyOfRange(val, keep, vlen);
    }
}
