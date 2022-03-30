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

package com.alibaba.polardbx.common.datatype;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.google.common.primitives.UnsignedLongs;
import io.airlift.slice.Slice;

import java.math.BigDecimal;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.*;

public class DecimalConverter {
    public static final DecimalStructure UNSIGNED_ZERO = new DecimalStructure();
    public static final DecimalStructure SIGNED_ZERO = new DecimalStructure();

    static {
        unsignedlongToDecimal(0L, UNSIGNED_ZERO);
        longToDecimal(0L, SIGNED_ZERO);
    }

    public static int decimalToBin(DecimalStructure from, byte[] to, int precision, int scale) {
        int mask = from.isNeg() ? -1 : 0;
        int toPos = 0;
        int error = E_DEC_OK;


        int fractions = scale;
        int integers = precision - fractions;


        int intg0 = integers / DIG_PER_DEC1;
        int frac0 = fractions / DIG_PER_DEC1;


        int integersX0 = integers - intg0 * DIG_PER_DEC1;
        int fractionsX0 = fractions - frac0 * DIG_PER_DEC1;


        int frac1 = from.getFractions() / DIG_PER_DEC1;


        int fracX1 = from.getFractions() - frac1 * DIG_PER_DEC1;


        int intBytes0 = intg0 * Integer.BYTES + DIG_TO_BYTES[integersX0];
        int fracBytes0 = frac0 * Integer.BYTES + DIG_TO_BYTES[fractionsX0];
        int fracBytes1 = frac1 * Integer.BYTES + DIG_TO_BYTES[fracX1];

        final int originalBytesInt0 = intBytes0;
        final int originalBytesFrac0 = fracBytes0;
        int originalToPos = 0;


        int[] removedResults = from.removeLeadingZeros();
        int bufPos = removedResults[0];
        int integers1 = removedResults[1];

        if (integers1 + fracBytes1 == 0) {
            mask = 0;
            integers = 1;
            bufPos = 0;
        }


        int intg1 = integers1 / DIG_PER_DEC1;
        int intgX1 = integers1 - intg1 * DIG_PER_DEC1;

        int bytesInt1 = intg1 * Integer.BYTES + DIG_TO_BYTES[intgX1];

        if (integers < integers1) {
            int x = intgX1 > 0 ? 1 : 0;
            int y = integersX0 > 0 ? 1 : 0;
            bufPos += (intg1 - intg0 + x - y);
            intg1 = intg0;
            intgX1 = integersX0;
            error = E_DEC_OVERFLOW;
        } else if (intBytes0 > bytesInt1) {
            while (intBytes0-- > bytesInt1) {
                to[toPos] = (byte) mask;
                toPos++;
            }
        }

        if (fracBytes0 < fracBytes1 ||
            (fracBytes0 == fracBytes1 && (fractionsX0 <= fracX1 || frac0 <= frac1))) {
            if (fracBytes0 < fracBytes1 || (fracBytes0 == fracBytes1 && fractionsX0 < fracX1) || (
                fracBytes0 == fracBytes1 && frac0 < frac1)) {
                error = E_DEC_TRUNCATED;
            }
            frac1 = frac0;
            fracX1 = fractionsX0;

        } else if (fracBytes0 > fracBytes1 && fracX1 != 0) {
            if (frac0 == frac1) {
                fracX1 = fractionsX0;
                fracBytes0 = fracBytes1;
            } else {
                frac1++;
                fracX1 = 0;
            }
        }

        if (intgX1 != 0) {
            int i = DIG_TO_BYTES[intgX1];
            int x = (from.getBuffValAt(bufPos) % POW_10[intgX1]) ^ mask;
            bufPos++;
            writeInt(to, toPos, x, i);
            toPos += i;
        }

        int stop1 = 0;

        for (stop1 = bufPos + intg1 + frac1; bufPos < stop1; toPos += Integer.BYTES) {
            int x = from.getBuffValAt(bufPos) ^ mask;
            bufPos++;
            writeInt(to, toPos, x, 4);
        }

        if (fracX1 != 0) {
            int x;
            int i = DIG_TO_BYTES[fracX1],
                lim = (frac1 < frac0 ? DIG_PER_DEC1 : fractionsX0);
            while (fracX1 < lim && DIG_TO_BYTES[fracX1] == i) {
                fracX1++;
            }

            x = (from.getBuffValAt(bufPos) / POW_10[DIG_PER_DEC1 - fracX1]) ^ mask;
            writeInt(to, toPos, x, i);
            toPos += i;
        }
        if (fracBytes0 > fracBytes1) {
            int toEnd = originalToPos + originalBytesFrac0 + originalBytesInt0;

            while (fracBytes0-- > fracBytes1 && toPos < toEnd) {
                to[toPos] = (byte) mask;
                toPos++;
            }

        }
        to[originalToPos] ^= 0x80;


        assert toPos == originalToPos + originalBytesFrac0 + originalBytesInt0;
        return error;
    }

    public static int[] binToDecimal(byte[] from, DecimalStructure to, int precision, int scale) {

        int error;


        int integers = precision - scale;
        int intg0 = integers / DIG_PER_DEC1;
        int frac0 = scale / DIG_PER_DEC1;
        int intgX0 = integers - intg0 * DIG_PER_DEC1;
        int fracX0 = scale - frac0 * DIG_PER_DEC1;
        int intg1 = intg0 + (intgX0 > 0 ? 1 : 0);
        int frac1 = frac0 + (fracX0 > 0 ? 1 : 0);

        int bufPos = 0;
        int mask = (from[0] & 0x80) != 0 ? 0 : -1;


        int binarySize = binarySize(precision, scale);


        byte[] tmpCopy = new byte[binarySize];
        int fromPos = 0;
        System.arraycopy(from, 0, tmpCopy, 0, binarySize);
        tmpCopy[0] ^= 0x80;
        from = tmpCopy;


        if (intg1 + frac1 > DecimalTypeBase.WORDS_LEN) {
            if (intg1 > DecimalTypeBase.WORDS_LEN) {
                intg1 = DecimalTypeBase.WORDS_LEN;
                frac1 = 0;
                error = DecimalTypeBase.E_DEC_OVERFLOW;
            } else {
                frac1 = DecimalTypeBase.WORDS_LEN - intg1;
                error = DecimalTypeBase.E_DEC_TRUNCATED;
            }
        } else {
            error = E_DEC_OK;
        }

        if (error != 0) {
            if (intg1 < intg0 + (intgX0 > 0 ? 1 : 0)) {
                fromPos += DIG_TO_BYTES[intgX0] + Integer.BYTES * (intg0 - intg1);
                frac0 = fracX0 = intgX0 = 0;
                intg0 = intg1;
            } else {
                fracX0 = 0;
                frac0 = frac1;
            }
        }

        to.setNeg((mask != 0));
        to.setIntegers(intg0 * DIG_PER_DEC1 + intgX0);
        to.setFractions(frac0 * DIG_PER_DEC1 + fracX0, true);

        if (intgX0 != 0) {
            int i = DIG_TO_BYTES[intgX0];
            int x = readInt(from, fromPos, i);

            fromPos += i;
            to.setBuffValAt(bufPos, x ^ mask);
            if (UnsignedLongs.compare(to.getBuffValAt(bufPos) & 0xFFFFFFFFL, POW_10[intgX0 + 1] & 0xFFFFFFFFL)
                >= 0) {
                to.toZero();
                return new int[] {binarySize, E_DEC_BAD_NUM};
            }

            if (bufPos > 0 || to.getBuffValAt(bufPos) != 0) {
                bufPos++;
            } else {
                to.setIntegers(to.getIntegers() - intgX0);
            }

        }
        int stopPos;
        for (stopPos = fromPos + intg0 * Integer.BYTES; fromPos < stopPos; fromPos += Integer.BYTES) {
            to.setBuffValAt(bufPos, readInt(from, fromPos, 4) ^ mask);
            if (Integer.toUnsignedLong(to.getBuffValAt(bufPos)) > MAX_VALUE_IN_WORDS) {

                to.toZero();
                return new int[] {binarySize, E_DEC_BAD_NUM};
            }
            if (bufPos > 0 || to.getBuffValAt(bufPos) != 0) {
                bufPos++;
            } else {
                to.setIntegers(to.getIntegers() - DIG_PER_DEC1);
            }

        }

        for (stopPos = fromPos + frac0 * Integer.BYTES; fromPos < stopPos; fromPos += Integer.BYTES) {
            to.setBuffValAt(bufPos, readInt(from, fromPos, 4) ^ mask);
            if (Integer.toUnsignedLong(to.getBuffValAt(bufPos)) > MAX_VALUE_IN_WORDS) {

                to.toZero();
                return new int[] {binarySize, E_DEC_BAD_NUM};
            }

            bufPos++;
        }
        if (fracX0 != 0) {
            int i = DIG_TO_BYTES[fracX0];
            int x = readInt(from, fromPos, i);

            to.setBuffValAt(bufPos, (x ^ mask) * POW_10[DIG_PER_DEC1 - fracX0]);
            if (Integer.toUnsignedLong(to.getBuffValAt(bufPos)) > MAX_VALUE_IN_WORDS) {

                to.toZero();
                return new int[] {binarySize, E_DEC_BAD_NUM};
            }
            bufPos++;
        }

        if (to.getIntegers() == 0 && to.getFractions() == 0) {
            to.toZero();
        }
        to.setDerivedFractions(to.getFractions());

        return new int[] {binarySize, error};
    }

    public static int binarySize(int precision, int scale) {
        return BINARY_SIZE[precision][scale];
    }

    public static int binarySizeByCompute(int precision, int scale) {
        int integers = precision - scale;
        int intWords = integers / DIG_PER_DEC1;
        int fracWords = scale / DIG_PER_DEC1;
        int intX0 = integers - intWords * DIG_PER_DEC1;
        int fracX0 = scale - fracWords * DIG_PER_DEC1;

        return intWords * Integer.BYTES + DIG_TO_BYTES[intX0] +
            fracWords * Integer.BYTES + DIG_TO_BYTES[fracX0];
    }

    public static Pair<byte[], Integer> decimal2String(DecimalStructure from, int fixedPrecision, int fixedScale,
                                                       byte filler) {
        int fixedIntegerLen = fixedPrecision != 0 ? (fixedPrecision - fixedScale) : 0;
        int error = E_DEC_OK;
        int resultLen = stringSize(from);

        int[] removedResults = from.removeLeadingZeros();
        int bufPos0 = removedResults[0];
        int intg = removedResults[1];
        int frac = from.getFractions();

        if (intg + frac == 0) {
            intg = 1;
            bufPos0 = 0;
        }

        int fracLen, intLen;
        if ((intLen = fixedPrecision != 0 ? fixedScale : intg) == 0) {
            intLen = 1;
        }
        fracLen = fixedPrecision != 0 ? fixedScale : frac;
        int len = (from.isNeg() ? 1 : 0) + intLen + (frac != 0 ? 1 : 0) + fracLen;
        if (fixedPrecision != 0) {
            if (frac > fixedScale) {
                error = E_DEC_TRUNCATED;
                frac = fixedScale;
            }
            if (intg > fixedIntegerLen) {
                error = E_DEC_OVERFLOW;
                intg = fixedIntegerLen;
            }
        } else if (len > resultLen) {
            int j = len - resultLen;
            error = (frac != 0 && j < frac + 1) ? E_DEC_TRUNCATED : E_DEC_OVERFLOW;

            if (frac != 0 && j >= frac + 1) {
                j--;
            }
            if (j > frac) {
                intLen = (intg -= (j - frac));
                frac = 0;
            } else {
                frac -= j;
            }
            fracLen = frac;
            len = (from.isNeg() ? 1 : 0) + intLen + (frac != 0 ? 1 : 0) + fracLen;
        }

        resultLen = len;
        byte[] results = new byte[resultLen];

        int bytePos = 0;
        int fill;
        if (from.isNeg()) {
            results[bytePos++] = '-';
        }
        if (frac != 0) {
            int fracPos = bytePos + intLen;
            fill = fracLen - frac;
            int buffPos = bufPos0 + roundUp(intg);
            results[fracPos++] = '.';
            for (; frac > 0; frac -= DIG_PER_DEC1) {
                int x = from.getBuffValAt(buffPos++);
                for (int i = Math.min(frac, DIG_PER_DEC1); i != 0; i--) {
                    int y = x / DIG_MASK;
                    results[fracPos++] = (byte) ('0' + y);
                    x -= y * DIG_MASK;
                    x *= 10;
                }
            }
            for (; fill > 0; fill--) {
                results[fracPos++] = filler;
            }
        }
        fill = intLen - intg;
        if (intg == 0) {
            fill--;
        }
        for (; fill > 0; fill--) {
            results[bytePos++] = filler;
        }
        if (intg != 0) {
            bytePos += intg;
            for (int buffPos = bufPos0 + roundUp(intg); intg > 0; intg -= DIG_PER_DEC1) {
                int x = from.getBuffValAt(--buffPos);
                for (int i = Math.min(intg, DIG_PER_DEC1); i != 0; i--) {
                    int y = x / 10;
                    results[--bytePos] = (byte) ('0' + (x - y * 10));
                    x = y;
                }
            }
        } else {
            results[bytePos] = '0';
        }

        return Pair.of(results, error);
    }

    public static int parseString(Slice slice, DecimalStructure result, boolean fixed) {
        if (slice == null) {
            return E_DEC_ERROR;
        }
        return parseString(slice.getBytes(), result, fixed);
    }

    public static int parseString(byte[] decimalAsBytes, final int offset, final int length, DecimalStructure result,
                                  boolean fixed) {
        final int error = doParseString(decimalAsBytes, offset, length, result, fixed);
        // check overflow
        if (error == E_DEC_OVERFLOW) {
            boolean isNeg = result.isNeg();
            DecimalBounds.maxValue(MAX_DECIMAL_PRECISION, 0).copyTo(result);
            result.setNeg(isNeg);
        }
        if (error != E_DEC_DIV_ZERO && result.isNeg() && result.isZero()) {
            result.setNeg(false);
        }
        return error;
    }

    private static int doParseString(byte[] decimalAsBytes, final int offset, final int length, DecimalStructure result,
                                     boolean fixed) {
        int pos = offset;
        int len = length;
        int error = E_DEC_BAD_NUM;

        while (pos < len && isSpace(decimalAsBytes[pos])) {
            pos++;
        }
        if (pos == len) {
            return error;
        }

        boolean isNeg = decimalAsBytes[pos] == '-';
        result.setNeg(isNeg);
        if (isNeg) {
            pos++;
        } else if (decimalAsBytes[pos] == '+') {
            pos++;
        }

        int pos1 = pos;
        int endPos;
        int integerLen, fractionalLen;
        int integerLen1 = 0, fractionalLen1;
        while (pos < len && isDigits(decimalAsBytes[pos])) {
            pos++;
        }
        integerLen = pos - pos1;
        if (pos < len && decimalAsBytes[pos] == '.') {
            endPos = pos + 1;
            while (endPos < len && isDigits(decimalAsBytes[endPos])) {
                endPos++;
            }
            fractionalLen = endPos - pos - 1;
        } else {
            fractionalLen = 0;
            endPos = pos;
        }

        len = endPos;
        if (fractionalLen + integerLen == 0) {
            return error;
        }

        error = 0;
        if (fixed) {
            if (fractionalLen > result.getFractions()) {
                error = E_DEC_TRUNCATED;
                fractionalLen = result.getFractions();
            }
            if (integerLen > result.getIntegers()) {
                error = E_DEC_OVERFLOW;
                integerLen = result.getIntegers();
            }
            integerLen1 = roundUp(integerLen);
            fractionalLen1 = roundUp(fractionalLen);
            if (integerLen1 + fractionalLen1 > WORDS_LEN) {
                error = E_DEC_OOM;
                result.toZero();
                return error;
            }
        } else {
            integerLen1 = roundUp(integerLen);
            fractionalLen1 = roundUp(fractionalLen);
            if (integerLen1 + fractionalLen1 > WORDS_LEN) {
                if (integerLen1 > WORDS_LEN) {
                    integerLen1 = WORDS_LEN;
                    fractionalLen1 = 0;
                    error = E_DEC_OVERFLOW;
                } else {
                    fractionalLen1 = WORDS_LEN - integerLen1;
                    error = E_DEC_TRUNCATED;
                }
            } else {
                error = E_DEC_OK;
            }

            if (error != 0) {
                fractionalLen = fractionalLen1 * DIG_PER_DEC1;
                if (error == E_DEC_OVERFLOW) {
                    integerLen = integerLen1 * DIG_PER_DEC1;
                }
            }
        }

        result.setIntegers(integerLen);
        result.setFractions(fractionalLen, true);

        int bufPos = integerLen1;
        pos1 = pos;
        int x = 0, i = 0;

        for (; integerLen != 0; integerLen--) {
            x += (decimalAsBytes[--pos] - '0') * POW_10[i];
            if (++i == DIG_PER_DEC1) {
                result.setBuffValAt(--bufPos, x);
                x = 0;
                i = 0;
            }
        }
        if (i != 0) {
            result.setBuffValAt(--bufPos, x);
        }

        bufPos = integerLen1;
        for (x = 0, i = 0; fractionalLen != 0; fractionalLen--) {
            x = (Byte.toUnsignedInt(decimalAsBytes[++pos1]) - '0') + x * 10;
            if (++i == DIG_PER_DEC1) {
                result.setBuffValAt(bufPos++, x);
                x = 0;
                i = 0;
            }
        }
        if (i != 0) {
            result.setBuffValAt(bufPos, x * POW_10[DIG_PER_DEC1 - i]);
        }

        if (endPos + 1 < offset + length && (decimalAsBytes[endPos] == 'e' || decimalAsBytes[endPos] == 'E')) {
            long[] parseResults = StringNumericParser.parseString(decimalAsBytes, endPos + 1, decimalAsBytes.length);
            int strError = (int) parseResults[StringNumericParser.ERROR_INDEX];
            long exponent = parseResults[StringNumericParser.NUMERIC_INDEX];
            long currentPos = parseResults[StringNumericParser.POSITION_INDEX];

            // If at least one digit
            if (currentPos != endPos + 1) {
                if (strError > 0) {
                    error = E_DEC_BAD_NUM;
                    result.toZero();
                    return error;
                }
                if (exponent > Integer.MAX_VALUE / 2) {
                    error = E_DEC_OVERFLOW;
                    result.toZero();
                    return error;
                }
                if (exponent < Integer.MIN_VALUE / 2 && error != E_DEC_OVERFLOW) {
                    error = E_DEC_TRUNCATED;
                    result.toZero();
                    return error;
                }
                if (error != E_DEC_OVERFLOW) {
                    error = FastDecimalUtils.doShift(result, (int) exponent);
                }
            }
        }

        if (result.isNeg() && result.isZero()) {
            result.setNeg(false);
        }
        result.setDerivedFractions(result.getFractions());
        return error;
    }

    public static int parseString(byte[] decimalAsBytes, DecimalStructure result, boolean fixed) {
        if (decimalAsBytes == null) {
            return E_DEC_ERROR;
        }
        return parseString(decimalAsBytes, 0, decimalAsBytes.length, result, fixed);
    }

    public static long[] decimal2Long(DecimalStructure from, boolean isUnsigned) {
        if (from.isNeg() && isUnsigned) {

            return new long[] {0L, E_DEC_OVERFLOW};
        }

        DecimalStructure rounded = new DecimalStructure();
        FastDecimalUtils.round(from, rounded, 0, DecimalRoundMod.HALF_UP);
        return isUnsigned ? decimalToULong(rounded) : decimal2Long(rounded);
    }

    public static long[] decimal2Long(DecimalStructure from) {
        int bufPos = 0;
        long x = 0L;
        int intg, frac;
        long to;

        for (intg = from.getIntegers(); intg > 0; intg -= DIG_PER_DEC1) {
            long y = x;

            x = x * DIG_BASE - from.getBuffValAt(bufPos++);
            if (y < (Long.MIN_VALUE / DIG_BASE) || x > y) {

                to = from.isNeg() ? Long.MIN_VALUE : Long.MAX_VALUE;
                return new long[] {to, E_DEC_OVERFLOW};
            }
        }

        if (!from.isNeg() && x == Long.MIN_VALUE) {
            to = Long.MAX_VALUE;
            return new long[] {to, E_DEC_OVERFLOW};
        }

        to = from.isNeg() ? x : -x;
        for (frac = from.getFractions(); frac > 0; frac -= DIG_PER_DEC1) {
            if (from.getBuffValAt(bufPos++) != 0) {
                return new long[] {to, E_DEC_TRUNCATED};
            }
        }

        return new long[] {to, E_DEC_OK};
    }

    private static final long UNSIGNED_MAX_LONG = 0xffffffffffffffffL;
    private static final long MAX_UNSIGNED_LONG_DIV_DIG_BASE = UnsignedLongs.divide(UNSIGNED_MAX_LONG, DIG_BASE);

    public static long[] decimalToULong(DecimalStructure from) {
        int bufPos = 0;
        long x = 0;
        int intg, frac;
        long to;

        if (from.isNeg()) {
            return new long[] {0L, E_DEC_OVERFLOW};
        }

        for (intg = from.getIntegers(); intg > 0; intg -= DIG_PER_DEC1) {
            long y = x;
            x = x * DIG_BASE + from.getBuffValAt(bufPos++);
            if (UnsignedLongs.compare(y, MAX_UNSIGNED_LONG_DIV_DIG_BASE) > 0 || UnsignedLongs.compare(x, y) < 0) {
                return new long[] {UNSIGNED_MAX_LONG, E_DEC_OVERFLOW};
            }
        }
        to = x;
        for (frac = from.getFractions(); frac > 0; frac -= DIG_PER_DEC1) {
            if (from.getBuffValAt(bufPos++) != 0) {
                return new long[] {to, E_DEC_TRUNCATED};
            }
        }
        return new long[] {to, E_DEC_OK};
    }

    public static double decimalToDouble(DecimalStructure from) {
        Pair<byte[], Integer> res = DecimalConverter.decimal2String(from, 0, 0, (byte) 0);
        double d = Double.valueOf(new String(res.getKey()));
        return d;
    }

    private static int unsignedLongToDecimal(long from, DecimalStructure to) {
        int intg1;
        int error = E_DEC_OK;
        long x = from;

        if (from == 0) {
            intg1 = 1;
        } else {

            intg1 = 0;
            while (from != 0) {
                intg1++;
                from = UnsignedLongs.divide(from, DIG_BASE);
            }
        }
        if (intg1 > WORDS_LEN) {
            intg1 = WORDS_LEN;
            error = E_DEC_OVERFLOW;
        }
        to.setFractions(0, true);
        to.setIntegers(intg1 * DIG_PER_DEC1);

        for (int bufPos = intg1; intg1 != 0; intg1--) {
            long y = UnsignedLongs.divide(x, DIG_BASE);
            to.setBuffValAt(--bufPos, (int) (x - y * DIG_BASE));
            x = y;
        }
        return error;
    }

    public static int longToDecimal(long from, DecimalStructure to, boolean isUnsigned) {
        return isUnsigned ? unsignedlongToDecimal(from, to) : longToDecimal(from, to);
    }

    public static int longToDecimal(long from, DecimalStructure to) {
        to.setNeg(from < 0L);
        if (from < 0L) {
            return unsignedLongToDecimal(-from, to);
        }
        return unsignedLongToDecimal(from, to);
    }

    public static int unsignedlongToDecimal(long from, DecimalStructure to) {
        to.setNeg(false);
        return unsignedLongToDecimal(from, to);
    }

    private static boolean isSpace(byte b) {
        return b == ' ';
    }

    private static boolean isDigits(byte b) {
        int intVal;
        return (intVal = Byte.toUnsignedInt(b)) >= '0' && intVal <= '9';
    }

    private static int stringSize(DecimalStructure from) {
        return (from.getIntegers() != 0 ? from.getIntegers() : 1) + (from.getFractions() + 3);
    }

    private static void writeInt(byte[] b, int startPos, int x, int size) {
        long v = Integer.toUnsignedLong(x);
        switch (size) {
        case 1:
            b[startPos + 0] = (byte) (x);
            break;
        case 2:
            b[startPos + 0] = (byte) (v >> 8);
            b[startPos + 1] = (byte) (v);
            break;
        case 3:
            b[startPos + 0] = (byte) (v >> 16);
            b[startPos + 1] = (byte) (v >> 8);
            b[startPos + 2] = (byte) (v);
            break;
        case 4:
        default:
            b[startPos + 0] = (byte) (v >> 24);
            b[startPos + 1] = (byte) (v >> 16);
            b[startPos + 2] = (byte) (v >> 8);
            b[startPos + 3] = (byte) (v);
            break;
        }
    }

    private static int readInt(byte[] b, int startPos, int size) {
        int x;
        switch (size) {
        case 1:
            x = b[startPos + 0];
            break;
        case 2:
            x = ((int) b[startPos + 0] << 8)
                + Byte.toUnsignedInt(b[startPos + 1]);
            break;
        case 3:
            if ((Byte.toUnsignedInt(b[startPos + 0]) & 128) != 0) {
                x = ((Byte.toUnsignedInt((byte) 0xff) << 24)
                    | (Byte.toUnsignedInt(b[startPos + 0]) << 16)
                    | (Byte.toUnsignedInt(b[startPos + 1]) << 8)
                    | (Byte.toUnsignedInt(b[startPos + 2])));
            } else {
                x = ((Byte.toUnsignedInt(b[startPos + 0]) << 16)
                    | (Byte.toUnsignedInt(b[startPos + 1]) << 8)
                    | (Byte.toUnsignedInt(b[startPos + 2])));
            }
            break;
        case 4:
        default:
            x = Byte.toUnsignedInt(b[startPos + 3])
                + (Byte.toUnsignedInt(b[startPos + 2]) << 8)
                + (Byte.toUnsignedInt(b[startPos + 1]) << 16)
                + ((int) (b[startPos + 0]) << 24);
        }
        return x;
    }

    public static void rescale(DecimalStructure from, DecimalStructure to, int precision, int scale,
                               boolean isUnsigned) {
        int fromPrecision = from.getPrecision();
        int fractions = from.getFractions();
        int integers = fromPrecision - fractions;
        int toIntegers = precision - scale;
        boolean isNeg = from.isNeg();

        from.copyTo(to);

        if (!from.isZero() && integers > toIntegers) {

            if (isUnsigned && isNeg) {
                to.toZero();
            } else {
                DecimalStructure boundValue = isNeg
                    ? DecimalBounds.minValue(precision, scale)
                    : DecimalBounds.maxValue(precision, scale);
                boundValue.copyTo(to);
            }
        } else if (fractions != scale) {

            FastDecimalUtils.round(from, to, scale, DecimalRoundMod.HALF_UP);
        }

        if (isUnsigned && to != null && to.isNeg()) {
            to.toZero();
        }
    }
}
