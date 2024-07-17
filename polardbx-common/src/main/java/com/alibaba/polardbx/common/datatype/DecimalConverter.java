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
import com.alibaba.polardbx.common.utils.binlog.LogBuffer;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.google.common.primitives.UnsignedLongs;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.alibaba.polardbx.common.datatype.Decimal.MAX_128_BIT_PRECISION;
import static com.alibaba.polardbx.common.datatype.Decimal.MAX_64_BIT_PRECISION;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.BINARY_SIZE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIG_BASE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIG_MASK;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIG_PER_DEC1;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIG_TO_BYTES;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_BAD_NUM;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DIV_ZERO;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_ERROR;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OK;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OOM;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OVERFLOW;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_TRUNCATED;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.MAX_DECIMAL_PRECISION;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.MAX_VALUE_IN_WORDS;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.POW_10;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.WORDS_LEN;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.roundUp;
import static com.alibaba.polardbx.common.utils.binlog.LogBuffer.DIG_MAX;
import static com.alibaba.polardbx.common.utils.binlog.LogBuffer.SIZE_OF_INT32;
import static com.alibaba.polardbx.common.utils.binlog.LogBuffer.dig2bytes;

public class DecimalConverter {
    public static final DecimalStructure UNSIGNED_ZERO = new DecimalStructure();
    public static final DecimalStructure SIGNED_ZERO = new DecimalStructure();
    private static final long UNSIGNED_MAX_LONG = 0xffffffffffffffffL;
    private static final long MAX_UNSIGNED_LONG_DIV_DIG_BASE = UnsignedLongs.divide(UNSIGNED_MAX_LONG, DIG_BASE);

    static {
        unsignedlongToDecimal(0L, UNSIGNED_ZERO);
        longToDecimal(0L, SIGNED_ZERO);
    }

    public static boolean isDecimal64(int precision) {
        return precision <= MAX_64_BIT_PRECISION && precision > 0;
    }

    public static boolean isDecimal128(int precision) {
        return precision <= MAX_128_BIT_PRECISION && precision > 0;
    }

    public static boolean isDecimal64(Decimal decimal) {
        return isDecimal64(decimal.precision());
    }

    /**
     * Convert decimal to its binary fixed-length representation
     * two representations of the same length can be compared with memcmp
     * with the correct -1/0/+1 result
     * NOTE: the buffer is assumed to be of the size decimal_bin_size(precision, scale)
     *
     * @param from value to convert
     * @param precision wanted precision to store in binary.
     * @param scale wanted scale to store in binary.
     * @param to points to buffer where string representation should be stored
     * @return E_DEC_OK / E_DEC_TRUNCATED / E_DEC_OVERFLOW
     */
    public static int decimalToBin(DecimalStructure from, byte[] to, int precision, int scale) {
        int mask = from.isNeg() ? -1 : 0;
        int toPos = 0;
        int error = E_DEC_OK;

        // wanted digits number of integer/fractional
        int fractions = scale;
        int integers = precision - fractions;

        // number of positions in buff array, for full-DIG_PER_DEC1 integer / fraction digits
        int intg0 = integers / DIG_PER_DEC1;
        int frac0 = fractions / DIG_PER_DEC1;

        // digits number of full-DIG_PER_DEC1 integer / fraction digits (trailing & leading)
        int integersX0 = integers - intg0 * DIG_PER_DEC1;
        int fractionsX0 = fractions - frac0 * DIG_PER_DEC1;

        // original number of positions in buff array, for full-DIG_PER_DEC1 fraction digits
        int frac1 = from.getFractions() / DIG_PER_DEC1;

        // original number of positions in buff array, for not full-DIG_PER_DEC1 fraction digits
        int fracX1 = from.getFractions() - frac1 * DIG_PER_DEC1;

        // occupied bytes of integer / fraction part, from original & results
        int intBytes0 = intg0 * Integer.BYTES + DIG_TO_BYTES[integersX0];
        int fracBytes0 = frac0 * Integer.BYTES + DIG_TO_BYTES[fractionsX0];
        int fracBytes1 = frac1 * Integer.BYTES + DIG_TO_BYTES[fracX1];

        final int originalBytesInt0 = intBytes0;
        final int originalBytesFrac0 = fracBytes0;
        int originalToPos = 0;

        // get the start point to serialize.
        int[] removedResults = from.removeLeadingZeros();
        int bufPos = removedResults[0];
        int integers1 = removedResults[1];

        if (integers1 + fracBytes1 == 0) {
            mask = 0; /* just in case */
            integers = 1;
            bufPos = 0;
        }

        // original number of positions in buff array, for (not)full-DIG_PER_DEC1 integer digits
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

        /* wordIntX1 part */
        if (intgX1 != 0) {
            int i = DIG_TO_BYTES[intgX1];
            int x = (from.getBuffValAt(bufPos) % POW_10[intgX1]) ^ mask;
            bufPos++;
            writeInt(to, toPos, x, i);
            toPos += i;
        }

        int stop1 = 0;
        /* wordIntX1 + wordFracX1 part */
        for (stop1 = bufPos + intg1 + frac1; bufPos < stop1; toPos += Integer.BYTES) {
            int x = from.getBuffValAt(bufPos) ^ mask;
            bufPos++;
            writeInt(to, toPos, x, 4);
        }

        /* wordFrac1 part */
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

        // Check that we have written the whole decimal and nothing more
        assert toPos == originalToPos + originalBytesFrac0 + originalBytesInt0;
        return error;
    }

    /**
     * Restores decimal from its binary fixed-length representation
     *
     * @param from value to convert
     * @param to result
     * @param precision wanted precision to restore from binary.
     * @param scale wanted scale to restore from binary.
     * @return array[0]=read index, array[1]=error:E_DEC_OK / E_DEC_TRUNCATED / E_DEC_OVERFLOW
     */
    public static int[] binToDecimal(byte[] from, DecimalStructure to, int precision, int scale) {

        int error;

        // digits number/occupied positions/byte size of integer part
        int integers = precision - scale;
        int intg0 = integers / DIG_PER_DEC1;
        int frac0 = scale / DIG_PER_DEC1;
        int intgX0 = integers - intg0 * DIG_PER_DEC1;
        int fracX0 = scale - frac0 * DIG_PER_DEC1;
        int intg1 = intg0 + (intgX0 > 0 ? 1 : 0);
        int frac1 = frac0 + (fracX0 > 0 ? 1 : 0);

        int bufPos = 0;
        int mask = (from[0] & 0x80) != 0 ? 0 : -1;

        // size to read
        int binarySize = binarySize(precision, scale);
        int fromPos = 0;

        // fix int and frac error
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

        int toIntegers = intg0 * DIG_PER_DEC1 + intgX0;
        int toFractions = frac0 * DIG_PER_DEC1 + fracX0;
        to.setNeg((mask != 0));
        to.setIntegers(toIntegers);
        to.setFractions(toFractions, true);

        if (intgX0 != 0) {
            int i = DIG_TO_BYTES[intgX0];
            int x = readInt4Bin(from, fromPos, i);

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
                to.setIntegers(toIntegers = (toIntegers - intgX0));
            }

        }
        int stopPos;
        for (stopPos = fromPos + intg0 * Integer.BYTES; fromPos < stopPos; fromPos += Integer.BYTES) {
            to.setBuffValAt(bufPos, readInt4Bin(from, fromPos, 4) ^ mask);
            if (Integer.toUnsignedLong(to.getBuffValAt(bufPos)) > MAX_VALUE_IN_WORDS) {
                // error
                to.toZero();
                return new int[] {binarySize, E_DEC_BAD_NUM};
            }
            if (bufPos > 0 || to.getBuffValAt(bufPos) != 0) {
                bufPos++;
            } else {
                to.setIntegers(toIntegers = (toIntegers - DIG_PER_DEC1));
            }

        }

        for (stopPos = fromPos + frac0 * Integer.BYTES; fromPos < stopPos; fromPos += Integer.BYTES) {
            to.setBuffValAt(bufPos, readInt4Bin(from, fromPos, 4) ^ mask);
            if (Integer.toUnsignedLong(to.getBuffValAt(bufPos)) > MAX_VALUE_IN_WORDS) {
                // error
                to.toZero();
                return new int[] {binarySize, E_DEC_BAD_NUM};
            }

            bufPos++;
        }
        if (fracX0 != 0) {
            int i = DIG_TO_BYTES[fracX0];
            int x = readInt4Bin(from, fromPos, i);

            to.setBuffValAt(bufPos, (x ^ mask) * POW_10[DIG_PER_DEC1 - fracX0]);
            if (Integer.toUnsignedLong(to.getBuffValAt(bufPos)) > MAX_VALUE_IN_WORDS) {
                // error
                to.toZero();
                return new int[] {binarySize, E_DEC_BAD_NUM};
            }
            bufPos++;
        }

        // No digits? We have read the number zero, of unspecified precision.
        // Make it a proper zero, with non-zero precision.
        if (toIntegers == 0 && toFractions == 0) {
            to.toZero();
        }
        to.setDerivedFractions(toFractions);

        return new int[] {binarySize, error};
    }

    /**
     * Returns the size of array to hold a binary representation of a decimal
     *
     * @param precision wanted precision to restore/store from binary.
     * @param scale wanted scale to restore/store from binary.
     * @return size in bytes
     */
    public static int binarySize(int precision, int scale) {
        return BINARY_SIZE[precision][scale];
    }

    /**
     * Returns the size of array to hold a binary representation of a decimal
     *
     * @param precision wanted precision to restore/store from binary.
     * @param scale wanted scale to restore/store from binary.
     * @return size in bytes
     */
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

        // remove leading zeros
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

            // If we need to cut more places than frac is wide, we'll end up
            // dropping the decimal point as well.  Account for this.
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
        // skip space
        while (pos < len && isSpace(decimalAsBytes[pos])) {
            pos++;
        }
        if (pos == len) {
            return error;
        }
        // handle negative
        boolean isNeg = decimalAsBytes[pos] == '-';
        result.setNeg(isNeg);
        if (isNeg) {
            pos++;
        } else if (decimalAsBytes[pos] == '+') {
            pos++;
        }

        // find integer part
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
        // valid decimal string
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

        // for integer
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

        // for fractional
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

        // handle exponent
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

        // Avoid returning negative zero
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

    /**
     * Convert decimal to long value.
     *
     * @param from value to convert.
     * @return long[0]=convert result, long[1]=error code.
     */
    public static long[] decimal2Long(DecimalStructure from, boolean isUnsigned) {
        if (from.isNeg() && isUnsigned) {
            // Converting a signed decimal to unsigned int
            return new long[] {0L, E_DEC_OVERFLOW};
        }

        // round to 0 scale
        DecimalStructure rounded = new DecimalStructure();
        FastDecimalUtils.round(from, rounded, 0, DecimalRoundMod.HALF_UP);
        return isUnsigned ? decimalToULong(rounded) : decimal2Long(rounded);
    }

    /**
     * Convert decimal to long value.
     *
     * @param from value to convert.
     * @return long[0]=convert result, long[1]=error code.
     */
    public static long[] decimal2Long(DecimalStructure from) {
        int bufPos = 0;
        long x = 0L;
        int intg, frac;
        long to;

        for (intg = from.getIntegers(); intg > 0; intg -= DIG_PER_DEC1) {
            long y = x;

            // Attention: trick!
            // we're calculating -|from| instead of |from| here
            // because |LLONG_MIN| > LLONG_MAX
            // so we can convert -9223372036854775808 correctly
            x = x * DIG_BASE - from.getBuffValAt(bufPos++);
            if (y < (Long.MIN_VALUE / DIG_BASE) || x > y) {
                //the decimal is bigger than any possible integer
                // return border integer depending on the sign
                to = from.isNeg() ? Long.MIN_VALUE : Long.MAX_VALUE;
                return new long[] {to, E_DEC_OVERFLOW};
            }
        }
        // boundary case: 9223372036854775808
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

    /**
     * Convert decimal to unsigned long value.
     *
     * @param from value to convert.
     * @return long[0]=convert result, long[1]=error code.
     */
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
            // Count the number of decimal_digit_t's we need.
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

    /**
     * Write integer value to array from designated position with designated length.
     */
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

    /**
     * Read integer value from array, from designated position with designated length.
     */
    private static int readInt4Bin(byte[] b, int startPos, int size) {
        if (startPos == 0) {
            return readInt0(b, size);
        }
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

    private static int readInt0(byte[] b, int size) {
        int x;
        byte b0 = (byte) (b[0] ^ 0x80);
        switch (size) {
        case 1:
            x = b0;
            break;
        case 2:
            x = ((int) b0 << 8)
                + Byte.toUnsignedInt(b[1]);
            break;
        case 3:
            if ((Byte.toUnsignedInt(b0) & 128) != 0) {
                x = ((Byte.toUnsignedInt((byte) 0xff) << 24)
                    | (Byte.toUnsignedInt(b0) << 16)
                    | (Byte.toUnsignedInt(b[1]) << 8)
                    | (Byte.toUnsignedInt(b[2])));
            } else {
                x = ((Byte.toUnsignedInt(b0) << 16)
                    | (Byte.toUnsignedInt(b[1]) << 8)
                    | (Byte.toUnsignedInt(b[2])));
            }
            break;
        case 4:
        default:
            x = Byte.toUnsignedInt(b[3])
                + (Byte.toUnsignedInt(b[2]) << 8)
                + (Byte.toUnsignedInt(b[1]) << 16)
                + ((int) (b0) << 24);
        }
        return x;
    }

    /**
     * Rescale the decimal to specified precision & scale.
     *
     * @param from rescaled decimal value.
     * @param to destination decimal value.
     * @param precision specified precision.
     * @param scale specified scale.
     * @param isUnsigned is data type unsigned.
     */
    public static void rescale(DecimalStructure from, DecimalStructure to, int precision, int scale,
                               boolean isUnsigned) {
        int targetIntegers = precision - scale;
        boolean isNeg = from.isNeg();

        // destination scale does not match the original fractions
        // need round.
        FastDecimalUtils.round(from, to, scale, DecimalRoundMod.HALF_UP);
        boolean toNeg = to.isNeg();

        // prevent from minus unsigned value
        if (isUnsigned && toNeg) {
            to.toZero();
        }

        int toPrecision = to.getPrecision();
        int toFractions = to.getFractions();
        int toIntegers = toPrecision - toFractions;
        if (targetIntegers < toIntegers) {
            // overflow the valid range, use max / min decimal value
            DecimalStructure boundValue = isNeg
                ? DecimalBounds.minValue(precision, scale)
                : DecimalBounds.maxValue(precision, scale);
            boundValue.copyTo(to);
        }
    }

    public static long getUnscaledDecimal(byte[] buffer, int precision, int scale) {
        int position = 0;
        int origin = 0;
        int limit = buffer.length;
        int intg = precision - scale;
        int intg0 = intg / 9;
        int frac0 = scale / 9;
        int intg0x = intg - intg0 * 9;
        int frac0x = scale - frac0 * 9;
        int binSize = intg0 * 4 + dig2bytes[intg0x] + frac0 * 4 + dig2bytes[frac0x];
        if (position + binSize > origin + limit) {
            throw new IllegalArgumentException("limit excceed: " + (position + binSize - origin));
        } else {
            BigDecimal decimal =
                new BigDecimal(getDecimalString(buffer, position, intg, scale, intg0, frac0, intg0x, frac0x))
                    .setScale(scale, RoundingMode.HALF_UP);
            return decimal.unscaledValue().longValue();
        }
    }

    public static Decimal getDecimal(byte[] buffer, int precision, int scale) {
        int position = 0;
        int origin = 0;
        int limit = buffer.length;
        int intg = precision - scale;
        int intg0 = intg / 9;
        int frac0 = scale / 9;
        int intg0x = intg - intg0 * 9;
        int frac0x = scale - frac0 * 9;
        int binSize = intg0 * 4 + dig2bytes[intg0x] + frac0 * 4 + dig2bytes[frac0x];
        if (position + binSize > origin + limit) {
            throw new IllegalArgumentException("limit excceed: " + (position + binSize - origin));
        } else {
            String str = getDecimalString(buffer, position, intg, scale, intg0, frac0, intg0x, frac0x);
            return Decimal.fromString(str);
        }
    }

    /**
     * TODO Optimize getting unscaled long
     *
     * @see LogBuffer#getDecimal0(int, int, int, int, int, int, int)
     */
    static String getDecimalString(byte[] buffer, int begin, int intg, int frac, int intg0, int frac0,
                                   int intg0x, int frac0x) {
        final int mask = ((buffer[begin] & 0x80) == 0x80) ? 0 : -1;
        int from = begin;

        /* max string length */
        final int len = ((mask != 0) ? 1 : 0) + ((intg != 0) ? intg : 1) // NL
            + ((frac != 0) ? 1 : 0) + frac;
        char[] buf = new char[len];
        int pos = 0;

        if (mask != 0) /* decimal sign */ {
            buf[pos++] = ('-');
        }

        final byte[] d_copy = buffer;
        d_copy[begin] ^= 0x80; /* clear sign */
        int mark = pos;

        if (intg0x != 0) {
            final int i = dig2bytes[intg0x];
            int x = 0;
            switch (i) {
            case 1:
                x = d_copy[from] /* one byte */;
                break;
            case 2:
                x = getInt16BE(d_copy, from);
                break;
            case 3:
                x = getInt24BE(d_copy, from);
                break;
            case 4:
                x = getInt32BE(d_copy, from);
                break;
            }
            from += i;
            x ^= mask;
            if (x < 0 || x >= POW_10[intg0x + 1]) {
                throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + POW_10[intg0x + 1]);
            }
            if (x != 0 /* !digit || x != 0 */) {
                for (int j = intg0x; j > 0; j--) {
                    final int divisor = POW_10[j - 1];
                    final int y = x / divisor;
                    if (mark < pos || y != 0) {
                        buf[pos++] = ((char) ('0' + y));
                    }
                    x -= y * divisor;
                }
            }
        }

        for (final int stop = from + intg0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
            int x = getInt32BE(d_copy, from);
            x ^= mask;
            if (x < 0 || x > DIG_MAX) {
                throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
            }
            if (x != 0) {
                if (mark < pos) {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = POW_10[i - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                } else {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = POW_10[i - 1];
                        final int y = x / divisor;
                        if (mark < pos || y != 0) {
                            buf[pos++] = ((char) ('0' + y));
                        }
                        x -= y * divisor;
                    }
                }
            } else if (mark < pos) {
                for (int i = DIG_PER_DEC1; i > 0; i--) {
                    buf[pos++] = ('0');
                }
            }
        }

        if (mark == pos)
            /* fix 0.0 problem, only '.' may cause BigDecimal parsing exception. */ {
            buf[pos++] = ('0');
        }

        if (frac > 0) {
            buf[pos++] = ('.');
            mark = pos;

            for (final int stop = from + frac0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
                int x = getInt32BE(d_copy, from);
                x ^= mask;
                if (x < 0 || x > DIG_MAX) {
                    throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
                }
                if (x != 0) {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = POW_10[i - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                } else {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        buf[pos++] = ('0');
                    }
                }
            }

            if (frac0x != 0) {
                final int i = dig2bytes[frac0x];
                int x = 0;
                switch (i) {
                case 1:
                    x = d_copy[from] /* one byte */;
                    break;
                case 2:
                    x = getInt16BE(d_copy, from);
                    break;
                case 3:
                    x = getInt24BE(d_copy, from);
                    break;
                case 4:
                    x = getInt32BE(d_copy, from);
                    break;
                }
                x ^= mask;
                if (x != 0) {
                    final int dig = DIG_PER_DEC1 - frac0x;
                    x *= POW_10[dig];
                    if (x < 0 || x > DIG_MAX) {
                        throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
                    }
                    for (int j = DIG_PER_DEC1; j > dig; j--) {
                        final int divisor = POW_10[j - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                }
            }

            if (mark == pos)
                /* make number more friendly */ {
                buf[pos++] = ('0');
            }
        }

        d_copy[begin] ^= 0x80; /* restore sign */
        return String.valueOf(buf, 0, pos);
    }

    private static int getInt16BE(byte[] buffer, int pos) {
        return buffer[pos] << 8 | 255 & buffer[pos + 1];
    }

    private static int getInt24BE(byte[] buffer, int pos) {
        return buffer[pos] << 16 | (255 & buffer[pos + 1]) << 8 | 255 & buffer[pos + 2];
    }

    private static int getInt32BE(byte[] buffer, int pos) {
        return buffer[pos] << 24 | (255 & buffer[pos + 1]) << 16 | (255 & buffer[pos + 2]) << 8 | 255 & buffer[pos + 3];
    }

}
