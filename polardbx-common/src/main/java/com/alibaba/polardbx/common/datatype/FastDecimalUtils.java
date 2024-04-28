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

import com.alibaba.polardbx.common.utils.BigDecimalUtil;
import com.google.common.annotations.VisibleForTesting;

import java.math.BigInteger;

import static com.alibaba.polardbx.common.datatype.Decimal.MAX_128_BIT_PRECISION;
import static com.alibaba.polardbx.common.datatype.DecimalRoundMod.HALF_UP;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIG_BASE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIG_MASK;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIG_PER_DEC1;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OK;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OVERFLOW;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_TRUNCATED;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.POW_10;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.WORDS_LEN;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.roundUp;

public class FastDecimalUtils {
    public static int compare(DecimalStructure from, DecimalStructure to) {
        if (from.isNeg() == to.isNeg()) {
            return doCompare(from, to);
        }
        if (from.isNeg()) {
            return -1;
        }
        return 1;
    }

    public static int add(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        return add(from1, from2, to, false);
    }

    public static int add(DecimalStructure from1, DecimalStructure from2, DecimalStructure to, boolean needReset) {
        if (from1 == to) {
            // copy as new decimal object
            from1 = to.copy();
        }
        if (from2 == to) {
            // copy as new decimal object
            from2 = to.copy();
        }
        // clear status of to
        if (needReset) {
            to.reset();
        }

        to.setDerivedFractions(Math.max(from1.getDerivedFractions(), from2.getDerivedFractions()));
        if (from1.isNeg() == from2.isNeg()) {
            return doAdd(from1, from2, to);
        } else {
            return doSub(from1, from2, to)[1];
        }
    }

    public static int sub(DecimalStructure from1, DecimalStructure from2, DecimalStructure to, boolean needReset) {
        if (from1 == to) {
            // copy as new decimal object
            from1 = to.copy();
        }
        if (from2 == to) {
            // copy as new decimal object
            from2 = to.copy();
        }
        // clear status of to
        if (needReset) {
            to.reset();
        }

        to.setDerivedFractions(Math.max(from1.getDerivedFractions(), from2.getDerivedFractions()));
        if (from1.isNeg() == from2.isNeg()) {
            return doSub(from1, from2, to)[1];
        } else {
            return doAdd(from1, from2, to);
        }
    }

    public static int sub(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        return sub(from1, from2, to, false);
    }

    public static int mul(DecimalStructure from1, DecimalStructure from2, DecimalStructure to, boolean needReset) {
        if (from1 == to) {
            // copy as new decimal object
            from1 = to.copy();
        }
        if (from2 == to) {
            // copy as new decimal object
            from2 = to.copy();
        }
        // clear status of to
        if (needReset) {
            to.reset();
        }

        to.setDerivedFractions(
            Math.min(from1.getDerivedFractions() + from2.getDerivedFractions(), DecimalTypeBase.MAX_DECIMAL_SCALE));
        return doMul(from1, from2, to);
    }

    public static int mul(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        return mul(from1, from2, to, false);
    }

    /**
     * Fast Decimal division
     *
     * @param from1 dividend
     * @param from2 divisor
     * @param to quotient
     * @param scaleIncr Set by div_precision_increment. the number of digits by which to increase the scale of the result of division operations performed with the / operator. The default value is 4.
     * @return E_DEC_OK / E_DEC_TRUNCATED / E_DEC_OVERFLOW / E_DEC_DIV_ZERO
     */
    public static int div(DecimalStructure from1, DecimalStructure from2, DecimalStructure to, int scaleIncr) {
        return div(from1, from2, to, scaleIncr, false);
    }

    public static int div(DecimalStructure from1, DecimalStructure from2, DecimalStructure to, int scaleIncr,
                          boolean needReset) {
        if (from1 == to) {
            // copy as new decimal object
            from1 = to.copy();
        }
        if (from2 == to) {
            // copy as new decimal object
            from2 = to.copy();
        }
        // clear status of to
        if (needReset) {
            to.reset();
        }

        to.setDerivedFractions(Math.min(from1.getDerivedFractions() + scaleIncr, DecimalTypeBase.MAX_DECIMAL_SCALE));
        return doDiv(from1, from2, to, null, scaleIncr);
    }

    public static int mod(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        return mod(from1, from2, to, false);
    }

    public static int mod(DecimalStructure from1, DecimalStructure from2, DecimalStructure to, boolean needReset) {
        if (from1 == to) {
            // copy as new decimal object
            from1 = to.copy();
        }
        if (from2 == to) {
            // copy as new decimal object
            from2 = to.copy();
        }
        // clear status of to
        if (needReset) {
            to.reset();
        }

        to.setDerivedFractions(Math.max(from1.getDerivedFractions(), from2.getDerivedFractions()));
        return doDiv(from1, from2, null, to, 0);
    }

    public static int shift(DecimalStructure from, DecimalStructure to, int scale) {
        if (from != to) {
            from.copyTo(to);
        }
        return doShift(to, scale);
    }

    public static int round(DecimalStructure from, DecimalStructure to, int scale, DecimalRoundMod mode) {
        if (from == to) {
            // copy as new decimal object
            from = to.copy();
        }
        // clear status of to
        to.reset();

        return doDecimalRound(from, to, scale, mode);
    }

    /**
     * @param buffer Buffer pre-allocated by caller
     */
    public static int setLongWithScale(DecimalStructure buffer, DecimalStructure result,
                                       long longVal, int scale) {
        buffer.reset();
        // parse long & set scale.
        DecimalConverter.longToDecimal(longVal, buffer);
        // shift by scale value.
        shift(buffer, buffer, -scale);

        return round(buffer, result, scale, HALF_UP);
    }

    /**
     * @param lowBits unsigned long
     * @param buffer Buffer pre-allocated by caller
     */
    public static int setDecimal128WithScale(DecimalStructure buffer, DecimalStructure result,
                                             long lowBits, long highBits, int scale) {
        buffer.reset();
        result.reset();
        byte[] int128Bytes = BigDecimalUtil.fastInt128ToBytes(lowBits, highBits);
        DecimalConverter.parseString(int128Bytes, buffer, false);
        // shift by scale value.
        shift(buffer, buffer, -scale);
        return round(buffer, result, scale, HALF_UP);
    }

    @VisibleForTesting
    public static long[] convertToDecimal128(Decimal decimal) {
        long[] decimal128 = new long[2];
        convertToDecimal128(decimal, decimal128);
        return decimal128;
    }

    @VisibleForTesting
    public static Decimal convert128ToDecimal(long[] decimal128, int scale) {
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        setDecimal128WithScale(buffer, result, decimal128[0], decimal128[1], scale);
        return new Decimal(result);
    }

    /**
     * low performance, should be used in test only
     */
    @VisibleForTesting
    public static void convertToDecimal128(Decimal decimal, long[] result) {
        if (!DecimalConverter.isDecimal128(decimal.precision())) {
            throw new IllegalArgumentException("Decimal precision: " + decimal.precision()
                + " exceeds range of decimal128: " + MAX_128_BIT_PRECISION);
        }
        DecimalStructure bufferStructure = new DecimalStructure();
        Decimal unscaledDecimal = decimal;
        if (decimal.scale() != 0) {
            unscaledDecimal = new Decimal(bufferStructure);
            FastDecimalUtils.shift(decimal.getDecimalStructure(), bufferStructure,
                decimal.scale());
        }
        BigInteger bigInteger;
        if (decimal.getDecimalStructure().isZero()) {
            bigInteger = BigInteger.ZERO;
        } else {
            bigInteger = new BigInteger(unscaledDecimal.toString());
        }
        boolean isNeg = bigInteger.signum() < 0;
        bigInteger = bigInteger.abs();
        long low = 0L, high = 0L;
        byte[] byteArray = bigInteger.toByteArray();
        if (byteArray.length <= 8) {
            for (int i = 0; i < byteArray.length; i++) {
                low = (low << 8) | (byteArray[i] & 0xFF);
            }
        } else if (byteArray.length <= 16) {
            int lowStart = byteArray.length - 8;
            for (int i = 0; i < lowStart; i++) {
                high = (high << 8) | (byteArray[i] & 0xFF);
            }
            for (int i = lowStart; i < byteArray.length; i++) {
                low = (low << 8) | (byteArray[i] & 0xFF);
            }
        } else {
            throw new IllegalArgumentException("Decimal representation is larger than 128 bits");
        }
        if (isNeg) {
            low = ~low + 1;
            high = ~high;
            if (low == 0) {
                high += 1;
            }
        }
        result[0] = low;
        result[1] = high;
    }

    protected static int doAdd(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        int error;
        int from1IntWords = DecimalTypeBase.roundUp(from1.getIntegers());
        int from1FracWords = DecimalTypeBase.roundUp(from1.getFractions());
        int from2IntWords = DecimalTypeBase.roundUp(from2.getIntegers());
        int from2FracWords = DecimalTypeBase.roundUp(from2.getFractions());
        int toIntWords = Math.max(from1IntWords, from2IntWords);
        int toFracWords = Math.max(from1FracWords, from2FracWords);

        int bufPos1, bufPos2, bufEndPos, bufEndPos2;
        // is there a need for extra word because of carry ?
        int x;
        if (from1IntWords > from2IntWords) {
            x = from1.getBuffValAt(0);
        } else if (from1IntWords < from2IntWords) {
            x = from2.getBuffValAt(0);
        } else {
            x = from1.getBuffValAt(0) + from2.getBuffValAt(0);
        }
        if (x > DecimalTypeBase.MAX_VALUE_IN_WORDS - 1) {
            toIntWords++;
            to.setBuffValAt(0, 0);
        }

        // fix intg and frac error
        if (toIntWords + toFracWords > DecimalTypeBase.WORDS_LEN) {
            if (toIntWords > DecimalTypeBase.WORDS_LEN) {
                toIntWords = DecimalTypeBase.WORDS_LEN;
                toFracWords = 0;
                error = DecimalTypeBase.E_DEC_OVERFLOW;
            } else {
                toFracWords = DecimalTypeBase.WORDS_LEN - toIntWords;
                error = DecimalTypeBase.E_DEC_TRUNCATED;
            }
        } else {
            error = DecimalTypeBase.E_DEC_OK;
        }
        if (error == DecimalTypeBase.E_DEC_OVERFLOW) {
            // get max decimal from this precision & scale.
            DecimalStructure max = DecimalBounds.maxValue(WORDS_LEN * DIG_PER_DEC1, 0);
            max.copyTo(to);
            return error;
        }
        int toIndex = toIntWords + toFracWords;
        to.setNeg(from1.isNeg());
        to.setIntegers(toIntWords * DIG_PER_DEC1);
        to.setFractions(Math.max(from1.getFractions(), from2.getFractions()));

        if (error != DecimalTypeBase.E_DEC_OK) {
            if (to.getFractions() > toFracWords * DIG_PER_DEC1) {
                to.setFractions(toFracWords * DIG_PER_DEC1);
            }
            if (from1FracWords > toFracWords) {
                from1FracWords = toFracWords;
            }
            if (from2FracWords > toFracWords) {
                from2FracWords = toFracWords;
            }
            if (from1IntWords > toIntWords) {
                from1IntWords = toIntWords;
            }
            if (from2IntWords > toIntWords) {
                from2IntWords = toIntWords;
            }
        }

        DecimalStructure dec1 = from1, dec2 = from2;
        /* part 1 - max(frac) ... min (frac) */
        if (from1FracWords > from2FracWords) {
            bufPos1 = from1IntWords + from1FracWords;
            bufEndPos = from1IntWords + from2FracWords;
            bufPos2 = from2IntWords + from2FracWords;
            bufEndPos2 = from1IntWords > from2IntWords ? from1IntWords - from2IntWords : 0;
        } else {
            bufPos1 = from2IntWords + from2FracWords;
            bufEndPos = from2IntWords + from1FracWords;
            bufPos2 = from1IntWords + from1FracWords;
            bufEndPos2 = from2IntWords > from1IntWords ? from2IntWords - from1IntWords : 0;
            dec1 = from2;
            dec2 = from1;
        }
        while (bufPos1 > bufEndPos) {
            toIndex--;
            bufPos1--;
            to.setBuffValAt(toIndex, dec1.getBuffValAt(bufPos1));
        }

        /* part 2 - min(frac) ... min(intg) */
        int carry = 0;
        while (bufPos1 > bufEndPos2) {
            // add adds a and b and carry, returns the sum and new carry.
            int sum = dec1.getBuffValAt(--bufPos1) + dec2.getBuffValAt(--bufPos2) + carry;
            if (sum >= DecimalTypeBase.DIG_BASE) {
                carry = 1;
                sum -= DecimalTypeBase.DIG_BASE;
            } else {
                carry = 0;
            }
            to.setBuffValAt(--toIndex, sum);
        }

        /* part 3 - min(intg) ... max(intg) */
        bufEndPos = 0;
        if (from1IntWords > from2IntWords) {
            bufPos1 = from1IntWords - from2IntWords;
            dec1 = from1;
        } else {
            bufPos1 = from2IntWords - from1IntWords;
            dec1 = from2;
        }
        while (bufPos1 > bufEndPos) {
            // add a and b and carry, returns the sum and new carry.
            int sum = dec1.getBuffValAt(--bufPos1) + 0 + carry;
            if (sum >= DecimalTypeBase.DIG_BASE) {
                carry = 1;
                sum -= DecimalTypeBase.DIG_BASE;
            } else {
                carry = 0;
            }
            to.setBuffValAt(--toIndex, sum);
        }
        if (carry > 0) {
            to.setBuffValAt(--toIndex, 1);
        }

        return error;
    }

    protected static int doMul(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        int error;
        int from1IntWords = DecimalTypeBase.roundUp(from1.getIntegers());
        int from1FracWords = DecimalTypeBase.roundUp(from1.getFractions());
        int from2IntWords = DecimalTypeBase.roundUp(from2.getIntegers());
        int from2FracWords = DecimalTypeBase.roundUp(from2.getFractions());
        int toIntWords = DecimalTypeBase.roundUp(from1.getIntegers() + from2.getIntegers());
        int toFracWords = from1FracWords + from2FracWords;

        int bufPos1 = from1IntWords;
        int bufPos2 = from2IntWords;
        int toPos = 0;
        int tmp1 = toIntWords;
        int tmp2 = toFracWords;

        // fix integers & fractions error.
        if (toIntWords + toFracWords > DecimalTypeBase.WORDS_LEN) {
            if (toIntWords > DecimalTypeBase.WORDS_LEN) {
                toIntWords = DecimalTypeBase.WORDS_LEN;
                toFracWords = 0;
                error = DecimalTypeBase.E_DEC_OVERFLOW;
            } else {
                toFracWords = DecimalTypeBase.WORDS_LEN - toIntWords;
                error = DecimalTypeBase.E_DEC_TRUNCATED;
            }
        } else {
            error = DecimalTypeBase.E_DEC_OK;
        }

        // ensure the field values of result decimal.
        to.setNeg(from1.isNeg() != from2.isNeg());
        to.setFractions(from1.getFractions() + from2.getFractions());
        if (to.getFractions() > DecimalTypeBase.NOT_FIXED_DEC) {
            to.setFractions(DecimalTypeBase.NOT_FIXED_DEC);
        }
        to.setIntegers(toIntWords * DIG_PER_DEC1);
        if (error == DecimalTypeBase.E_DEC_OVERFLOW) {
            return error;
        }

        // for error
        if (error != DecimalTypeBase.E_DEC_OK) {
            if (to.getFractions() > toFracWords * DIG_PER_DEC1) {
                to.setFractions(toFracWords * DIG_PER_DEC1);
            }
            if (to.getIntegers() > toIntWords * DIG_PER_DEC1) {
                to.setIntegers(toIntWords * DIG_PER_DEC1);
            }
            if (tmp1 > toIntWords) {
                tmp1 -= toIntWords;
                tmp2 = tmp1 >> 1;
                from2IntWords -= tmp1 - tmp2;
                from1FracWords = 0;
                from2FracWords = 0;
            } else {
                tmp2 -= toFracWords;
                tmp1 = tmp2 >> 1;
                if (from1FracWords <= from2FracWords) {
                    from1FracWords -= tmp1;
                    from2FracWords -= tmp2 - tmp1;
                } else {
                    from2FracWords -= tmp1;
                    from1FracWords -= tmp2 - tmp1;
                }
            }
        }
        int startPos0 = toIntWords + toFracWords - 1;
        int startPos2 = bufPos2 + from2FracWords - 1;
        int endPos1 = bufPos1 - from1IntWords;
        int endPos2 = bufPos2 - from2IntWords;

        int carry, hi, lo;
        long p;
        for (bufPos1 += from1FracWords - 1; bufPos1 >= endPos1; bufPos1--, startPos0--) {
            carry = 0;
            for (toPos = startPos0, bufPos2 = startPos2; bufPos2 >= endPos2; bufPos2--, toPos--) {
                p = (from1.getBuffValAt(bufPos1) & DecimalTypeBase.LONG_MASK) * (from2.getBuffValAt(bufPos2)
                    & DecimalTypeBase.LONG_MASK);
                hi = (int) (p / DecimalTypeBase.DIG_BASE);
                lo = (int) (p - (hi & DecimalTypeBase.LONG_MASK) * DecimalTypeBase.DIG_BASE);

                carry = add2(to, toPos, lo, carry);

                carry += hi;
            }
            if (carry > 0) {
                if (toPos < 0) {
                    return DecimalTypeBase.E_DEC_OVERFLOW;
                }

                carry = add2(to, toPos, 0, carry);
            }
            for (toPos--; carry > 0; toPos--) {
                if (toPos < 0) {
                    return DecimalTypeBase.E_DEC_OVERFLOW;
                }
                // add a and b and carry, returns the sum and new carry.
                carry = add(to, toPos, 0, carry);
            }
        }

        /* Now we have to check for -0.000 case */
        if (to.isNeg()) {
            int idx = 0;
            int end = toIntWords + toFracWords;
            for (; ; ) {
                if (to.getBuffValAt(idx) != 0) {
                    break;
                }
                idx++;
                /* We got decimal zero */
                if (idx == end) {
                    to.toZero();
                    break;
                }
            }
        }

        toPos = 0;
        int toMove = toIntWords + DecimalTypeBase.roundUp(to.getFractions());
        while (to.getBuffValAt(toPos) == 0 && to.getIntegers() > DIG_PER_DEC1) {
            toPos++;
            to.setIntegers(to.getIntegers() - DIG_PER_DEC1);
            toMove--;
        }
        if (toPos > 0) {
            int curIdx = 0;
            for (; toMove > 0; curIdx++, toPos++, toMove--) {
                to.setBuffValAt(curIdx, to.getBuffValAt(toPos));
            }
        }
        return error;
    }

    /**
     * to=from1-from2.
     * if to==null, return -1/0/+1 - the result of the comparison
     */
    protected static int[] doSub(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        int error;
        int from1IntWords = DecimalTypeBase.roundUp(from1.getIntegers());
        int from1FracWords = DecimalTypeBase.roundUp(from1.getFractions());
        int from2IntWords = DecimalTypeBase.roundUp(from2.getIntegers());
        int from2FracWords = DecimalTypeBase.roundUp(from2.getFractions());
        int toFracWords = Math.max(from1FracWords, from2FracWords);

        /* let carry:=1 if from2 > from1 */
        int start1 = 0, start2 = 0;
        int stop1 = from1IntWords, stop2 = from2IntWords;
        int bufPos1 = 0, bufPos2 = 0;
        if (from1.getBuffValAt(bufPos1) == 0) {
            for (; bufPos1 < stop1 && from1.getBuffValAt(bufPos1) == 0; ) {
                bufPos1++;
            }
            start1 = bufPos1;
            from1IntWords = stop1 - bufPos1;
        }
        if (from2.getBuffValAt(bufPos2) == 0) {
            for (; bufPos2 < stop2 && from2.getBuffValAt(bufPos2) == 0; ) {
                bufPos2++;
            }
            start2 = bufPos2;
            from2IntWords = stop2 - bufPos2;
        }

        int carry = 0;
        if (from2IntWords > from1IntWords) {
            carry = 1;
        } else if (from2IntWords == from1IntWords) {
            int end1 = stop1 + from1FracWords - 1;
            int end2 = stop2 + from2FracWords - 1;
            while (bufPos1 <= end1 && from1.getBuffValAt(end1) == 0) {
                end1--;
            }
            while (bufPos2 <= end2 && from2.getBuffValAt(end2) == 0) {
                end2--;
            }
            from1FracWords = end1 - stop1 + 1;
            from2FracWords = end2 - stop2 + 1;
            for (; bufPos1 <= end1 && bufPos2 <= end2 && from1.getBuffValAt(bufPos1) == from2.getBuffValAt(bufPos2); ) {
                bufPos1++;
                bufPos2++;
            }
            if (bufPos1 <= end1) {
                if (bufPos2 <= end2 && from2.getBuffValAt(bufPos2) > from1.getBuffValAt(bufPos1)) {
                    carry = 1;
                } else {
                    carry = 0;
                }
            } else {
                /* short-circuit everything: from1 == from2 */
                if (bufPos2 <= end2) {
                    carry = 1;
                } else {
                    if (to == null) {
                        return new int[] {0, DecimalTypeBase.E_DEC_OK};
                    }
                    to.toZero();
                    return new int[] {0, DecimalTypeBase.E_DEC_OK};
                }
            }
        }

        if (to == null) {
            if ((carry > 0) == from1.isNeg()) { // from2 is negative too.
                return new int[] {1, 1};
            }
            return new int[] {-1, 1};
        }

        to.setNeg(from1.isNeg());

        /* ensure that always from1 > from2 (and intg1 >= intg2) */
        if (carry > 0) {
            Object tmp = from1;
            from1 = from2;
            from2 = (DecimalStructure) tmp;

            tmp = start1;
            start1 = start2;
            start2 = (int) tmp;

            tmp = from1IntWords;
            from1IntWords = from2IntWords;
            from2IntWords = (int) tmp;

            tmp = from1FracWords;
            from1FracWords = from2FracWords;
            from2FracWords = (int) tmp;

            to.setNeg(!to.isNeg());
        }

        // fix intg and frac error
        if (from1IntWords + toFracWords > DecimalTypeBase.WORDS_LEN) {
            if (from1IntWords > DecimalTypeBase.WORDS_LEN) {
                from1IntWords = DecimalTypeBase.WORDS_LEN;
                toFracWords = 0;
                error = DecimalTypeBase.E_DEC_OVERFLOW;
            } else {
                toFracWords = DecimalTypeBase.WORDS_LEN - from1IntWords;
                error = DecimalTypeBase.E_DEC_TRUNCATED;
            }
        } else {
            error = DecimalTypeBase.E_DEC_OK;
        }

        int bufPos0 = from1IntWords + toFracWords;
        to.setFractions(from1.getFractions());
        if (to.getFractions() < from2.getFractions()) {
            to.setFractions(from2.getFractions());
        }
        to.setIntegers(from1IntWords * DIG_PER_DEC1);
        if (error != DecimalTypeBase.E_DEC_OK) {
            if (to.getFractions() > toFracWords * DIG_PER_DEC1) {
                to.setFractions(toFracWords * DIG_PER_DEC1);
            }
            if (from1FracWords > toFracWords) {
                from1FracWords = toFracWords;
            }
            if (from2FracWords > toFracWords) {
                from2FracWords = toFracWords;
            }
            if (from2IntWords > from1IntWords) {
                from2IntWords = from1IntWords;
            }
        }
        carry = 0;

        /* part 1 - max(frac) ... min (frac) */
        if (from1FracWords > from2FracWords) {
            bufPos1 = start1 + from1IntWords + from1FracWords;
            stop1 = start1 + from1IntWords + from2FracWords;
            bufPos2 = start2 + from2IntWords + from2FracWords;
            while (toFracWords > from1FracWords) {
                toFracWords--;
                to.setBuffValAt(--bufPos0, 0);
            }
            while (bufPos1 > stop1) {
                to.setBuffValAt(--bufPos0, from1.getBuffValAt(--bufPos1));
            }
        } else {
            bufPos1 = start1 + from1IntWords + from1FracWords;
            bufPos2 = start2 + from2IntWords + from2FracWords;
            stop2 = start2 + from2IntWords + from1FracWords;
            while (toFracWords > from2FracWords) {
                toFracWords--;
                to.setBuffValAt(--bufPos0, 0);
            }
            while (bufPos2 > stop2) {
                carry = sub(to, --bufPos0, 0, from2.getBuffValAt(--bufPos2), carry);
            }
        }

        /* part 2 - min(frac) ... intg2 */
        while (bufPos2 > start2) {
            carry = sub(to, --bufPos0, from1.getBuffValAt(--bufPos1), from2.getBuffValAt(--bufPos2), carry);
        }

        /* part 3 - intg2 ... intg1 */
        while (carry > 0 && bufPos1 > start1) {
            carry = sub(to, --bufPos0, from1.getBuffValAt(--bufPos1), 0, carry);
        }
        while (bufPos1 > start1) {
            to.setBuffValAt(--bufPos0, from1.getBuffValAt(--bufPos1));
        }
        while (bufPos0 > 0) {
            to.setBuffValAt(--bufPos0, 0);
        }
        return new int[] {0, error};
    }

    /**
     * naive division algorithm (Knuth's Algorithm D in 4.3.1) -
     * it's ok for short numbers
     * <p>
     * XXX if this library is to be used with huge numbers of thousands of
     * digits, fast division must be implemented
     *
     * @param from1 dividend
     * @param from2 divisor
     * @param to quotient
     * @param mod mod
     * @param scaleIncr increment of fraction
     * @return E_DEC_OK / E_DEC_TRUNCATED / E_DEC_OVERFLOW / E_DEC_DIV_ZERO
     */
    protected static int doDiv(DecimalStructure from1, DecimalStructure from2, DecimalStructure to,
                               DecimalStructure mod, int scaleIncr) {
        int error = DecimalTypeBase.E_DEC_OK;

        //    frac* - number of digits in fractional part of the number
        //    prec* - precision of the number
        //    intg* - number of digits in the integer part
        //    buf* - buffer having the actual number
        //    All variables ending with 0 - like frac0, intg0 etc are
        //    for the final result. Similarly frac1, intg1 etc are for
        //    the first number and frac2, intg2 etc are for the second number

        int from1Fractions = DecimalTypeBase.roundUp(from1.getFractions()) * DIG_PER_DEC1;
        int from1Precision = from1.getIntegers() + from1Fractions;
        int from2Fractions = DecimalTypeBase.roundUp(from2.getFractions()) * DIG_PER_DEC1;
        int from2Precision = from2.getIntegers() + from2Fractions;

        if (mod != null) {
            to = mod;
        }

        // removing all the leading zeroes in the second number. Leading zeroes are
        // added later to the result.
        int i = ((from2Precision - 1) % DIG_PER_DEC1) + 1;
        int bufPos2 = 0;
        while (from2Precision > 0 && from2.getBuffValAt(bufPos2) == 0) {
            from2Precision -= i;
            i = DIG_PER_DEC1;
            bufPos2++;
        }
        if (from2Precision <= 0) {
            /* short-circuit everything: from2 == 0 */
            return DecimalTypeBase.E_DEC_DIV_ZERO;
        }

        // Remove the remanining zeroes . For ex: for 0.000000000001
        // the above while loop removes 9 zeroes and the result will have 0.0001
        // these remaining zeroes are removed here
        from2Precision -= countLeadingZeros((from2Precision - 1) % DIG_PER_DEC1, from2.getBuffValAt(bufPos2));

        // Do the same for the first number. Remove the leading zeroes.
        // Check if the number is actually 0. Then remove the remaining zeroes.
        i = ((from1Precision - 1) % DIG_PER_DEC1) + 1;
        int bufPos1 = 0;
        while (from1Precision > 0 && from1.getBuffValAt(bufPos1) == 0) {
            from1Precision -= i;
            i = DIG_PER_DEC1;
            bufPos1++;
        }
        if (from1Precision <= 0) {
            /* short-circuit everything: from1 == 0 */
            to.toZero();
            return DecimalTypeBase.E_DEC_OK;
        }
        from1Precision -= countLeadingZeros((from1Precision - 1) % DIG_PER_DEC1, from1.getBuffValAt(bufPos1));

        // let's fix scaleIncr, taking into account frac1,frac2 increase
        scaleIncr -= (from1Fractions - from1.getFractions() + from2Fractions - from2.getFractions());

        if (scaleIncr < 0) {
            scaleIncr = 0;
        }

        int toIntegers = (from1Precision - from1Fractions) - (from2Precision - from2Fractions);
        if (from1.getBuffValAt(bufPos1) >= from2.getBuffValAt(bufPos2)) {
            toIntegers++;
        }
        int toIntWords;
        int toFracWords = 0;
        if (toIntegers < 0) {
            toIntegers /= DIG_PER_DEC1;
            toIntWords = 0;
        } else {
            toIntWords = DecimalTypeBase.roundUp(toIntegers);
        }

        if (mod != null) {
            // we're calculating N1 % N2. The result will have
            // frac=max(frac1, frac2), as for subtraction intg=intg2
            to.setNeg(from1.isNeg());
            to.setFractions(Math.max(from1.getFractions(), from2.getFractions()));
        } else {
            // we're calculating N1/N2. N1 is in the buf1, has prec1 digits
            // N2 is in the buf2, has prec2 digits. Scales are frac1 and
            // frac2 accordingly. Thus, the result will have
            //         frac = ROUND_UP(frac1+frac2+scale_incr)
            //      and
            //         intg = (prec1-frac1) - (prec2-frac2) + 1
            //         prec = intg+frac
            toFracWords = DecimalTypeBase.roundUp(from1Fractions + from2Fractions + scaleIncr);
            // fix intg and frac error
            if (toIntWords + toFracWords > DecimalTypeBase.WORDS_LEN) {
                if (toIntWords > DecimalTypeBase.WORDS_LEN) {
                    toIntWords = DecimalTypeBase.WORDS_LEN;
                    toFracWords = 0;
                    error = DecimalTypeBase.E_DEC_OVERFLOW;
                } else {
                    toFracWords = DecimalTypeBase.WORDS_LEN - toIntWords;
                    error = DecimalTypeBase.E_DEC_TRUNCATED;
                }
            } else {
                error = DecimalTypeBase.E_DEC_OK;
            }

            to.setNeg(from1.isNeg() != from2.isNeg());
            to.setIntegers(toIntWords * DIG_PER_DEC1);
            to.setFractions(toFracWords * DIG_PER_DEC1);
        }
        int toBufPos = 0;
        int stopTo = toIntWords + toFracWords;
        if (mod == null) {
            while (toIntegers < 0 && toBufPos < DecimalTypeBase.WORDS_LEN) {
                to.setBuffValAt(toBufPos, 0);
                toBufPos++;
                toIntegers++;
            }
        }
        i = DecimalTypeBase.roundUp(from1Precision);
        int len1 = i + DecimalTypeBase.roundUp(2 * from2Fractions + scaleIncr + 1) + 1;
        if (len1 < 3) {
            len1 = 3;
        }

        int[] tmp1 = new int[len1];

        for (int sourcePos = bufPos1, destPos = 0; destPos < i; sourcePos++, destPos++) {
            tmp1[destPos] = from1.getBuffValAt(sourcePos);
        }

        int start1 = 0;
        int stop1;
        int start2 = bufPos2;
        int stop2 = bufPos2 + DecimalTypeBase.roundUp(from2Precision) - 1;

        /* removing end zeroes */
        while (from2.getBuffValAt(stop2) == 0 && stop2 >= start2) {
            stop2--;
        }
        int len2 = stop2 - start2;
        stop2++;

        //     calculating norm2 (normalized *start2) - we need *start2 to be large
        //    (at least > DIG_BASE/2), but unlike Knuth's Alg. D we don't want to
        //    normalize input numbers (as we don't make a copy of the divisor).
        //    Thus we normalize first dec1 of buf2 only, and we'll normalize *start1
        //    on the fly for the purpose of guesstimation only.
        //    It's also faster, as we're saving on normalization of buf2

        long normFactor = DecimalTypeBase.DIG_BASE / (long) (from2.getBuffValAt(start2) + 1);
        int norm2 = (int) (normFactor * (long) (from2.getBuffValAt(start2)));
        if (len2 > 0) {
            norm2 += (int) (normFactor * (long) (from2.getBuffValAt(start2 + 1)) / DecimalTypeBase.DIG_BASE);
        }
        int dcarry = 0;
        if (tmp1[start1] < from2.getBuffValAt(start2)) {
            dcarry = tmp1[start1];
            start1++;
        }

        // main loop
        long guess;
        for (; toBufPos < stopTo; toBufPos++) {
            // short-circuit, if possible
            if (dcarry == 0 && tmp1[start1] < from2.getBuffValAt(start2)) {
                guess = 0;
            } else {
                /* D3: make a guess */
                long x = (long) (tmp1[start1]) + (long) (dcarry) * DecimalTypeBase.DIG_BASE;
                long y = (long) (tmp1[start1 + 1]);
                guess = (normFactor * x + normFactor * y / DecimalTypeBase.DIG_BASE) / (long) (norm2);
                if (guess >= DecimalTypeBase.DIG_BASE) {
                    guess = DecimalTypeBase.DIG_BASE - 1;
                }

                if (len2 > 0) {
                    // removed normalization here
                    if ((from2.getBuffValAt(start2 + 1) * guess
                        > (x - guess * (from2.getBuffValAt(start2))) * DecimalTypeBase.DIG_BASE + y)) {
                        guess--;
                    }
                    if ((from2.getBuffValAt(start2 + 1) * guess
                        > (x - guess * (from2.getBuffValAt(start2))) * DecimalTypeBase.DIG_BASE + y)) {
                        guess--;
                    }
                }

                /* D4: multiply and subtract */
                bufPos2 = stop2;
                bufPos1 = start1 + len2;
                int carry;
                for (carry = 0; bufPos2 > start2; bufPos1--) {
                    int hi, lo;
                    bufPos2--;
                    x = guess * (long) (from2.getBuffValAt(bufPos2));
                    hi = (int) (x / DecimalTypeBase.DIG_BASE);
                    lo = (int) (x - (long) (hi) * DecimalTypeBase.DIG_BASE);
                    carry = sub2(tmp1, bufPos1, tmp1[bufPos1], lo, carry);
                    carry += hi;
                }
                if (dcarry < carry) {
                    carry = 1;
                } else {
                    carry = 0;
                }

                /* D5: check the remainder */
                if (carry > 0) {
                    /* D6: correct the guess */
                    guess--;
                    bufPos2 = stop2;
                    bufPos1 = start1 + len2;
                    for (carry = 0; bufPos2 > start2; bufPos1--) {
                        bufPos2--;
                        carry = add(tmp1, bufPos1, from2.getBuffValAt(bufPos2), carry);
                    }
                }
            }
            if (mod == null) {
                to.setBuffValAt(toBufPos, (int) guess);
            }
            dcarry = tmp1[start1];
            start1++;
        }
        if (mod != null) {
            // now the result is in tmp1, it has
            // intg=prec1-frac1  if there were no leading zeroes.
            // If leading zeroes were present, they have been removed
            // earlier. We need to now add them back to the result.
            //  frac=max(frac1, frac2)=to->frac
            if (dcarry != 0) {
                start1--;
                tmp1[start1] = dcarry;
            }
            toBufPos = 0;

            toIntegers = from1Precision - from1Fractions - start1 * DIG_PER_DEC1;
            if (toIntegers < 0) {
                // If leading zeroes in the fractional part were earlier stripped
                toIntWords = toIntegers / DIG_PER_DEC1;
            } else {
                toIntWords = DecimalTypeBase.roundUp(toIntegers);
            }

            toFracWords = DecimalTypeBase.roundUp(to.getFractions());

            if (toIntWords == 0 && toFracWords == 0) {
                to.toZero();
                return error;
            }
            if (toIntWords <= 0) {
                if (-toIntWords >= DecimalTypeBase.WORDS_LEN) {
                    to.toZero();
                    return DecimalTypeBase.E_DEC_TRUNCATED;
                }
                stop1 = start1 + toIntWords + toFracWords;
                toFracWords += toIntWords;
                to.setIntegers(0);
                while (toIntWords < 0) {
                    to.setBuffValAt(toBufPos, 0);
                    toBufPos++;
                    toIntWords++;
                }
            } else {
                if (toIntWords > DecimalTypeBase.WORDS_LEN) {
                    to.setIntegers(DIG_PER_DEC1 * DecimalTypeBase.WORDS_LEN);
                    to.setFractions(0);
                    return DecimalTypeBase.E_DEC_OVERFLOW;
                }
                stop1 = start1 + toIntWords + toFracWords;
                to.setIntegers(Math.min(toIntWords * DIG_PER_DEC1, from2.getIntegers()));
            }
            if (toIntWords + toFracWords > DecimalTypeBase.WORDS_LEN) {
                stop1 -= toIntWords + toFracWords - DecimalTypeBase.WORDS_LEN;
                toFracWords = DecimalTypeBase.WORDS_LEN - toIntWords;
                to.setFractions(toFracWords * DIG_PER_DEC1);
                error = DecimalTypeBase.E_DEC_TRUNCATED;
            }
            while (start1 < stop1) {
                to.setBuffValAt(toBufPos, tmp1[start1]);
                toBufPos++;
                start1++;
            }
        }

        // remove leading zeros
        int[] removedResults = to.removeLeadingZeros();
        toBufPos = removedResults[0];
        to.setIntegers(removedResults[1]);

        if (toBufPos != 0) {
            for (int sourcePos = toBufPos, destPos = 0; destPos < DecimalTypeBase.WORDS_LEN - toBufPos;
                 destPos++, sourcePos++) {
                to.setBuffValAt(destPos, to.getBuffValAt(sourcePos));
            }
        }

        if (to.isZero()) {
            to.setNeg(false);
        }
        return error;
    }

    /**
     * short-circuit from do sub.
     *
     * @param from1 compare from
     * @param from2 compare to
     * @return -1/0/1
     */
    protected static int doCompare(DecimalStructure from1, DecimalStructure from2) {
        int from1IntWords = DecimalTypeBase.roundUp(from1.getIntegers());
        int from1FracWords = DecimalTypeBase.roundUp(from1.getFractions());
        int from2IntWords = DecimalTypeBase.roundUp(from2.getIntegers());
        int from2FracWords = DecimalTypeBase.roundUp(from2.getFractions());

        int bufPos1 = 0, bufPos2 = 0;
        int bufEndPos1 = from1IntWords, bufEndPos2 = from2IntWords;

        // remove leading zeros
        if (from1.getBuffValAt(bufPos1) == 0) {
            while (bufPos1 < bufEndPos1 && from1.getBuffValAt(bufPos1) == 0) {
                bufPos1++;
            }
            from1IntWords = bufEndPos1 - bufPos1;
        }
        if (from2.getBuffValAt(bufPos2) == 0) {
            while (bufPos2 < bufEndPos2 && from2.getBuffValAt(bufPos2) == 0) {
                bufPos2++;
            }
            from2IntWords = bufEndPos2 - bufPos2;
        }

        int carry = 0;
        if (from2IntWords > from1IntWords) {
            carry = 1;
        } else if (from2IntWords == from1IntWords) {
            int end1 = bufEndPos1 + from1FracWords - 1;
            int end2 = bufEndPos2 + from2FracWords - 1;
            while (bufPos1 <= end1 && from1.getBuffValAt(end1) == 0) {
                end1--;
            }
            while (bufPos2 <= end2 && from2.getBuffValAt(end2) == 0) {
                end2--;
            }
            while (bufPos1 <= end1 && bufPos2 <= end2 && from1.getBuffValAt(bufPos1) == from2.getBuffValAt(bufPos2)) {
                bufPos1++;
                bufPos2++;
            }
            if (bufPos1 <= end1) {
                if (bufPos2 <= end2 && from2.getBuffValAt(bufPos2) > from1.getBuffValAt(bufPos1)) {
                    carry = 1;
                } else {
                    carry = 0;
                }
            } else {
                if (bufPos2 <= end2) {
                    carry = 1;
                } else {
                    return 0;
                }
            }
        }

        if ((carry > 0) == from1.isNeg()) {
            // from2 is negative too.
            return 1;
        }
        return -1;
    }

    protected static int doShift(DecimalStructure dec, int shift) {
        // index of first non zero digit (all indexes from 0)
        int beg = 0;
        // index of position after last decimal digit
        int end = 0;

        // index of digit position just after point
        int point = DecimalTypeBase.roundUp(dec.getIntegers()) * DIG_PER_DEC1;
        // new point position
        int newPoint = point + shift;

        // number of digits in result
        int integers, fractions;
        // length of result and new fraction in big digits
        int newLen, newFracLen;
        // return code
        int err = E_DEC_OK;
        int newFront;

        if (shift == 0) {
            return E_DEC_OK;
        }

        int[] bounds = digitsBounds(dec);
        beg = bounds[0];
        end = bounds[1];

        if (beg == end) {
            dec.toZero();
            return E_DEC_OK;
        }

        integers = newPoint - beg;
        if (integers < 0) {
            integers = 0;
        }
        fractions = end - newPoint;
        if (fractions < 0) {
            fractions = 0;
        }

        newLen = roundUp(integers) + (newFracLen = roundUp(fractions));
        if (newLen > WORDS_LEN) {
            int lack = newLen - WORDS_LEN;
            int diff;

            if (newFracLen < lack) {
                // lack more then we have in fraction
                return E_DEC_OVERFLOW;
            }

            // cat off fraction part to allow new number to fit in our buffer
            err = E_DEC_TRUNCATED;
            newFracLen -= lack;
            diff = fractions - (newFracLen * DIG_PER_DEC1);
            // Make rounding method as parameter?
            doDecimalRound(dec, dec, end - point - diff, HALF_UP);
            end -= diff;
            fractions = newFracLen * DIG_PER_DEC1;

            if (end <= beg) {
                // we lost all digits (they will be shifted out of buffer), so we can
                // just return 0
                dec.toZero();
                return E_DEC_TRUNCATED;
            }
        }

        if (shift % DIG_PER_DEC1 != 0) {
            int lMiniShift, rMiniShift, miniShift;
            boolean doLeft;

            // Calculate left/right shift to align decimal digits inside our bug digits correctly
            if (shift > 0) {
                lMiniShift = shift % DIG_PER_DEC1;
                rMiniShift = DIG_PER_DEC1 - lMiniShift;
                // It is left shift so prefer left shift, but if we have not place from
                // left, we have to have it from right, because we checked length of result
                doLeft = lMiniShift <= beg;
            } else {
                rMiniShift = (-shift) % DIG_PER_DEC1;
                lMiniShift = DIG_PER_DEC1 - rMiniShift;
                // see comment above
                doLeft = !((WORDS_LEN * DIG_PER_DEC1 - end) >= rMiniShift);
            }
            if (doLeft) {
                doMiniLeftShift(dec, lMiniShift, beg, end);
                miniShift = -lMiniShift;
            } else {
                doMiniRightShift(dec, rMiniShift, beg, end);
                miniShift = rMiniShift;
            }
            newPoint += miniShift;

            // If number is shifted and correctly aligned in buffer we can finish
            if ((shift += miniShift) == 0 && (newPoint - integers) < DIG_PER_DEC1) {
                dec.setIntegers(integers);
                dec.setFractions(fractions, true);
                // already shifted as it should be
                return err;
            }
            beg += miniShift;
            end += miniShift;
        }

        // if new 'decimal front' is in first digit, we do not need move digits
        if ((newFront = (newPoint - integers)) >= DIG_PER_DEC1 ||
            newFront < 0) {
            // need to move digits
            int dShift;
            int toPos;
            int barierPos;
            if (newFront > 0) {
                // move left
                dShift = newFront / DIG_PER_DEC1;
                toPos = roundUp(beg + 1) - 1 - dShift;
                barierPos = roundUp(end) - 1 - dShift;

                for (; toPos <= barierPos; toPos++) {
                    dec.setBuffValAt(toPos, dec.getBuffValAt(toPos + dShift));
                }
                for (barierPos += dShift; toPos <= barierPos; toPos++) {
                    dec.setBuffValAt(toPos, 0);
                }
                dShift = -dShift;
            } else {
                // move right
                dShift = (1 - newFront) / DIG_PER_DEC1;
                toPos = roundUp(end) - 1 + dShift;
                barierPos = roundUp(beg + 1) - 1 + dShift;

                for (; toPos >= barierPos; toPos--) {
                    dec.setBuffValAt(toPos, dec.getBuffValAt(toPos - dShift));
                }
                for (barierPos -= dShift; toPos >= barierPos; toPos--) {
                    dec.setBuffValAt(toPos, 0);
                }
            }
            dShift *= DIG_PER_DEC1;
            beg += dShift;
            end += dShift;
            newPoint += dShift;
        }

        // If there are gaps then fill ren with 0.
        // Only one of following 'for' loops will work because beg <= end
        beg = roundUp(beg + 1) - 1;
        end = roundUp(end) - 1;

        // We don't want negative new_point below
        if (newPoint != 0) {
            newPoint = roundUp(newPoint) - 1;
        }

        if (newPoint > end) {
            do {
                dec.setBuffValAt(newPoint, 0);
            } while (--newPoint > end);
        } else {
            for (; newPoint < beg; newPoint++) {
                dec.setBuffValAt(newPoint, 0);
            }
        }
        dec.setIntegers(integers);
        dec.setFractions(fractions, true);
        return err;
    }

    /**
     * Rounds the decimal to "scale" digits
     *
     * @param from decimal to round
     * @param to result buffer. from == to is allowed
     * @param scale to what position to round. can be negative.
     * @param mode round to nearest even or truncate
     * @return error code = E_DEC_OK / E_DEC_TRUNCATED
     */
    protected static int doDecimalRound(DecimalStructure from, DecimalStructure to, int scale, DecimalRoundMod mode) {
        int frac0 = scale > 0 ? roundUp(scale) : (scale + 1) / DIG_PER_DEC1,
            frac1 = roundUp(from.getFractions()),
            intg0 = roundUp(from.getIntegers()),
            error = E_DEC_OK,
            len = WORDS_LEN;

        int roundDigit = 0;
        int buf0Pos = 0;
        int buf1Pos = 0;
        int x, y, carry = 0;

        switch (mode) {
        case HALF_UP:
        case HALF_EVEN:
            roundDigit = 5;
            break;
        case CEILING:
            roundDigit = from.isNeg() ? 10 : 0;
            break;
        case FLOOR:
            roundDigit = from.isNeg() ? 0 : 10;
            break;
        case TRUNCATE:
            roundDigit = 10;
            break;
        default:
        }

        // For my_decimal we always use len == DECIMAL_BUFF_LENGTH == 9
        // For internal testing here (ifdef MAIN) we always use len == 100/4
        if (frac0 + intg0 > len) {
            frac0 = len - intg0;
            scale = frac0 * DIG_PER_DEC1;
            error = E_DEC_TRUNCATED;
        }

        if (scale + from.getIntegers() < 0) {
            to.toZero();
            return E_DEC_OK;
        }

        if (to != from) {
            int p0 = buf0Pos + intg0 + Math.max(frac1, frac0);
            int p1 = buf1Pos + intg0 + Math.max(frac1, frac0);

            while (buf0Pos < p0) {
                to.setBuffValAt(--p1, from.getBuffValAt(--p0));
            }

            buf0Pos = 0;
            buf1Pos = 0;
            to.setNeg(from.isNeg());
            to.setIntegers(Math.min(intg0, len) * DIG_PER_DEC1);
        }

        if (frac0 > frac1) {
            buf1Pos += intg0 + frac1;
            while (frac0-- > frac1) {
                to.setBuffValAt(buf1Pos++, 0);
            }

            to.setFractions(scale, true);
            return error;
        }

        if (scale >= from.getFractions()) {
            to.setFractions(scale, true);
            return error;
        }

        buf0Pos += intg0 + frac0 - 1;
        buf1Pos += intg0 + frac0 - 1;
        if (scale == frac0 * DIG_PER_DEC1) {
            boolean doInc = false;

            switch (roundDigit) {
            case 0: {
                int p0 = buf0Pos + (frac1 - frac0);
                for (; p0 > buf0Pos; p0--) {
                    if (to.getBuffValAt(p0) != 0) {
                        doInc = true;
                        break;
                    }
                }
                break;
            }
            case 5: {
                x = to.getBuffValAt(buf0Pos + 1) / DIG_MASK;
                doInc = (x > 5) || ((x == 5) &&
                    (mode == HALF_UP || (frac0 + intg0 > 0 && (to.getBuffValAt(buf0Pos) & 1) != 0)));
                break;
            }
            default:
                break;
            }
            if (doInc) {
                if (frac0 + intg0 > 0) {
                    to.setBuffValAt(buf1Pos, to.getBuffValAt(buf1Pos) + 1);
                } else {
                    to.setBuffValAt(++buf1Pos, DIG_BASE);
                }

            } else if (frac0 + intg0 == 0) {
                to.toZero();
                return E_DEC_OK;
            }
        } else {
            int pos = frac0 * DIG_PER_DEC1 - scale - 1;

            x = to.getBuffValAt(buf1Pos) / POW_10[pos];
            y = x % 10;
            if (y > roundDigit ||
                (roundDigit == 5 && y == 5 && (mode == HALF_UP || ((x / 10) & 1) != 0))) {
                x += 10;
            }

            to.setBuffValAt(buf1Pos, POW_10[pos] * (x - y));
        }
        // In case we're rounding e.g. 1.5e9 to 2.0e9, the decimal_digit_t's inside
        // the buffer are as follows.
        // Before <1, 5e8>
        // After  <2, 5e8>
        // Hence we need to set the 2nd field to 0.
        // The same holds if we round 1.5e-9 to 2e-9.
        if (frac0 < frac1) {
            int bufPos = (scale == 0 && intg0 == 0) ? 1 : intg0 + frac0;
            int endPos = len;

            while (bufPos < endPos) {
                to.setBuffValAt(bufPos++, 0);
            }

        }
        if (to.getBuffValAt(buf1Pos) >= DIG_BASE) {
            carry = 1;
            to.setBuffValAt(buf1Pos, to.getBuffValAt(buf1Pos) - DIG_BASE);
            while (carry != 0 && --buf1Pos >= 0) {
                carry = add(to, buf1Pos, 0, carry);
            }

            if (carry != 0) {
                // shifting the number to create space for new digit
                if (frac0 + intg0 >= len) {
                    frac0--;
                    scale = frac0 * DIG_PER_DEC1;
                    error = E_DEC_TRUNCATED;
                }
                for (buf1Pos = intg0 + Math.max(frac0, 0); buf1Pos > 0; buf1Pos--) {
                    // Avoid out-of-bounds write.
                    if (buf1Pos < len) {
                        to.setBuffValAt(buf1Pos, to.getBuffValAt(buf1Pos - 1));
                    } else {
                        error = E_DEC_OVERFLOW;
                    }

                }
                to.setBuffValAt(buf1Pos, 1);
                // We cannot have more than 9 * 9 = 81 digits.
                if (to.getIntegers() < len * DIG_PER_DEC1) {
                    to.setIntegers(to.getIntegers() + 1);
                } else {
                    error = E_DEC_OVERFLOW;
                }

            }
        } else {
            for (; ; ) {
                if (to.getBuffValAt(buf1Pos) != 0) {
                    break;
                }
                if (buf1Pos-- == 0) {
                    // making 'zero' with the proper scale
                    int p0 = frac0 + 1;
                    to.setIntegers(1);
                    to.setFractions(Math.max(scale, 0), true);
                    to.setNeg(false);
                    for (buf1Pos = 0; buf1Pos < p0; buf1Pos++) {
                        to.setBuffValAt(buf1Pos, 0);
                    }
                    return E_DEC_OK;
                }
            }
        }

        // Here we  check 999.9 -> 1000 case when we need to increase intg
        int firstDig = to.getIntegers() % DIG_PER_DEC1;
        if (firstDig != 0 && (to.getBuffValAt(buf1Pos) >= POW_10[firstDig])) {
            to.setIntegers(to.getIntegers() + 1);
        }
        if (scale < 0) {
            scale = 0;
        }

        to.setFractions(scale, true);
        return error;
    }

    /**
     * Left shift for alignment of data in buffer
     *
     * @param dec decimal number which have to be shifted
     * @param shift number of decimal digits on which it should be shifted
     * @param beg bounds of decimal digits
     * @param last bounds of decimal digits
     */
    protected static void doMiniLeftShift(DecimalStructure dec, int shift, int beg, int last) {
        int fromPos = roundUp(beg + 1) - 1;
        int endPos = roundUp(last) - 1;
        int cShift = DIG_PER_DEC1 - shift;

        if (beg % DIG_PER_DEC1 < shift) {
            dec.setBuffValAt(fromPos - 1, dec.getBuffValAt(fromPos) / POW_10[cShift]);
        }
        for (; fromPos < endPos; fromPos++) {
            int v1 = dec.getBuffValAt(fromPos);
            int v2 = dec.getBuffValAt(fromPos + 1);
            dec.setBuffValAt(fromPos, (v1 % POW_10[cShift]) * POW_10[shift] + v2 / POW_10[cShift]);
        }

        dec.setBuffValAt(fromPos, (dec.getBuffValAt(fromPos) % POW_10[cShift]) * POW_10[shift]);
    }

    /**
     * Right shift for alignment of data in buffer
     *
     * @param dec decimal number which have to be shifted
     * @param shift number of decimal digits on which it should be shifted
     * @param beg bounds of decimal digits
     * @param last bounds of decimal digits
     */
    protected static void doMiniRightShift(DecimalStructure dec, int shift, int beg, int last) {
        int fromPos = roundUp(last) - 1;
        int endPos = roundUp(beg + 1) - 1;
        int cShift = DIG_PER_DEC1 - shift;

        if (DIG_PER_DEC1 - ((last - 1) % DIG_PER_DEC1 + 1) < shift) {
            dec.setBuffValAt(fromPos + 1, (dec.getBuffValAt(fromPos) % POW_10[shift]) * POW_10[cShift]);
        }
        for (; fromPos > endPos; fromPos--) {
            int v1 = dec.getBuffValAt(fromPos);
            int v2 = dec.getBuffValAt(fromPos - 1);
            dec.setBuffValAt(fromPos, v1 / POW_10[shift] + (v2 % POW_10[shift]) * POW_10[cShift]);
        }
        dec.setBuffValAt(fromPos, dec.getBuffValAt(fromPos) / POW_10[shift]);
    }

    /**
     * Return bounds of decimal digits in the number.
     *
     * @param from decimal value to check.
     * @return array[0] = start, array[1] = end.
     */
    static int[] digitsBounds(DecimalStructure from) {
        int startResult, endResult;

        int start, stop, i;
        int bufBegPos = 0;
        final int endPos = roundUp(from.getIntegers()) + roundUp(from.getFractions());
        int bufEndPos = endPos - 1;

        // find non-zero digit from number begining
        while (bufBegPos < endPos && from.getBuffValAt(bufBegPos) == 0) {
            bufBegPos++;
        }

        if (bufBegPos >= endPos) {
            // it is zero
            startResult = 0;
            endResult = 0;
            return new int[] {startResult, endResult};
        }

        // find non-zero decimal digit from number beginning
        if (bufBegPos == 0 && from.getIntegers() != 0) {
            start = DIG_PER_DEC1 - (i = ((from.getIntegers() - 1) % DIG_PER_DEC1 + 1));
            i--;
        } else {
            i = DIG_PER_DEC1 - 1;
            start = (bufBegPos) * DIG_PER_DEC1;
        }
        if (bufBegPos < endPos) {
            start += countLeadingZeros(i, from.getBuffValAt(bufBegPos));
        }

        // index of first decimal digit (from 0)
        startResult = start;

        // find non-zero digit at the end
        while (bufEndPos > bufBegPos && from.getBuffValAt(bufEndPos) == 0) {
            bufEndPos--;
        }
        // find non-zero decimal digit from the end
        if (bufEndPos == endPos - 1 && from.getFractions() != 0) {
            stop = bufEndPos * DIG_PER_DEC1 +
                (i = ((from.getFractions() - 1) % DIG_PER_DEC1 + 1));
            i = DIG_PER_DEC1 - i + 1;
        } else {
            stop = (bufEndPos + 1) * DIG_PER_DEC1;
            i = 1;
        }
        stop -= countTrailingZeros(i, from.getBuffValAt(bufEndPos));

        // index of position after last decimal digit (from 0)
        endResult = stop;
        return new int[] {startResult, endResult};
    }

    private static int add2(int[] array, int index, int b, int carry) {
        long sum = (array[index] & DecimalTypeBase.LONG_MASK) + (b & DecimalTypeBase.LONG_MASK) + (carry
            & DecimalTypeBase.LONG_MASK);

        carry = sum >= DecimalTypeBase.DIG_BASE ? 1 : 0;
        sum -= (carry == 1 ? DecimalTypeBase.DIG_BASE : 0);
        boolean x = sum >= DecimalTypeBase.DIG_BASE;
        sum = x ? sum - DecimalTypeBase.DIG_BASE : sum;
        carry = x ? carry + 1 : carry;
        array[index] = (int) sum;
        return carry;
    }

    private static int add2(DecimalStructure d, int index, int b, int carry) {
        long sum = (d.getBuffValAt(index) & DecimalTypeBase.LONG_MASK) + (b & DecimalTypeBase.LONG_MASK) + (carry
            & DecimalTypeBase.LONG_MASK);

        carry = sum >= DecimalTypeBase.DIG_BASE ? 1 : 0;
        sum -= (carry == 1 ? DecimalTypeBase.DIG_BASE : 0);
        boolean x = sum >= DecimalTypeBase.DIG_BASE;
        sum = x ? sum - DecimalTypeBase.DIG_BASE : sum;
        carry = x ? carry + 1 : carry;
        d.setBuffValAt(index, (int) sum);
        return carry;
    }

    private static int add(int[] array, int index, int b, int carry) {
        long sum = (array[index] & DecimalTypeBase.LONG_MASK) + (b & DecimalTypeBase.LONG_MASK) + (carry
            & DecimalTypeBase.LONG_MASK);
        carry = sum >= DecimalTypeBase.DIG_BASE ? 1 : 0;
        sum -= (carry == 1 ? DecimalTypeBase.DIG_BASE : 0);
        array[index] = (int) sum;
        return carry;
    }

    private static int add(DecimalStructure d, int index, int b, int carry) {
        long sum = (d.getBuffValAt(index) & DecimalTypeBase.LONG_MASK) + (b & DecimalTypeBase.LONG_MASK) + (carry
            & DecimalTypeBase.LONG_MASK);
        carry = sum >= DecimalTypeBase.DIG_BASE ? 1 : 0;
        sum -= (carry == 1 ? DecimalTypeBase.DIG_BASE : 0);
        d.setBuffValAt(index, (int) sum);
        return carry;
    }

    private static int sub(int[] array, int index, int a, int b, int carry) {
        long diff =
            (a & DecimalTypeBase.LONG_MASK) - (b & DecimalTypeBase.LONG_MASK) - (carry & DecimalTypeBase.LONG_MASK);
        if (diff < 0) {
            carry = 1;
            diff += DecimalTypeBase.DIG_BASE;
        } else {
            carry = 0;
        }

        array[index] = (int) diff;
        return carry;
    }

    private static int sub(DecimalStructure d, int index, int a, int b, int carry) {
        long diff =
            (a & DecimalTypeBase.LONG_MASK) - (b & DecimalTypeBase.LONG_MASK) - (carry & DecimalTypeBase.LONG_MASK);
        if (diff < 0) {
            carry = 1;
            diff += DecimalTypeBase.DIG_BASE;
        } else {
            carry = 0;
        }

        d.setBuffValAt(index, (int) diff);
        return carry;
    }

    private static int sub2(int[] array, int index, int a, int b, int carry) {
        long diff =
            (a & DecimalTypeBase.LONG_MASK) - (b & DecimalTypeBase.LONG_MASK) - (carry & DecimalTypeBase.LONG_MASK);
        if (diff < 0) {
            carry = 1;
            diff += DecimalTypeBase.DIG_BASE;
        } else {
            carry = 0;
        }
        if (diff < 0) {
            diff += DecimalTypeBase.DIG_BASE;
            carry++;
        }

        array[index] = (int) diff;
        return carry;
    }

    static int countTrailingZeros(int i, int val) {
        int ret = 0;
        switch (i) {
        case 0:
            if ((val % 1) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 1:
            if ((val % 10) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 2:
            if ((val % 100) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 3:
            if ((val % 1000) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 4:
            if ((val % 10000) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 5:
            if ((val % 100000) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 6:
            if ((val % 1000000) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 7:
            if ((val % 10000000) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 8:
            if ((val % 100000000) != 0) {
                break;
            }
            ++ret;  // Fall through.
        case 9:
            if ((val % 1000000000) != 0) {
                break;
            }
            ++ret;  // Fall through.
        default:
        }
        return ret;
    }

    static int countLeadingZeros(int i, int val) {
        int ret = 0;
        switch (i) {
        case 9:
            if (val >= 1000000000) {
                break;
            }
            ++ret;  // Fall through.
        case 8:
            if (val >= 100000000) {
                break;
            }
            ++ret;  // Fall through.
        case 7:
            if (val >= 10000000) {
                break;
            }
            ++ret;  // Fall through.
        case 6:
            if (val >= 1000000) {
                break;
            }
            ++ret;  // Fall through.
        case 5:
            if (val >= 100000) {
                break;
            }
            ++ret;  // Fall through.
        case 4:
            if (val >= 10000) {
                break;
            }
            ++ret;  // Fall through.
        case 3:
            if (val >= 1000) {
                break;
            }
            ++ret;  // Fall through.
        case 2:
            if (val >= 100) {
                break;
            }
            ++ret;  // Fall through.
        case 1:
            if (val >= 10) {
                break;
            }
            ++ret;  // Fall through.
        case 0:
            if (val >= 1) {
                break;
            }
            ++ret;  // Fall through.
        default:
        }
        return ret;
    }

}