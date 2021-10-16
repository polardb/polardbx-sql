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
        if (from1 == to) {

            from1 = to.copy();
        }
        if (from2 == to) {

            from2 = to.copy();
        }

        to.reset();

        to.setDerivedFractions(Math.max(from1.getDerivedFractions(), from2.getDerivedFractions()));
        if (from1.isNeg() == from2.isNeg()) {
            return doAdd(from1, from2, to);
        } else {
            return doSub(from1, from2, to)[1];
        }
    }

    public static int sub(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        if (from1 == to) {

            from1 = to.copy();
        }
        if (from2 == to) {

            from2 = to.copy();
        }

        to.reset();

        to.setDerivedFractions(Math.max(from1.getDerivedFractions(), from2.getDerivedFractions()));
        if (from1.isNeg() == from2.isNeg()) {
            return doSub(from1, from2, to)[1];
        } else {
            return doAdd(from1, from2, to);
        }
    }

    public static int mul(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        if (from1 == to) {

            from1 = to.copy();
        }
        if (from2 == to) {

            from2 = to.copy();
        }

        to.reset();

        to.setDerivedFractions(
            Math.min(from1.getDerivedFractions() + from2.getDerivedFractions(), DecimalTypeBase.MAX_DECIMAL_SCALE));
        return doMul(from1, from2, to);
    }


    public static int div(DecimalStructure from1, DecimalStructure from2, DecimalStructure to, int scaleIncr) {
        if (from1 == to) {

            from1 = to.copy();
        }
        if (from2 == to) {

            from2 = to.copy();
        }

        to.reset();

        to.setDerivedFractions(Math.min(from1.getDerivedFractions() + scaleIncr, DecimalTypeBase.MAX_DECIMAL_SCALE));
        return doDiv(from1, from2, to, null, scaleIncr);
    }

    public static int mod(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        if (from1 == to) {

            from1 = to.copy();
        }
        if (from2 == to) {

            from2 = to.copy();
        }

        to.reset();

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

            from = to.copy();
        }

        to.reset();
        return doDecimalRound(from, to, scale, mode);
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


        int carry = 0;
        while (bufPos1 > bufEndPos2) {

            int sum = dec1.getBuffValAt(--bufPos1) + dec2.getBuffValAt(--bufPos2) + carry;
            if (sum >= DecimalTypeBase.DIG_BASE) {
                carry = 1;
                sum -= DecimalTypeBase.DIG_BASE;
            } else {
                carry = 0;
            }
            to.setBuffValAt(--toIndex, sum);
        }


        bufEndPos = 0;
        if (from1IntWords > from2IntWords) {
            bufPos1 = from1IntWords - from2IntWords;
            dec1 = from1;
        } else {
            bufPos1 = from2IntWords - from1IntWords;
            dec1 = from2;
        }
        while (bufPos1 > bufEndPos) {

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
        to.setFractions(from1.getFractions() + from2.getFractions());
        if (to.getFractions() > DecimalTypeBase.NOT_FIXED_DEC) {
            to.setFractions(DecimalTypeBase.NOT_FIXED_DEC);
        }
        to.setIntegers(toIntWords * DIG_PER_DEC1);
        if (error == DecimalTypeBase.E_DEC_OVERFLOW) {
            return error;
        }


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

                carry = add(to, toPos, 0, carry);
            }
        }


        if (to.isNeg()) {
            int idx = 0;
            int end = toIntWords + toFracWords;
            for (; ; ) {
                if (to.getBuffValAt(idx) != 0) {
                    break;
                }
                idx++;

                if (idx == end) {
                    to.setBuff(new int[9]);
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


    protected static int[] doSub(DecimalStructure from1, DecimalStructure from2, DecimalStructure to) {
        int error;
        int from1IntWords = DecimalTypeBase.roundUp(from1.getIntegers());
        int from1FracWords = DecimalTypeBase.roundUp(from1.getFractions());
        int from2IntWords = DecimalTypeBase.roundUp(from2.getIntegers());
        int from2FracWords = DecimalTypeBase.roundUp(from2.getFractions());
        int toFracWords = Math.max(from1FracWords, from2FracWords);


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
            if ((carry > 0) == from1.isNeg()) {
                return new int[] {1, 1};
            }
            return new int[] {-1, 1};
        }

        to.setNeg(from1.isNeg());

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

        while (bufPos2 > start2) {
            carry = sub(to, --bufPos0, from1.getBuffValAt(--bufPos1), from2.getBuffValAt(--bufPos2), carry);
        }

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

    protected static int doDiv(DecimalStructure from1, DecimalStructure from2, DecimalStructure to,
                               DecimalStructure mod, int scaleIncr) {
        int error = DecimalTypeBase.E_DEC_OK;

        int from1Fractions = DecimalTypeBase.roundUp(from1.getFractions()) * DIG_PER_DEC1;
        int from1Precision = from1.getIntegers() + from1Fractions;
        int from2Fractions = DecimalTypeBase.roundUp(from2.getFractions()) * DIG_PER_DEC1;
        int from2Precision = from2.getIntegers() + from2Fractions;

        if (mod != null) {
            to = mod;
        }

        int i = ((from2Precision - 1) % DIG_PER_DEC1) + 1;
        int bufPos2 = 0;
        while (from2Precision > 0 && from2.getBuffValAt(bufPos2) == 0) {
            from2Precision -= i;
            i = DIG_PER_DEC1;
            bufPos2++;
        }
        if (from2Precision <= 0) {

            return DecimalTypeBase.E_DEC_DIV_ZERO;
        }

        from2Precision -= countLeadingZeros((from2Precision - 1) % DIG_PER_DEC1, from2.getBuffValAt(bufPos2));

        i = ((from1Precision - 1) % DIG_PER_DEC1) + 1;
        int bufPos1 = 0;
        while (from1Precision > 0 && from1.getBuffValAt(bufPos1) == 0) {
            from1Precision -= i;
            i = DIG_PER_DEC1;
            bufPos1++;
        }
        if (from1Precision <= 0) {

            to.toZero();
            return DecimalTypeBase.E_DEC_OK;
        }
        from1Precision -= countLeadingZeros((from1Precision - 1) % DIG_PER_DEC1, from1.getBuffValAt(bufPos1));

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

            to.setNeg(from1.isNeg());
            to.setFractions(Math.max(from1.getFractions(), from2.getFractions()));
        } else {

            toFracWords = DecimalTypeBase.roundUp(from1Fractions + from2Fractions + scaleIncr);

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

        while (from2.getBuffValAt(stop2) == 0 && stop2 >= start2) {
            stop2--;
        }
        int len2 = stop2 - start2;
        stop2++;

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

        long guess;
        for (; toBufPos < stopTo; toBufPos++) {

            if (dcarry == 0 && tmp1[start1] < from2.getBuffValAt(start2)) {
                guess = 0;
            } else {

                long x = (long) (tmp1[start1]) + (long) (dcarry) * DecimalTypeBase.DIG_BASE;
                long y = (long) (tmp1[start1 + 1]);
                guess = (normFactor * x + normFactor * y / DecimalTypeBase.DIG_BASE) / (long) (norm2);
                if (guess >= DecimalTypeBase.DIG_BASE) {
                    guess = DecimalTypeBase.DIG_BASE - 1;
                }

                if (len2 > 0) {

                    if ((from2.getBuffValAt(start2 + 1) * guess
                        > (x - guess * (from2.getBuffValAt(start2))) * DecimalTypeBase.DIG_BASE + y)) {
                        guess--;
                    }
                    if ((from2.getBuffValAt(start2 + 1) * guess
                        > (x - guess * (from2.getBuffValAt(start2))) * DecimalTypeBase.DIG_BASE + y)) {
                        guess--;
                    }
                }

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

                if (carry > 0) {

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

            if (dcarry != 0) {
                start1--;
                tmp1[start1] = dcarry;
            }
            toBufPos = 0;

            toIntegers = from1Precision - from1Fractions - start1 * DIG_PER_DEC1;
            if (toIntegers < 0) {

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

    protected static int doCompare(DecimalStructure from1, DecimalStructure from2) {
        int from1IntWords = DecimalTypeBase.roundUp(from1.getIntegers());
        int from1FracWords = DecimalTypeBase.roundUp(from1.getFractions());
        int from2IntWords = DecimalTypeBase.roundUp(from2.getIntegers());
        int from2FracWords = DecimalTypeBase.roundUp(from2.getFractions());

        int bufPos1 = 0, bufPos2 = 0;
        int bufEndPos1 = from1IntWords, bufEndPos2 = from2IntWords;

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

            return 1;
        }
        return -1;
    }

    protected static int doShift(DecimalStructure dec, int shift) {

        int beg = 0;

        int end = 0;

        int point = DecimalTypeBase.roundUp(dec.getIntegers()) * DIG_PER_DEC1;

        int newPoint = point + shift;

        int integers, fractions;

        int newLen, newFracLen;

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

                return E_DEC_OVERFLOW;
            }

            err = E_DEC_TRUNCATED;
            newFracLen -= lack;
            diff = fractions - (newFracLen * DIG_PER_DEC1);

            doDecimalRound(dec, dec, end - point - diff, HALF_UP);
            end -= diff;
            fractions = newFracLen * DIG_PER_DEC1;

            if (end <= beg) {

                dec.toZero();
                return E_DEC_TRUNCATED;
            }
        }

        if (shift % DIG_PER_DEC1 != 0) {
            int lMiniShift, rMiniShift, miniShift;
            boolean doLeft;

            if (shift > 0) {
                lMiniShift = shift % DIG_PER_DEC1;
                rMiniShift = DIG_PER_DEC1 - lMiniShift;

                doLeft = lMiniShift <= beg;
            } else {
                rMiniShift = (-shift) % DIG_PER_DEC1;
                lMiniShift = DIG_PER_DEC1 - rMiniShift;

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

            if ((shift += miniShift) == 0 && (newPoint - integers) < DIG_PER_DEC1) {
                dec.setIntegers(integers);
                dec.setFractions(fractions, true);

                return err;
            }
            beg += miniShift;
            end += miniShift;
        }

        if ((newFront = (newPoint - integers)) >= DIG_PER_DEC1 ||
            newFront < 0) {

            int dShift;
            int toPos;
            int barierPos;
            if (newFront > 0) {

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

        beg = roundUp(beg + 1) - 1;
        end = roundUp(end) - 1;

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

                if (frac0 + intg0 >= len) {
                    frac0--;
                    scale = frac0 * DIG_PER_DEC1;
                    error = E_DEC_TRUNCATED;
                }
                for (buf1Pos = intg0 + Math.max(frac0, 0); buf1Pos > 0; buf1Pos--) {

                    if (buf1Pos < len) {
                        to.setBuffValAt(buf1Pos, to.getBuffValAt(buf1Pos - 1));
                    } else {
                        error = E_DEC_OVERFLOW;
                    }

                }
                to.setBuffValAt(buf1Pos, 1);

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

    static int[] digitsBounds(DecimalStructure from) {
        int startResult, endResult;

        int start, stop, i;
        int bufBegPos = 0;
        final int endPos = roundUp(from.getIntegers()) + roundUp(from.getFractions());
        int bufEndPos = endPos - 1;

        while (bufBegPos < endPos && from.getBuffValAt(bufBegPos) == 0) {
            bufBegPos++;
        }

        if (bufBegPos >= endPos) {

            startResult = 0;
            endResult = 0;
            return new int[] {startResult, endResult};
        }

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

        startResult = start;

        while (bufEndPos > bufBegPos && from.getBuffValAt(bufEndPos) == 0) {
            bufEndPos--;
        }

        if (bufEndPos == endPos - 1 && from.getFractions() != 0) {
            stop = bufEndPos * DIG_PER_DEC1 +
                (i = ((from.getFractions() - 1) % DIG_PER_DEC1 + 1));
            i = DIG_PER_DEC1 - i + 1;
        } else {
            stop = (bufEndPos + 1) * DIG_PER_DEC1;
            i = 1;
        }
        stop -= countTrailingZeros(i, from.getBuffValAt(bufEndPos));

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
            ++ret;
        case 1:
            if ((val % 10) != 0) {
                break;
            }
            ++ret;
        case 2:
            if ((val % 100) != 0) {
                break;
            }
            ++ret;
        case 3:
            if ((val % 1000) != 0) {
                break;
            }
            ++ret;
        case 4:
            if ((val % 10000) != 0) {
                break;
            }
            ++ret;
        case 5:
            if ((val % 100000) != 0) {
                break;
            }
            ++ret;
        case 6:
            if ((val % 1000000) != 0) {
                break;
            }
            ++ret;
        case 7:
            if ((val % 10000000) != 0) {
                break;
            }
            ++ret;
        case 8:
            if ((val % 100000000) != 0) {
                break;
            }
            ++ret;
        case 9:
            if ((val % 1000000000) != 0) {
                break;
            }
            ++ret;
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
            ++ret;
        case 8:
            if (val >= 100000000) {
                break;
            }
            ++ret;
        case 7:
            if (val >= 10000000) {
                break;
            }
            ++ret;
        case 6:
            if (val >= 1000000) {
                break;
            }
            ++ret;
        case 5:
            if (val >= 100000) {
                break;
            }
            ++ret;
        case 4:
            if (val >= 10000) {
                break;
            }
            ++ret;
        case 3:
            if (val >= 1000) {
                break;
            }
            ++ret;
        case 2:
            if (val >= 100) {
                break;
            }
            ++ret;
        case 1:
            if (val >= 10) {
                break;
            }
            ++ret;
        case 0:
            if (val >= 1) {
                break;
            }
            ++ret;
        default:
        }
        return ret;
    }

}