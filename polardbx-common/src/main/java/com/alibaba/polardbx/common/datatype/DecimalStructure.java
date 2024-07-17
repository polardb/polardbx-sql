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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.Serializable;

import static com.alibaba.polardbx.common.datatype.DecimalRoundMod.HALF_UP;

public class DecimalStructure extends DecimalTypeBase implements Serializable {

    private Slice decimalMemorySegment;

    public static Slice allocateDecimalSlice() {
        return Slices.allocate(DECIMAL_MEMORY_SIZE);
    }

    public DecimalStructure() {

        decimalMemorySegment = allocateDecimalSlice();
    }

    public DecimalStructure(Slice decimalMemorySegment) {

        decimalMemorySegment.getByte(DECIMAL_MEMORY_SIZE - 1);

        this.decimalMemorySegment = decimalMemorySegment;
    }

    public void toZero() {
        setIntegers(1);
        setFractions(getDerivedFractions());
        setNeg(false);

        int pos = 0;
        int end = roundUp(getIntegers()) + roundUp(getFractions());
        while (pos < end) {
            setBuffValAt(pos++, 0);
        }
    }

    public boolean isZero() {
        int pos = 0;
        int end = roundUp(getIntegers()) + roundUp(getFractions());
        while (pos < end) {
            if (getBuffValAt(pos++) != 0) {
                return false;
            }
        }
        return true;
    }

    public int[] removeLeadingZeros() {
        int integers = this.getIntegers();
        int i = ((integers - 1) % DIG_PER_DEC1) + 1;
        int fromIndex = 0;
        while (integers > 0 && getBuffValAt(fromIndex) == 0) {
            integers -= i;
            i = DIG_PER_DEC1;
            fromIndex++;
        }
        if (integers > 0) {
            integers -= FastDecimalUtils.countLeadingZeros((integers - 1) % DIG_PER_DEC1, getBuffValAt(fromIndex));
        } else {
            integers = 0;
        }
        return new int[] {fromIndex, integers};
    }

    public int[] removeTrailingZeros() {
        int fractions = this.getFractions();
        int i = ((fractions - 1) % DIG_PER_DEC1) + 1;
        int endIndex = roundUp(this.getIntegers()) + roundUp(this.getFractions());
        while (fractions > 0 && getBuffValAt(endIndex - 1) == 0) {
            fractions -= i;
            i = DIG_PER_DEC1;
            endIndex--;
        }
        if (fractions > 0) {
            fractions -=
                FastDecimalUtils.countTrailingZeros(9 - ((fractions - 1) % DIG_PER_DEC1), getBuffValAt(endIndex - 1));
        } else {
            fractions = 0;
        }
        return new int[] {endIndex, fractions};
    }

    private int getLeadingNonZeroIndex() {
        int integers = this.getIntegers();
        int i = ((integers - 1) % DIG_PER_DEC1) + 1;
        int fromIndex = 0;
        while (integers > 0 && getBuffValAt(fromIndex) == 0) {
            integers -= i;
            i = DIG_PER_DEC1;
            fromIndex++;
        }
        return fromIndex;
    }

    private int getTrailingNonZeroIndex() {
        int fractions = this.getFractions();
        int i = ((fractions - 1) % DIG_PER_DEC1) + 1;
        int endIndex = roundUp(this.getIntegers()) + roundUp(this.getFractions());
        while (fractions > 0 && endIndex > 0 && getBuffValAt(endIndex - 1) == 0) {
            fractions -= i;
            i = DIG_PER_DEC1;
            endIndex--;
        }
        return endIndex;
    }

    @Override
    public int hashCode() {
        int fromIndex = getLeadingNonZeroIndex();
        int endIndex = getTrailingNonZeroIndex();

        int result = 1;
        for (int i = fromIndex; i != endIndex; i++) {
            int element = getBuffValAt(i);
            result = 31 * result + element;
        }

        return isNeg() ? -result : result;
    }

    public DecimalStructure copy() {
        DecimalStructure res = new DecimalStructure();
        res.decimalMemorySegment.setBytes(0, this.decimalMemorySegment);
        return res;
    }

    public void copyTo(DecimalStructure to) {
        to.decimalMemorySegment.setBytes(0, this.decimalMemorySegment);
    }

    public void reset() {
        decimalMemorySegment.setBytes(0, BYTES_0);
    }

    public byte[] toBytes() {
        Pair<byte[], Integer> res = DecimalConverter.decimal2String(this, 0, this.getDerivedFractions(), (byte) 0);
        return res.getKey();
    }

    @Override
    public String toString() {
        return new String(toBytes());
    }

    public int getPrecision() {
        int intPart = removeLeadingZeros()[1];
        return getFractions() + intPart;
    }

    public int getIntegers() {
        return ((int) decimalMemorySegment.getByteUnchecked(INTEGERS_OFFSET)) & 0xFF;
    }

    public void setIntegers(int integers) {
        decimalMemorySegment.setByteUnchecked(INTEGERS_OFFSET, integers);
    }

    public int getFractions() {
        return ((int) decimalMemorySegment.getByteUnchecked(FRACTIONS_OFFSET)) & 0xFF;
    }

    public void setFractions(int fractions) {
        setFractions(fractions, false);
    }

    public void setFractions(int fractions, boolean alsoSetDerivedFractions) {
        decimalMemorySegment.setByteUnchecked(FRACTIONS_OFFSET, fractions);
        if (alsoSetDerivedFractions) {
            decimalMemorySegment.setByteUnchecked(DERIVED_FRACTIONS_OFFSET, fractions);
        }
    }

    public boolean isNeg() {
        return decimalMemorySegment.getByteUnchecked(IS_NEG_OFFSET) == NEGATIVE_FLAG;
    }

    public void setNeg(boolean neg) {
        decimalMemorySegment.setByteUnchecked(IS_NEG_OFFSET, neg ? NEGATIVE_FLAG : POSITIVE_FLAG);
    }

    public int getBuffValAt(int position) {
        return decimalMemorySegment.getIntUnchecked(BUFF_OFFSETS[position]);
    }

    public void setBuffValAt(int position, int value) {
        decimalMemorySegment.setIntUnchecked(BUFF_OFFSETS[position], value);
    }

    public void setBuff(int[] buff) {
        decimalMemorySegment.setIntArrayUnchecked(0, buff, 0, buff.length);
    }

    public int getDerivedFractions() {
        return ((int) decimalMemorySegment.getByteUnchecked(DERIVED_FRACTIONS_OFFSET)) & 0xFF;
    }

    public void setDerivedFractions(int derivedFractions) {
        decimalMemorySegment.setByteUnchecked(DERIVED_FRACTIONS_OFFSET, derivedFractions);
    }

    public int toDiv(DivStructure div) {
        int intPart = roundUp(getIntegers());
        int fracPart = roundUp(getFractions());
        if (intPart > 2) {

            div.setQuot(isNeg() ? DivStructure.MIN_QUOTIENT : DivStructure.MAX_QUOTIENT);
            div.setRem(0L);
            return E_DEC_OVERFLOW;
        }

        long quot, rem;
        if (intPart == 2) {
            quot = ((long) getBuffValAt(0)) * DIG_BASE + getBuffValAt(1);
        } else if (intPart == 1) {
            quot = getBuffValAt(0);
        } else {
            quot = 0L;
        }
        rem = fracPart != 0 ? getBuffValAt(intPart) : 0;

        if (isNeg()) {
            quot = -quot;
            rem = -rem;
        }

        div.setQuot(quot);
        div.setRem(rem);
        return E_DEC_OK;
    }

    public Slice getDecimalMemorySegment() {
        return decimalMemorySegment;
    }

    public void setLongWithScale(long longVal, int scale) {
        this.reset();
        // parse long & set scale.
        DecimalConverter.longToDecimal(longVal, this);
        // shift by scale value.
        FastDecimalUtils.shift(this, this, -scale);

        FastDecimalUtils.round(this, this, scale, HALF_UP);
    }
}
