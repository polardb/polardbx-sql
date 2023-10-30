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

import io.airlift.slice.Slice;

/**
 * Decimal Box represent the value in format of:
 * sum + { intVal2 * 10 ^ (9 * 1) + intVal1 * 10 ^ (9 * 0) + fracVal * 10 ^ (9 * -1)}
 */
public class DecimalBox {
    private DecimalStructure sum;
    private long intVal1;
    private long intVal2;
    private long fracVal;
    private int carry;

    private boolean isSumZero;

    public DecimalBox() {
        sum = new DecimalStructure();
        intVal1 = 0;
        intVal2 = 0;
        fracVal = 0;
        carry = 0;
        isSumZero = true;
    }

    public void add(int a1, int a2, int b) {
        fracVal += b;
        if (fracVal < 1000_000_000) {
            carry = 0;
        } else {
            carry = 1;
            fracVal -= 1000_000_000;
        }

        intVal1 = intVal1 + a1 + carry;
        if (intVal1 < 1000_000_000) {
            carry = 0;
        } else {
            carry = 1;
            intVal1 -= 1000_000_000;
        }

        intVal2 = intVal2 + a2 + carry;

        // handle overflow
        if (intVal2 >= 1000_000_000) {
            addToSum(intVal1, intVal2, fracVal);

            intVal1 = 0;
            intVal2 = 0;
            fracVal = 0;
            carry = 0;
        }
    }

    public Decimal getDecimalSum() {
        if (intVal1 != 0 || intVal2 != 0 || fracVal != 0) {
            addToSum(intVal1, intVal2, fracVal);
            intVal1 = 0;
            intVal2 = 0;
            fracVal = 0;
            carry = 0;
        }
        return new Decimal(sum.copy());
    }

    private void addToSum(long a1, long a2, long b) {
        if (a2 == 0) {
            if (isSumZero) {
                doAddToSum1(a1, b);
            } else {
                doAddToSum3(a1, b);
            }
        } else {
            if (isSumZero) {
                doAddToSum2(a1, a2, b);
            } else {
                doAddToSum4(a1, a2, b);
            }
        }
        isSumZero = false;
    }

    private void doAddToSum1(long a1, long b) {
        int fractions = countFractions(b);
        Slice raw = sum.getDecimalMemorySegment();

        raw.setInt(0, (int) a1);
        raw.setInt(4, (int) b);
        raw.setByte(DecimalTypeBase.INTEGERS_OFFSET, 9);
        raw.setByte(DecimalTypeBase.FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.DERIVED_FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.IS_NEG_OFFSET, 0);
    }

    private void doAddToSum2(long a1, long a2, long b) {
        int fractions = countFractions(b);
        Slice raw = sum.getDecimalMemorySegment();

        if (a2 >= 1000_000_000) {
            raw.setInt(0, 1);
            raw.setInt(4, (int) a2 - 1000_000_000);
            raw.setInt(8, (int) a1);
            raw.setInt(12, (int) b);
            raw.setByte(DecimalTypeBase.INTEGERS_OFFSET, 19);
        } else {
            raw.setInt(0, (int) a2);
            raw.setInt(4, (int) a1);
            raw.setInt(8, (int) b);
            raw.setByte(DecimalTypeBase.INTEGERS_OFFSET, 18);
        }
        raw.setByte(DecimalTypeBase.FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.DERIVED_FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.IS_NEG_OFFSET, 0);
    }

    private void doAddToSum3(long a1, long b) {
        int fractions = countFractions(b);

        DecimalStructure tmp = new DecimalStructure();
        Slice raw = tmp.getDecimalMemorySegment();

        raw.setInt(0, (int) a1);
        raw.setInt(4, (int) b);
        raw.setByte(DecimalTypeBase.INTEGERS_OFFSET, 9);
        raw.setByte(DecimalTypeBase.FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.DERIVED_FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.IS_NEG_OFFSET, 0);

        FastDecimalUtils.add(sum, tmp, sum);
    }

    private void doAddToSum4(long a1, long a2, long b) {
        int fractions = countFractions(b);

        DecimalStructure tmp = new DecimalStructure();
        Slice raw = tmp.getDecimalMemorySegment();

        if (a2 >= 1000_000_000) {
            raw.setInt(0, 1);
            raw.setInt(4, (int) a2 - 1000_000_000);
            raw.setInt(8, (int) a1);
            raw.setInt(12, (int) b);
            raw.setByte(DecimalTypeBase.INTEGERS_OFFSET, 19);
        } else {
            raw.setInt(0, (int) a2);
            raw.setInt(4, (int) a1);
            raw.setInt(8, (int) b);
            raw.setByte(DecimalTypeBase.INTEGERS_OFFSET, 18);
        }
        raw.setByte(DecimalTypeBase.FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.DERIVED_FRACTIONS_OFFSET, fractions);
        raw.setByte(DecimalTypeBase.IS_NEG_OFFSET, 0);

        FastDecimalUtils.add(sum, tmp, sum);
    }

    private int countFractions(long b) {
        if (b == 0) {
            return 0;
        }
        if (b % 10_000_000 == 0) {
            return 2;
        }
        if (b % 100_000 == 0) {
            return 4;
        }
        if (b % 1000 == 0) {
            return 6;
        }
        return 9;
    }
}
