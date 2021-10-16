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

import com.google.common.base.Preconditions;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.*;

public class DecimalBounds {
    private static final int[] FRAC_MAX =
        {900000000, 990000000, 999000000, 999900000, 999990000, 999999000, 999999900, 999999990};

    private static final DecimalStructure[][] DECIMAL_MAX_VALUE;

    private static final DecimalStructure[][] DECIMAL_MIN_VALUE;

    static {
        DECIMAL_MAX_VALUE = new DecimalStructure[MAX_DECIMAL_PRECISION + 1][MAX_DECIMAL_SCALE + 1];
        DECIMAL_MIN_VALUE = new DecimalStructure[MAX_DECIMAL_PRECISION + 1][MAX_DECIMAL_SCALE + 1];
        for(int precision = 1; precision <= MAX_DECIMAL_PRECISION; precision++) {
            for (int scale = 0; scale <= Math.min(precision, MAX_DECIMAL_SCALE); scale++) {
                DECIMAL_MAX_VALUE[precision][scale] = boundValue(precision, scale, false);
                DECIMAL_MIN_VALUE[precision][scale] = boundValue(precision, scale, true);
            }
        }
    }

    public static DecimalStructure maxValue(int precision, int scale) {
        Preconditions.checkArgument(precision > 0 && precision <= MAX_DECIMAL_PRECISION);
        Preconditions.checkArgument(scale >= 0 && scale <= MAX_DECIMAL_SCALE && scale <= precision);
        return DECIMAL_MAX_VALUE[precision][scale];
    }

    public static DecimalStructure minValue(int precision, int scale) {
        Preconditions.checkArgument(precision > 0 && precision <= MAX_DECIMAL_PRECISION);
        Preconditions.checkArgument(scale >= 0 && scale <= MAX_DECIMAL_SCALE && scale <= precision);
        return DECIMAL_MIN_VALUE[precision][scale];
    }

    public static DecimalStructure boundValue(int precision, int scale, boolean isNeg) {
        Preconditions.checkArgument(precision != 0);
        Preconditions.checkArgument(precision >= scale);
        DecimalStructure to = new DecimalStructure();

        to.setNeg(isNeg);

        int intPart = precision - scale;
        to.setIntegers(intPart);
        int bufPos = 0;
        if (intPart != 0) {
            int firstDigits = intPart % DIG_PER_DEC1;
            if (firstDigits != 0) {

                to.setBuffValAt(bufPos, POW_10[firstDigits] - 1);
                bufPos++;
            }
            intPart = intPart / DIG_PER_DEC1;
            for (; intPart != 0; intPart--) {
                to.setBuffValAt(bufPos, DIG_BASE - 1);
                bufPos++;
            }
        }

        to.setFractions(scale, true);
        if (scale != 0) {
            int lastDigits = scale % DIG_PER_DEC1;
            scale = scale / DIG_PER_DEC1;
            for (; scale != 0; scale--) {
                to.setBuffValAt(bufPos, DIG_BASE - 1);
                bufPos++;
            }
            if (lastDigits != 0) {
                to.setBuffValAt(bufPos, FRAC_MAX[lastDigits - 1]);
            }
        }

        return to;
    }
}
