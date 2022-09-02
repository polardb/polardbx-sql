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

package com.alibaba.polardbx.qatest.dql.sharding.type.numeric;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBounds;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;

import java.util.Random;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.MAX_DECIMAL_PRECISION;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.MAX_DECIMAL_SCALE;

public class CastTestUtils {
    private static final DecimalStructure SIGNED_MIN = new DecimalStructure();
    private static final DecimalStructure SIGNED_MAX = new DecimalStructure();
    private static final DecimalStructure UNSIGNED_MAX = new DecimalStructure();

    private static final long INT_64_MAX = 0x7FFFFFFFFFFFFFFFL;
    private static final long INT_64_MIN = -0x7FFFFFFFFFFFFFFFL - 1;

    private static final int INT_32_MAX = 0x7FFFFFFF;
    private static final int INT_32_MIN = ~0x7FFFFFFF;

    private static final int INT_24_MAX = 0x007FFFFF;
    private static final int INT_24_MIN = ~0x007FFFFF;

    static {
        DecimalConverter.unsignedlongToDecimal(-1L, UNSIGNED_MAX);
        DecimalConverter.longToDecimal(INT_64_MIN, SIGNED_MIN);
        DecimalConverter.longToDecimal(INT_64_MAX, SIGNED_MAX);
    }

    private static final Random R = new Random();
    private static final String NUMBER_STR = "0123456789";

    /**
     * Generate random decimal value in specified max precision & max scale.
     *
     * @param maxPrecision maximum precision.
     * @param maxScale maximum scale.
     * @param isSigned TRUE if use '-'.
     * @return random decimal value in bytes.
     */
    public static byte[] randomDecimal(int maxPrecision, int maxScale, boolean isSigned) {
        int precision = R.nextInt(maxPrecision) + 1;
        int scale = R.nextInt(Math.min(precision, maxScale + 1));
        if (precision == scale) {
            scale--;
        }

        boolean isNeg = isSigned && R.nextInt() % 2 == 0;

        byte[] res = new byte[(scale == 0 ? precision : precision + 1) + (isNeg ? 1 : 0)];
        int i = 0;
        if (isNeg) {
            res[i++] = '-';
        }
        res[i++] = (byte) NUMBER_STR.charAt(R.nextInt(9) + 1);
        for (; i < precision - scale + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) NUMBER_STR.charAt(R.nextInt(10));
        }
        if (scale == 0) {
            return res;
        }
        res[i++] = '.';
        for (; i < precision + 1 + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) NUMBER_STR.charAt(R.nextInt(10));
        }
        return res;
    }

    public static byte[] randomDecimal(int maxPrecision, int maxScale) {
        return randomDecimal(maxPrecision, maxScale, true);
    }

    /**
     * Returns a pseudo-random number between min and max, inclusive.
     *
     * @param lowerBound Minimum value
     * @param upperBound Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     */
    public static long randomNumberIn(int lowerBound, int upperBound) {
        return R.nextInt((upperBound - lowerBound) + 1) + lowerBound;
    }

    public static long randomTinyint() {
        return randomNumberIn(Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    public static long randomUTinyint() {
        return randomNumberIn(0, Byte.MAX_VALUE - Byte.MIN_VALUE);
    }

    public static long randomSmallint() {
        return randomNumberIn(Short.MIN_VALUE, Short.MAX_VALUE);
    }

    public static long randomUSmallint() {
        return randomNumberIn(0, Short.MAX_VALUE - Short.MIN_VALUE);
    }

    public static long randomMediumInt() {
        return randomNumberIn(INT_24_MIN, INT_24_MAX);
    }

    public static long randomUMediumInt() {
        return randomNumberIn(0, INT_24_MAX - INT_24_MIN);
    }

    public static long randomInt() {
        while (true) {
            byte[] decBytes = randomDecimal(10, 0, true);
            String decStr = new String(decBytes);
            Decimal dec = Decimal.fromString(decStr);
            if (dec.compareTo(Decimal.MAX_INT32) <= 0 && dec.compareTo(Decimal.MIN_INT32) >= 0) {
                return dec.longValue();
            }
        }
    }

    public static long randomUInt() {
        while (true) {
            byte[] decBytes = randomDecimal(10, 0, false);
            String decStr = new String(decBytes);
            Decimal dec = Decimal.fromString(decStr);
            if (dec.compareTo(Decimal.MAX_UNSIGNED_INT32) <= 0 && dec.compareTo(Decimal.ZERO) >= 0) {
                return dec.longValue();
            }
        }
    }

    public static String randomBigint() {
        while (true) {
            byte[] decBytes = randomDecimal(19, 0, true);
            String decStr = new String(decBytes);
            Decimal dec = Decimal.fromString(decStr);
            if (dec.compareTo(Decimal.MAX_SIGNED) <= 0 && dec.compareTo(Decimal.MIN_SIGNED) >= 0) {
                return decStr;
            }
        }
    }

    public static String randomUBigint() {
        while (true) {
            byte[] decBytes = randomDecimal(20, 0, false);
            String decStr = new String(decBytes);
            Decimal dec = Decimal.fromString(decStr);
            if (dec.compareTo(Decimal.MAX_UNSIGNED) <= 0 && dec.compareTo(Decimal.ZERO) >= 0) {
                return decStr;
            }
        }
    }

    public static String randomLongStr() {
        while (true) {
            byte[] decBytes = randomDecimal(65, 0, true);
            String decStr = new String(decBytes);
            Decimal dec = Decimal.fromString(decStr);
            if (dec.compareTo(Decimal.MAX_UNSIGNED) >= 0 || dec.compareTo(Decimal.MIN_SIGNED) <= 0) {
                return decStr;
            }
        }
    }

    public static String randomStr() {
        return randomValidDecimal(MAX_DECIMAL_PRECISION, MAX_DECIMAL_SCALE);
    }

    public static String randomValidDecimal(int maxPrecision, int maxScale) {
        Decimal min = new Decimal(DecimalBounds.minValue(maxPrecision, maxScale));
        Decimal max = new Decimal(DecimalBounds.maxValue(maxPrecision, maxScale));
        while (true) {
            byte[] decBytes = randomDecimal(maxPrecision, maxScale, true);
            String decStr = new String(decBytes);
            Decimal dec = Decimal.fromString(decStr);
            if (dec.compareTo(min) >= 0 && dec.compareTo(max) <= 0) {
                return decStr;
            }
        }
    }

    public static String randomDouble() {
        while (true) {
            String dec = randomValidDecimal(30, 5);
            if (dec.contains(".")) {
                return dec;
            }
        }
    }

    public static String randomFloat() {
        while (true) {
            String dec = randomValidDecimal(15, 3);
            if (dec.contains(".")) {
                return dec;
            }
        }
    }

    public static boolean useNull() {
        return R.nextInt() % 5 == 0;
    }

    public static boolean useNatural() {
        return R.nextInt() % 3 == 0;
    }
}
