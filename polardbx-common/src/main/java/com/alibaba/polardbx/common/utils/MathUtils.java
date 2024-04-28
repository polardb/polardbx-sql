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

public class MathUtils {

    public static int ceilDiv(int x, int y) {
        return -Math.floorDiv(-x, y);
    }

    public static long ceilDiv(long x, long y) {
        return -Math.floorDiv(-x, y);
    }

    public static int ceilMod(int x, int y) {
        return x - ceilDiv(x, y) * y;
    }

    public static long ceilMod(long x, long y) {
        return x - ceilDiv(x, y) * y;
    }

    /**
     * Overflow iff both arguments have the opposite sign of the result
     */
    public static boolean longAddOverflow(long x, long y, long r) {
        return ((x ^ r) & (y ^ r)) < 0;
    }

    public static boolean longSubOverflow(long x, long y, long r) {
        return ((x ^ y) & (x ^ r)) < 0;
    }

    public static boolean longMultiplyOverflow(long x, long y, long r) {
        long ax = Math.abs(x);
        long ay = Math.abs(y);
        if (((ax | ay) >>> 31 != 0)) {
            // Some bits greater than 2^31 that might cause overflow
            // Check the result using the divide operator
            // and check for the special case of Long.MIN_VALUE * -1
            return ((y != 0) && (r / y != x)) ||
                (x == Long.MIN_VALUE && y == -1);
        }
        return false;
    }

    public static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }
}
