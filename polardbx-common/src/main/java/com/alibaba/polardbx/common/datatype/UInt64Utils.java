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

public class UInt64Utils {
    public static final long FLIP_MASK = 0x8000000000000000L;
    public static final long UNSIGNED_MASK = 0x7FFFFFFFFFFFFFFFL;

    public static int compareUnsigned(long a, int b) {
        return compareUnsigned(a, b & 0xFFFFFFFFL);
    }

    public static int compareUnsigned(int a, long b) {
        return compareUnsigned(a & 0xFFFFFFFFL, b);
    }

    public static int compareUnsigned(long a, long b) {
        a ^= FLIP_MASK;
        b ^= FLIP_MASK;
        return (a < b) ? -1 : ((a > b) ? 1 : 0);
    }


    public static long divide(long dividend, long divisor) {
        if (divisor < 0) {

            if ((dividend ^ FLIP_MASK) < (divisor ^ FLIP_MASK)) {
                return 0;
            } else {
                return 1;
            }
        }


        if (dividend >= 0) {
            return dividend / divisor;
        }

        long quotient = ((dividend >>> 1) / divisor) << 1;
        long rem = dividend - quotient * divisor;
        int carry = (rem ^ FLIP_MASK) >= (divisor ^ FLIP_MASK) ? 1 : 0;
        return quotient + carry;
    }

    public static long remainder(long dividend, long divisor) {
        if (divisor < 0) {

            if ((dividend ^ FLIP_MASK) < (divisor ^ FLIP_MASK)) {
                return dividend;
            } else {
                return dividend - divisor;
            }
        }

        if (dividend >= 0) {
            return dividend % divisor;
        }

        long quotient = ((dividend >>> 1) / divisor) << 1;
        long rem = dividend - quotient * divisor;
        return rem - ((rem ^ FLIP_MASK) >= (divisor ^ FLIP_MASK) ? divisor : 0);
    }

    public static long round(long value, long tens) {

        long tmp = divide(value, tens) * tens;
        return compareUnsigned(value - tmp, tens >> 1) < 0
            ? tmp : tmp + tens;
    }
}