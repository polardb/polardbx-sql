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

package com.alibaba.polardbx.common;

/**
 * @author yaozhili
 * <p>
 * This hash aims to hash an array containing LONG elements. and has the following properties:<br/>
 * 1. Order-invariant: these two arrays lead to the same hash code: [1, 2, 3] and [3, 2, 1]<br/>
 * 2. Revisable: use add(x) to calculate the hash code when adding an element into the array,
 * and use remove(x) to calculate the hash code when removing an element.
 * <p>
 * Initial hash value is 0. When two DynamicHash are added, remember to remove two 0 from the result.
 * Similarly, When n DynamicHash are added, remember to remove n 0 from the result.
 * <p>
 * <p>
 * See {@link RevisableOrderInvariantHashTest} for usage examples.
 */
public class RevisableOrderInvariantHash implements IOrderInvariantHash {
    private static final long p = 3860031L;
    private static final long q = 2779L;
    private static final long r = 2L;
    private static final long m = 1L << 31;
    private static final long m_1 = m - 1;
    private final ModularInverseSolver modularInverseSolver = new ModularInverseSolver();

    /**
     * Initial value is 0. When two DynamicHash are added, remember to remove one 0 from the result.
     * Similarly, When n DynamicHash are added, remember to remove (n-1) 0 from the result.
     */
    private Long result = 0L;

    public static long mod(long x) {
        // Calculate x mod 2^31 equals getting the low 30-bit of x.
        // For negative x, the result is positive.
        return x & m_1;
    }

    @Override
    public RevisableOrderInvariantHash add(long x) {
        // We omit some mod operation and let some intermediate results exceed (2^31 - 1),
        // which is ok if they do not exceed (2^63 - 1).
        result = mod(p + q * mod((result + mod(x))) + r * mod(result * mod(x)));
        return this;
    }

    /**
     * May cause overflow.
     */
    public RevisableOrderInvariantHash addNoMod(long x) {
        result = p + q * (result + x) + r * result * x;
        return this;
    }

    public RevisableOrderInvariantHash remove(long x) {
        long a = mod(result - mod(p + q * mod(x)));
        long b = mod(q + r * mod(x));
        if (a % b == 0) {
            result = a / b;
        } else {
            // Find modular inverse.
            long inverse = modularInverseSolver.solve(b, m);
            result = mod(a * inverse);
        }
        return this;
    }

    @Override
    public Long getResult() {
        return result;
    }

    public RevisableOrderInvariantHash reset() {
        result = 0L;
        return this;
    }

    public RevisableOrderInvariantHash reset(long value) {
        result = value;
        return this;
    }

    public static class ModularInverseSolver {
        /**
         * i and j are used for calculate modular inverse.
         */
        private long i;
        private long j;

        public long solve(long i0, long j0) {
            this.i = i0;
            this.j = j0;
            extendedEuclidean();
            return this.i;
        }

        private void extendedEuclidean() {
            if (0 == j) {
                i = 1;
            } else {
                // i, j are input of this level.
                long i0 = i, j0 = j;
                i = j0;
                j = i0 % j0;
                extendedEuclidean();
                // i, j are output of next level.
                long i1 = i, j1 = j;
                // i, j are output of this level.
                i = j1;
                j = i1 - (i0 / j0) * j1;
            }
        }
    }
}
