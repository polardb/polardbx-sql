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

package com.alibaba.polardbx.common.utils.bloomfilter;

public class BloomFilterUtil {


    /*
      Cheat sheet:
         m: total bits
         n: expected insertions
         b: m/n, bits per insertion
         p: expected false positive probability

         1) Optimal k = b * ln2
         2) p = (1 - e ^ (-kn/m))^k
         3) For optimal k: p = 2 ^ (-k) ~= 0.6185^b
         4) For optimal k: m = -nlnp / ((ln2) ^ 2)
     */

    /**
     * Computes the optimal k (number of hashes per element inserted in Bloom
     * filter), given the expected insertions and total number of bits in the
     * Bloom filter.
     * <p>
     * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for
     * the formula.
     *
     * @param n expected insertions (must be positive)
     * @param m total number of bits in Bloom filter (must be positive)
     */
    public static int optimalNumOfHashFunctions(long n, long m) {
        // (m / n) * log(2), but avoid truncation due to division!
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * Computes m (total bits of Bloom filter) which is expected to achieve, for
     * the specified expected insertions, the required false positive
     * probability.
     * <p>
     * See http://en.wikipedia.org/wiki/Bloom_filter#
     * Probability_of_false_positives for the formula.
     *
     * @param n expected insertions (must be positive)
     * @param p false positive rate (must be 0 < p < 1)
     */
    public static int optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }
}
