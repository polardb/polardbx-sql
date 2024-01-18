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

package com.alibaba.polardbx.common.partition;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class MurmurHashUtils {

    //-----------------------------------------------------------------------------
    // MurmurHash3, by Austin Appleby
    // (public domain, cf. https://sites.google.com/site/murmurhash/)
    //-----------------------------------------------------------------------------
    private static long murmurHash3_128(byte[] data, int seed) {
        HashFunction hashFunc = Hashing.murmur3_128(seed);
        HashCode hashCode = hashFunc.hashBytes(data);
        long longVal = hashCode.asLong();
        return longVal;
    }

    private static final HashFunction zeroSeedMurmur3hashFunc = Hashing.murmur3_128(0);

    private static long murmurHash3_128(long data) {
        HashCode hashCode = zeroSeedMurmur3hashFunc.hashLong(data);
        long longVal = hashCode.asLong();
        return longVal;
    }

    public static long murmurHashWithZeroSeed(byte[] data) {
        return murmurHash3_128(data, 0);
    }

    public static long murmurHashWithZeroSeed(long data) {
        return murmurHash3_128(data);
    }
}

