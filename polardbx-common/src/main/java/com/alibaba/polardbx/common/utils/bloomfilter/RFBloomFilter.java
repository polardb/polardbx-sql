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

public interface RFBloomFilter {
    static BlockLongBloomFilter createBlockLongBloomFilter(int expectedEntries) {
        return new BlockLongBloomFilter(expectedEntries);
    }

    static ConcurrentIntBloomFilter createConcurrentIntBloomFilter(int expectedEntries, double fpp) {
        return ConcurrentIntBloomFilter.create(expectedEntries, fpp);
    }

    void putInt(int value);

    boolean mightContainInt(int value);

    void putLong(long value);

    boolean mightContainLong(long value);

    long sizeInBytes();

    void merge(RFBloomFilter other);
}
