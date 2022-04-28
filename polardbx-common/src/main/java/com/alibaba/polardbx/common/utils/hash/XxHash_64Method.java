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

package com.alibaba.polardbx.common.utils.hash;

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterHashMethod;

public class XxHash_64Method extends BloomFilterHashMethod {

    public static final String METHOD_NAME = "xxhash_64";

    private final IStreamingHasher xxHash64Hasher = new XxHash_64Hasher(DEFAULT_HASH_SEED);

    private XxHash_64Method() {
        super(METHOD_NAME);
    }

    public static XxHash_64Method create(Object... args) {
        return new XxHash_64Method();
    }

    @Override
    public IStreamingHasher newHasher() {
        return new XxHash_64Hasher(DEFAULT_HASH_SEED);
    }

    @Override
    public IStreamingHasher getHasher() {
        return xxHash64Hasher;
    }

    @Override
    protected boolean is64Bit() {
        return true;
    }
}
