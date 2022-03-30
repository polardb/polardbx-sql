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

/**
 * 单个Method内 hasher可复用
 */
public class Murmur3_128Method extends BloomFilterHashMethod {
    public static final String METHOD_NAME = "murmur3_128";

    private final IStreamingHasher murmur3Hasher = new Murmur3_128Hasher(DEFAULT_HASH_SEED);

    private Murmur3_128Method() {
        super(METHOD_NAME);
    }

    public static Murmur3_128Method create(Object... args) {
        // 如果有附加参数 需要在此处理
        return new Murmur3_128Method();
    }

    @Override
    public IStreamingHasher newHasher() {
        return new Murmur3_128Hasher(DEFAULT_HASH_SEED);
    }

    @Override
    public IStreamingHasher getHasher() {
        return murmur3Hasher;
    }

    @Override
    protected boolean is64Bit() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
