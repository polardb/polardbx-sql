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

import com.alibaba.polardbx.common.utils.hash.BaseHashMethod;
import com.alibaba.polardbx.common.utils.hash.HashMethod;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;

public abstract class BloomFilterHashMethod extends BaseHashMethod implements HashMethod {
    /**
     * 由于udf没有传递seed参数
     * 此处默认seed值需保持与DN一致
     */
    protected static final int DEFAULT_HASH_SEED = 0;

    protected BloomFilterHashMethod(String funcName) {
        super(funcName);
    }

    public abstract IStreamingHasher newHasher();

    public abstract IStreamingHasher getHasher();

    /**
     * BloomFilter hash算法支持64位/128位
     */
    protected abstract boolean is64Bit();
}
