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

public class HashMethodFactory {

    public static BloomFilterHashMethod buildForBloomFilter(HashMethodInfo hashMethodInfo) {
        switch (hashMethodInfo.getMethodName()) {
        case Murmur3_128Method.METHOD_NAME:
            return Murmur3_128Method.create(hashMethodInfo.getArgs());
        case XxHash_64Method.METHOD_NAME:
            return XxHash_64Method.create(hashMethodInfo.getArgs());
        }
        throw new IllegalArgumentException("Unrecognized hash method name: " + hashMethodInfo);
    }
}
