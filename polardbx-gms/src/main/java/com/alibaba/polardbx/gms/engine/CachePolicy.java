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

package com.alibaba.polardbx.gms.engine;

import java.util.HashMap;
import java.util.Map;

public enum CachePolicy {
    NO_CACHE(0),
    META_CACHE(1),
    DATA_CACHE(2),
    META_AND_DATA_CACHE(3);

    long value;
    CachePolicy(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public static Map<Long, CachePolicy> MAP = new HashMap<>();
    static {
        for (CachePolicy cachePolicy : CachePolicy.values()) {
            MAP.put(cachePolicy.getValue(), cachePolicy);
        }
    }
}
