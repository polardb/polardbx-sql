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

public class OrderInvariantHasher {
    private final long p;
    private final long q;
    private final long r;

    private Long result;

    public OrderInvariantHasher() {
        p = 3860031L;
        q = 2779L;
        r = 2L;
        result = null;
    }

    public void add(long x) {
        if (result == null) {
            result = x;
        } else {
            result = p + q * (result + x) + r * result * x;
        }
    }

    public Long getResult() {
        return result;
    }
}
