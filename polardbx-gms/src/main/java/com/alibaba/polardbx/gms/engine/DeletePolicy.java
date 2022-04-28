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

public enum DeletePolicy {
    NEVER(0),
    MASTER_ONLY(1),
    MASTER_SLAVE(2);

    long value;
    DeletePolicy(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public static Map<Long, DeletePolicy> MAP = new HashMap<>();
    static {
        for (DeletePolicy deletePolicy : DeletePolicy.values()) {
            MAP.put(deletePolicy.getValue(), deletePolicy);
        }
    }
}
