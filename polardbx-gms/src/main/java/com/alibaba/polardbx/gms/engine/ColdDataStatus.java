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

public enum ColdDataStatus {
    IMPLICIT(-1),
    OFF(0),
    ON(1);

    private final int status;

    ColdDataStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    private static Map<Integer, ColdDataStatus> MAP = new HashMap<>();

    static {
        for (ColdDataStatus coldDataStatus : ColdDataStatus.values()) {
            MAP.put(coldDataStatus.getStatus(), coldDataStatus);
        }
    }

    public static ColdDataStatus of(int status) {
        return MAP.get(status);
    }
}
