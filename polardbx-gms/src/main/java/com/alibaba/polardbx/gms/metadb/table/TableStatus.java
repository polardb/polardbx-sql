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

package com.alibaba.polardbx.gms.metadb.table;

/**
 * Represents status of table schema change and column schema change
 */
public enum TableStatus {

    ABSENT(0),
    PUBLIC(1),
    WRITE_ONLY(2),
    WRITE_REORG(3);

    private final int value;

    TableStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static TableStatus convert(int value) {
        switch (value) {
        case 0:
            return ABSENT;
        case 1:
            return PUBLIC;
        case 2:
            return WRITE_ONLY;
        case 3:
            return WRITE_REORG;
        default:
            return null;
        }
    }

}
