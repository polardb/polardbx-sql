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

package com.alibaba.polardbx.optimizer.partition.common;

public enum BitSetLevel {

    /**
     * level for the bitset that the bitset size is the partition count of all 1st-level partition
     */
    BIT_SET_PART("PartSet"),

    /**
     * level for the bitset that the bitset size is the subpartition count of 2nd-level partitions of one of 1st-level partition
     */
    BIT_SET_ONE_SUBPART("OneSubPartSet"),

    /**
     * level for the bitset that the bitset size is the subpartition count of 2nd-level partitions of all 1st-level partitions
     */
    BIT_SET_ALL_SUBPART("AllSubPartSet");

    private String bitSetLevelName;

    BitSetLevel(String bitSetLevelName) {
        this.bitSetLevelName = bitSetLevelName;
    }

    public static BitSetLevel getBitSetLevelByPartLevel(PartKeyLevel partLevel, boolean useFullSubPartBitSet) {

        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            if (useFullSubPartBitSet) {
                return BIT_SET_ALL_SUBPART;
            } else {
                return BIT_SET_ONE_SUBPART;
            }
        } else {
            return BIT_SET_PART;
        }
    }

    public String getBitSetLevelName() {
        return bitSetLevelName;
    }
}
