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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * @author chenghui.lch
 */
public enum PartKeyLevel {
    /**
     * No use, maybe use for single-table or bro-table
     */
    LOGICAL_TBL_KEY(0),

    /**
     * Mean that current pruning bitset result
     * are only for a part bitset of one 1level-part
     * table or of a one 2-level-part table
     */
    PARTITION_KEY(1),

    /**
     * Mean that current pruning bitset result
     * are only for a subpart bitset of one part
     * of a non-template subpart table
     */
    SUBPARTITION_KEY(2),

    /**
     * Mean that current pruning bitset result
     * has no found part_key and full scan
     */
    NO_PARTITION_KEY(-1),

    /**
     * Only used by SubPartTable Pruning,
     * means current pruning bitset result
     * are including all subpart bitsets of all part
     */
    BOTH_PART_SUBPART_KEY(-2);

    private int levelIntVal;

    PartKeyLevel(int levelIntVal) {
        this.levelIntVal = levelIntVal;
    }

    public static PartKeyLevel getPartKeyLevelFromIntVal(Integer levelIntVal) {
        switch (levelIntVal) {
        case 0:
            return LOGICAL_TBL_KEY;
        case 1:
            return PARTITION_KEY;
        case 2:
            return SUBPARTITION_KEY;
        case -1:
            return NO_PARTITION_KEY;
        case -2:
            return BOTH_PART_SUBPART_KEY;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, "Invalid partition level value");
        }
    }

    public int getLevelIntVal() {
        return levelIntVal;
    }
}

