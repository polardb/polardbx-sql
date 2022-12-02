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

package com.alibaba.polardbx.optimizer.core.join;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class LookupEquiJoinKey extends EquiJoinKey {

    /**
     * canFindOriginalColumn = (meta data query originalColumn != null)
     */
    private final boolean canFindOriginalColumn;

    private String lookupColumnName;

    public LookupEquiJoinKey(int outerIndex, int innerIndex,
                             DataType unifiedType, boolean nullSafeEqual,
                             boolean canFindOriginalColumn, String lookupColumnName) {
        super(outerIndex, innerIndex, unifiedType, nullSafeEqual);
        this.canFindOriginalColumn = canFindOriginalColumn;
        this.lookupColumnName = lookupColumnName;
    }

    public boolean isCanFindOriginalColumn() {
        return canFindOriginalColumn;
    }

    public String getLookupColunmnName() {
        return lookupColumnName;
    }
}
