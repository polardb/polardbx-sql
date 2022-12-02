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

/**
 * Equi-Join Key Tuple
 *
 */
public class EquiJoinKey {

    private final int outerIndex;
    private final int innerIndex;
    private final DataType unifiedType;

    /**
     * Whether to treat NULL equals NULL? When set to true, it means <code><=></code>
     */
    private final boolean nullSafeEqual;

    public EquiJoinKey(int outerIndex, int innerIndex, DataType unifiedType, boolean nullSafeEqual) {
        this.outerIndex = outerIndex;
        this.innerIndex = innerIndex;
        this.unifiedType = unifiedType;
        this.nullSafeEqual = nullSafeEqual;
    }

    public int getOuterIndex() {
        return outerIndex;
    }

    public int getInnerIndex() {
        return innerIndex;
    }

    public DataType getUnifiedType() {
        return unifiedType;
    }

    public boolean isNullSafeEqual() {
        return nullSafeEqual;
    }
}
