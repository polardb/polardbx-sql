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

package com.alibaba.polardbx.optimizer.core.rel.util;

import java.util.Objects;

/**
 * @author bairui.lrj
 */
public class IndexedDynamicParamInfo implements DynamicParamInfo {
    private final int paramIndex;

    public IndexedDynamicParamInfo(int paramIndex) {
        this.paramIndex = paramIndex;
    }

    public int getParamIndex() {
        return paramIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexedDynamicParamInfo that = (IndexedDynamicParamInfo) o;
        return getParamIndex() == that.getParamIndex();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getParamIndex());
    }
}
