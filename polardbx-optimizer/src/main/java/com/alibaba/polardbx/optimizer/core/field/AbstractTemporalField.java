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

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public abstract class AbstractTemporalField extends com.alibaba.polardbx.optimizer.core.field.AbstractStorageField {

    protected AbstractTemporalField(DataType<?> fieldType) {
        super(fieldType);
    }

    /**
     * Extract the temporal value parser flags from user defined session variables.
     *
     * @param sessionProperties user defined session variables.
     * @return The date parser flags compaction by '|'
     */
    abstract protected int dateFlags(SessionProperties sessionProperties);
}
