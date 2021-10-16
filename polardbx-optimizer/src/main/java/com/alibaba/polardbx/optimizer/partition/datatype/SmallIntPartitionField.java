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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.USmallIntType;
import com.alibaba.polardbx.optimizer.core.field.SmallIntField;

public class SmallIntPartitionField extends IntPartitionField {
    public static final SmallIntPartitionField MAX_VALUE;
    public static final SmallIntPartitionField MIN_VALUE;
    public static final SmallIntPartitionField MAX_UNSIGNED_VALUE;
    public static final SmallIntPartitionField MIN_UNSIGNED_VALUE;

    static {
        MAX_VALUE = new SmallIntPartitionField(new SmallIntType());
        MIN_VALUE = new SmallIntPartitionField(new SmallIntType());
        MAX_UNSIGNED_VALUE = new SmallIntPartitionField(new USmallIntType());
        MIN_UNSIGNED_VALUE = new SmallIntPartitionField(new USmallIntType());

        MAX_VALUE.store(INT_16_MAX, new SmallIntType());
        MIN_VALUE.store(INT_16_MIN, new SmallIntType());
        MAX_UNSIGNED_VALUE.store(UNSIGNED_INT_16_MAX, new USmallIntType());
        MIN_UNSIGNED_VALUE.store(UNSIGNED_INT_16_MIN, new USmallIntType());
    }

    protected SmallIntPartitionField(DataType<?> fieldType) {
        field = new SmallIntField(fieldType);
        isUnsigned = fieldType.isUnsigned();
    }

    @Override
    public PartitionField maxValue() {
        return isUnsigned ? MAX_UNSIGNED_VALUE : MAX_VALUE;
    }

    @Override
    public PartitionField minValue() {
        return isUnsigned ? MIN_UNSIGNED_VALUE : MIN_VALUE;
    }
}
