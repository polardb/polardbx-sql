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
import com.alibaba.polardbx.optimizer.core.datatype.MediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UMediumIntType;
import com.alibaba.polardbx.optimizer.core.field.MediumIntField;

public class MediumIntPartitionField extends IntPartitionField {
    public static final MediumIntPartitionField MAX_VALUE;
    public static final MediumIntPartitionField MIN_VALUE;
    public static final MediumIntPartitionField MAX_UNSIGNED_VALUE;
    public static final MediumIntPartitionField MIN_UNSIGNED_VALUE;

    static {
        MAX_VALUE = new MediumIntPartitionField(new MediumIntType());
        MIN_VALUE = new MediumIntPartitionField(new MediumIntType());
        MAX_UNSIGNED_VALUE = new MediumIntPartitionField(new UMediumIntType());
        MIN_UNSIGNED_VALUE = new MediumIntPartitionField(new UMediumIntType());

        MAX_VALUE.store(INT_24_MAX, new MediumIntType());
        MIN_VALUE.store(INT_24_MIN, new MediumIntType());
        MAX_UNSIGNED_VALUE.store(UNSIGNED_INT_24_MAX, new UMediumIntType());
        MIN_UNSIGNED_VALUE.store(UNSIGNED_INT_24_MIN, new UMediumIntType());
    }

    protected MediumIntPartitionField(DataType<?> fieldType) {
        field = new MediumIntField(fieldType);
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
