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
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import com.alibaba.polardbx.optimizer.core.field.TinyIntField;

public class TinyIntPartitionField extends IntPartitionField {

    public static final TinyIntPartitionField MAX_VALUE;
    public static final TinyIntPartitionField MIN_VALUE;
    public static final TinyIntPartitionField MAX_UNSIGNED_VALUE;
    public static final TinyIntPartitionField MIN_UNSIGNED_VALUE;

    static {
        MAX_VALUE = new TinyIntPartitionField(new TinyIntType());
        MIN_VALUE = new TinyIntPartitionField(new TinyIntType());
        MAX_UNSIGNED_VALUE = new TinyIntPartitionField(new UTinyIntType());
        MIN_UNSIGNED_VALUE = new TinyIntPartitionField(new UTinyIntType());

        MAX_VALUE.store(INT_8_MAX, new TinyIntType());
        MIN_VALUE.store(INT_8_MIN, new TinyIntType());
        MAX_UNSIGNED_VALUE.store(UNSIGNED_INT_8_MAX, new UTinyIntType());
        MIN_UNSIGNED_VALUE.store(UNSIGNED_INT_8_MIN, new UTinyIntType());
    }

    protected TinyIntPartitionField(DataType<?> fieldType) {
        field = new TinyIntField(fieldType);
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
