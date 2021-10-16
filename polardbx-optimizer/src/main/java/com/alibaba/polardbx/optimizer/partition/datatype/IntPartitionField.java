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
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.UIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.field.IntField;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedInts;

public class IntPartitionField extends AbstractNumericPartitionField {
    public static final IntPartitionField MAX_VALUE;
    public static final IntPartitionField MIN_VALUE;
    public static final IntPartitionField MAX_UNSIGNED_VALUE;
    public static final IntPartitionField MIN_UNSIGNED_VALUE;

    static {
        MAX_VALUE = new IntPartitionField(new IntegerType());
        MIN_VALUE = new IntPartitionField(new IntegerType());
        MAX_UNSIGNED_VALUE = new IntPartitionField(new UIntegerType());
        MIN_UNSIGNED_VALUE = new IntPartitionField(new UIntegerType());

        MAX_VALUE.store(INT_32_MAX, new IntegerType());
        MIN_VALUE.store(INT_32_MIN, new IntegerType());
        MAX_UNSIGNED_VALUE.store(UNSIGNED_INT_32_MAX, new UIntegerType());
        MIN_UNSIGNED_VALUE.store(UNSIGNED_INT_32_MIN, new UIntegerType());
    }

    protected IntPartitionField() {
    }

    protected IntPartitionField(DataType<?> fieldType) {
        field = new IntField(fieldType);
        isUnsigned = fieldType.isUnsigned();
    }

    @Override
    public int compareTo(PartitionField o) {
        Preconditions.checkArgument(this.mysqlStandardFieldType() == o.mysqlStandardFieldType());
        long a = longValue();
        long b = o.longValue();
        return a < b ? -1 : ((a > b) ? 1 : 0);
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
