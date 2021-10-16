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
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.field.BigintField;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedLongs;

/**
 * Field type longlong int (8 bytes)
 * Suitable for unsigned or signed big int field.
 */
public class BigIntPartitionField extends AbstractNumericPartitionField {
    public static final BigIntPartitionField MAX_VALUE;
    public static final BigIntPartitionField MIN_VALUE;
    public static final BigIntPartitionField MAX_UNSIGNED_VALUE;
    public static final BigIntPartitionField MIN_UNSIGNED_VALUE;

    static {
        MAX_VALUE = new BigIntPartitionField(new LongType());
        MIN_VALUE = new BigIntPartitionField(new LongType());
        MAX_UNSIGNED_VALUE = new BigIntPartitionField(new ULongType());
        MIN_UNSIGNED_VALUE = new BigIntPartitionField(new ULongType());

        MAX_VALUE.store(INT_64_MAX, new LongType());
        MIN_VALUE.store(INT_64_MIN, new LongType());
        MAX_UNSIGNED_VALUE.store(UNSIGNED_INT_64_MAX, new ULongType());
        MIN_UNSIGNED_VALUE.store(UNSIGNED_INT_64_MIN, new ULongType());
    }

    boolean isUnsigned;

    protected BigIntPartitionField(DataType<?> fieldType) {
        field = new BigintField(fieldType);
        isUnsigned = fieldType.isUnsigned();
    }

    @Override
    public int compareTo(PartitionField o) {
        Preconditions.checkArgument(this.mysqlStandardFieldType() == o.mysqlStandardFieldType());
        long a = longValue();
        long b = o.longValue();
        if (isUnsigned) {
            return UnsignedLongs.compare(a, b) < 0 ? -1
                : (UnsignedLongs.compare(a, b) > 0 ? 1 : 0);
        }
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
