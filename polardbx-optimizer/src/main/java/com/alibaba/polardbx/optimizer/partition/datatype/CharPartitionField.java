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

import com.alibaba.polardbx.common.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.CharField;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class CharPartitionField extends AbstractPartitionField {
    protected CharPartitionField() {
    }

    protected CharPartitionField(DataType<?> fieldType) {
        field = new CharField(fieldType);
    }

    @Override
    public int compareTo(PartitionField o) {
        Preconditions.checkArgument(mysqlStandardFieldType() == o.mysqlStandardFieldType());
        Preconditions.checkArgument(packetLength() == o.packetLength());
        Preconditions.checkArgument(field.dataType().getCollationName() == o.dataType().getCollationName()
            || (field.dataType().isUtf8Encoding() && o.dataType().isUtf8Encoding()));

        CollationHandler collationHandler = field.getCollationHandler();

        Slice left = Slices.wrappedBuffer(field.rawBytes());
        Slice right = Slices.wrappedBuffer(o.rawBytes());
        int res = collationHandler.compareSp(left, right);

        return res;
    }
}
