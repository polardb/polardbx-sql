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

import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.TimestampField;
import com.google.common.base.Preconditions;

/**
 * timestamp(N) type
 * In string context: YYYY-MM-DD HH:MM:SS.FFFFFF
 * In number context: YYYYMMDDHHMMSS.FFFFFF
 * Stored as a 7 byte value
 */
public class TimestampPartitionField extends AbstractPartitionField {
    protected TimestampPartitionField(DataType<?> fieldType) {
        field = new TimestampField(fieldType);
    }

    @Override
    public int compareTo(PartitionField toCmp) {
        int len = packetLength();
        // util now, we can't ensure the metadata of both fields are the same.
        Preconditions.checkArgument(mysqlStandardFieldType() == toCmp.mysqlStandardFieldType());
        Preconditions.checkArgument(len == toCmp.packetLength());
        byte[] left = rawBytes();
        byte[] right = toCmp.rawBytes();
        return memCmp(left, right, len);
    }

    MySQLTimeVal readFromBinary() {
        return ((TimestampField) field).readFromBinary();
    }
}
