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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumHasher;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

public class NewGroupKey extends GroupKey {
    final List<PartitionField> partFields;
    final boolean compareRawBytes;

    public NewGroupKey(List<Object> groupKeys, List<DataType> srcDataTypes, List<ColumnMeta> columns,
                       boolean compareRawBytes, ExecutionContext executionContext) {
        super(groupKeys.toArray(), columns);
        this.compareRawBytes = compareRawBytes;

        this.partFields = new ArrayList<>();
        for (int i = 0; i < groupKeys.size(); i++) {
            Object value = groupKeys.get(i);
            partFields.add(
                PartitionPrunerUtils.buildPartField(value, srcDataTypes.get(i), columns.get(i).getDataType(), null,
                    executionContext, PartFieldAccessType.DML_PRUNING));
        }
    }

    public NewGroupKey(Object[] groupKeys, List<ColumnMeta> columns,
                       boolean compareRawBytes, ExecutionContext executionContext) {
        super(groupKeys, columns);
        this.compareRawBytes = compareRawBytes;

        this.partFields = new ArrayList<>();
        for (int i = 0; i < groupKeys.length; i++) {
            Object value = groupKeys[i];
            partFields.add(PartitionPrunerUtils.buildPartField(value, columns.get(i).getDataType(),
                columns.get(i).getDataType(), null, executionContext, PartFieldAccessType.DML_PRUNING));
        }
    }

    public NewGroupKey(Object[] groupKeys, List<ColumnMeta> columns, List<RexNode> rexNodes,
                       boolean compareRawBytes, ExecutionContext executionContext) {
        super(groupKeys, columns);
        this.compareRawBytes = compareRawBytes;

        this.partFields = new ArrayList<>();
        for (int i = 0; i < groupKeys.length; i++) {
            DataType dataType;
            RexNode rex = rexNodes.get(i);
            Object value = groupKeys[i];
            if (rexNodes.get(i) == null) {
                dataType = columns.get(i).getDataType();
            } else {
                dataType = RexUtils.getTypeFromRexNode(rex, value);
            }
            partFields.add(PartitionPrunerUtils.buildPartField(value, dataType, columns.get(i).getDataType(), null,
                executionContext, PartFieldAccessType.DML_PRUNING));
        }
    }

    // Two NewGroupKey's partition field type must be exactly the same!!!
    @Override
    public boolean equals(Object obj) {
        return compareTo(obj) == 0;
    }

    private static int memCmp(byte[] left, byte[] right) {
        int minLen = Math.min(left.length, right.length);
        int index = 0;
        while (index < minLen - 1 && left[index] == right[index]) {
            index++;
        }
        return (left[index] - right[index]) != 0 ? left[index] - right[index] : left.length - right.length;
    }

    // Two NewGroupKey's partition field type must be exactly the same!!!
    @Override
    public int compareTo(Object obj) {
        NewGroupKey that = (NewGroupKey) obj;

        for (int i = 0; i < groupKeys.length; i++) {
            PartitionField srcPartField = this.partFields.get(i);
            PartitionField tarPartField = that.partFields.get(i);

            // null == null
            if (tarPartField.isNull() || srcPartField.isNull()) {
                if (tarPartField.isNull() && srcPartField.isNull()) {
                    continue;
                }
                if (srcPartField.isNull()) {
                    return -1;
                } else {
                    return 1;
                }
            }

            int cmp = compareRawBytes ? memCmp(srcPartField.rawBytes(), tarPartField.rawBytes()) :
                srcPartField.compareTo(tarPartField);
            if (cmp != 0) {
                return cmp;
            }
        }

        return 0;
    }

    @Override
    public int hashCode() {
        int ret = 1;
        for (PartitionField partField : partFields) {
            long[] seeds = new long[2];
            seeds[0] = SearchDatumHasher.INIT_HASH_VALUE_1;
            seeds[1] = SearchDatumHasher.INIT_HASH_VALUE_2;
            partField.hash(seeds);
            ret = 31 * ret + (int) (seeds[0] >> 32);
        }
        return ret;
    }

    public List<PartitionField> getPartFields() {
        return partFields;
    }
}
