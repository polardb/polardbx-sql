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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.memory.ObjectSizeUtils;
import com.alibaba.polardbx.executor.chunk.ChunkUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;

import java.util.List;

/**
 * @author chenmo.cm
 */

public class GroupKey implements Comparable {

    private static final long BASE_SIZE = ObjectSizeUtils.calculateObjectSize(new GroupKey(new Object[0], null));

    // Static type for fast equals check.
    private static final DataType varcharBinaryCollation = new VarcharType(CollationName.BINARY);
    private static final DataType charBinaryCollation = new CharType(CollationName.BINARY);

    final Object[] groupKeys;
    final List<ColumnMeta> columns;

    public GroupKey(Object[] groupKeys, List<ColumnMeta> columns) {
        this.groupKeys = groupKeys;
        this.columns = columns;
    }

    public Object[] getGroupKeys() {
        return groupKeys;
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + arrayhashCode(groupKeys);

        return result;
    }

    private int arrayhashCode(Object[] a) {
        if (a == null) {
            return 0;
        }

        int result = 1;

        for (int i = 0; i < a.length; i++) {
            Object element = a[i];
            result = 31 * result;
            if (element == null) {
                result += 0;
            } else {
                element = columns.get(i).getDataType().convertFrom(element);
                if (element instanceof byte[]) {
                    byte[] byteArray = (byte[]) element;
                    result += ChunkUtil.hashCode(byteArray, 0, byteArray.length);
                } else {
                    result += element.hashCode();
                }
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof GroupKey)) {
            return false;
        }
        GroupKey that = (GroupKey) obj;
        if (this.groupKeys.length != that.groupKeys.length) {
            return false;
        }
        for (int i = 0; i < this.groupKeys.length; i++) {
            if (ExecUtils.comp(this.groupKeys[i], that.groupKeys[i], columns.get(i).getDataType(), true) != 0) {
                return false;
            }
        }
        return true;
    }

    public boolean equalsForUpdate(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof GroupKey)) {
            return false;
        }
        GroupKey that = (GroupKey) obj;
        if (this.groupKeys.length != that.groupKeys.length) {
            return false;
        }
        for (int i = 0; i < this.groupKeys.length; i++) {
            DataType dataType = columns.get(i).getDataType();
            if (dataType instanceof VarcharType) {
                dataType = varcharBinaryCollation; // Use cached collation.
            } else if (dataType instanceof CharType) {
                dataType = charBinaryCollation; // Use cached collation.
            }
            Object thisObject = this.groupKeys[i];
            if (thisObject != null && that.groupKeys[i] != null &&
                thisObject.getClass() != that.groupKeys[i].getClass()) {
                // TODO: Implicit type convert & compare should use field store.
                if (thisObject instanceof Number && (dataType instanceof VarcharType || dataType instanceof CharType)) {
                    thisObject = columns.get(i).getDataType().convertFrom(thisObject); // Force to string.
                }
            }
            if (thisObject instanceof String && that.groupKeys[i] instanceof String) {
                return thisObject.equals(that.groupKeys[i]);
            } else if (ExecUtils.comp(thisObject, that.groupKeys[i], dataType, true) != 0) {
                return false;
            }
        }
        return true;
    }

    public long estimateSize() {
        long size = BASE_SIZE;
        size += groupKeys.length * ObjectSizeUtils.REFERENCE_SIZE;
        for (Object data : groupKeys) {
            size += ObjectSizeUtils.calculateDataSize(data);
        }
        return size;
    }

    /**
     * Null first
     */
    @Override
    public int compareTo(Object obj) {
        if (this == obj) {
            return 0;
        }

        if (obj == null) {
            return 1;
        }

        if (!(obj instanceof GroupKey)) {
            return -1;
        }

        final GroupKey that = (GroupKey) obj;
        final int length = Math.min(this.groupKeys.length, that.groupKeys.length);
        for (int i = 0; i < length; i++) {
            final int comp = ExecUtils.comp(this.groupKeys[i], that.groupKeys[i], columns.get(i).getDataType(), true);
            if (comp != 0) {
                return comp;
            }
        }

        if (this.groupKeys.length == that.groupKeys.length) {
            return 0;
        } else {
            return this.groupKeys.length < that.groupKeys.length ? -1 : 1;
        }
    }
}
