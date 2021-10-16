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

package com.alibaba.polardbx.optimizer.core.row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.polardbx.common.utils.memory.ObjectSizeUtils;
import com.alibaba.polardbx.optimizer.core.CursorMeta;

/**
 * 基于数组的结果集。是最基本的一行数据的形式，效率最快。
 *
 * @author Whisper
 */
public class ArrayRow extends AbstractRow implements Row {

    private final Object[] row;
    private final List<byte[]> rowBytes;

    private transient long estimatedSize = -1L;

    public ArrayRow(int capacity, CursorMeta cursorMeta) {
        super(cursorMeta);
        this.row = new Object[capacity];
        this.rowBytes = null;
        this.colNum = row.length;
    }

    public ArrayRow(CursorMeta cursorMeta, Object[] row) {
        super(cursorMeta);
        this.row = row;
        this.rowBytes = null;
        this.colNum = row.length;
    }

    public ArrayRow(CursorMeta cursorMeta, Object[] row, long estimatedSize) {
        super(cursorMeta);
        this.row = row;
        this.rowBytes = null;
        this.colNum = row.length;
        this.estimatedSize = estimatedSize;
    }

    public ArrayRow(Object[] row) {
        super(null);
        this.row = row;
        this.rowBytes = null;
        this.colNum = row.length;
    }

    public ArrayRow(Object[] row, long estimatedSize) {
        super(null);
        this.row = row;
        this.rowBytes = null;
        this.colNum = row.length;
        this.estimatedSize = estimatedSize;
    }

    public ArrayRow(CursorMeta cursorMeta, Object[] row, List<byte[]> rowBytes) {
        super(cursorMeta);
        this.row = row;
        this.rowBytes = rowBytes;
        this.colNum = row.length;
        if (rowBytes.size() > 0) {
            this.estimatedSize = rowBytes.stream().mapToLong(t -> t.length).sum();
        }
    }

    @Override
    public byte[] getBytes(int index) {
        if (this.rowBytes != null) {
            return rowBytes.get(index);
        }
        return super.getBytes(index);
    }

    @Override
    public Object getObject(int index) {
        return row[index];
    }

    @Override
    public void setObject(int index, Object value) {
        row[index] = value;
    }

    @Override
    public List<Object> getValues() {
        return Arrays.asList(row);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(row);
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        ArrayRow other = (ArrayRow) obj;
        for (int i = 0; i < row.length; i++) {
            if (!ignoreNumberType(row[i], other.row[i])) {
                return false;
            }
        }
        //if (!Arrays.equals(row, other.row)) return false;
        return true;
    }

    private boolean ignoreNumberType(Object o, Object o1) {
        if (o instanceof Number && o1 instanceof Number) {
            return ((Number) o).doubleValue() == ((Number) o1).doubleValue();
        }
        return o == null ? o1 == null : o.equals(o1);
    }

    private static final long BASE_SIZE;

    static {
        ArrayRow emptyRow = new ArrayRow(null, new Object[0], new ArrayList<>());
        BASE_SIZE = ObjectSizeUtils.calculateObjectSize(emptyRow);
    }

    @Override
    public long estimateSize() {
        if (estimatedSize == -1L) {
            long size = BASE_SIZE;

            size += row.length * ObjectSizeUtils.REFERENCE_SIZE;
            for (Object data : row) {
                size += ObjectSizeUtils.calculateDataSize(data);
            }

            if (rowBytes != null) {
                size += rowBytes.size() * (ObjectSizeUtils.REFERENCE_SIZE + ObjectSizeUtils.ARRAY_HEADER_SIZE);
                for (byte[] bytes : rowBytes) {
                    size += bytes.length;
                }
            }
            estimatedSize = size;
        }
        return estimatedSize;
    }
}
