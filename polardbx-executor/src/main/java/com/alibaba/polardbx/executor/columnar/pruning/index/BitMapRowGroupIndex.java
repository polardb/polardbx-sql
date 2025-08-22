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

package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.roaringbitmap.RoaringBitmap;

import java.util.Map;

import static com.alibaba.polardbx.common.properties.ConnectionParams.COLUMNAR_BITMAP_INDEX_MAX_SCAN_SIZE_FOR_PRUNING;

/**
 * bitmap row group index for columnar pruning
 *
 * @author fangwu
 */
public class BitMapRowGroupIndex extends BaseColumnIndex {

    /**
     * col id -> col value -> bitset
     */
    private final Map<Integer, Map<String, RoaringBitmap>> val;
    private final Map<Integer, DataType> dtMap;
    private long sizeInBytes = -1;

    public BitMapRowGroupIndex(int rgNum, Map<Integer, Map<String, RoaringBitmap>> val, Map<Integer, DataType> dtMap) {
        super(rgNum);
        this.val = val;
        this.dtMap = dtMap;
    }

    /**
     * prune range value
     */
    public void between(int colId, Object start, boolean includeStart, Object end, boolean includeEnd,
                        RoaringBitmap cur) {
        Map<String, RoaringBitmap> bitmap = val.get(colId);
        if (bitmap == null) {
            return;
        }
        DataType dt = dtMap.get(colId);
        if (dt == null) {
            return;
        }
        if (bitmap.size() > InstConfUtil.getLong(COLUMNAR_BITMAP_INDEX_MAX_SCAN_SIZE_FOR_PRUNING)) {
            return;
        }

        // scan to merge row groups
        RoaringBitmap result = new RoaringBitmap();
        for (Map.Entry<String, RoaringBitmap> entry : bitmap.entrySet()) {
            String val = entry.getKey();
            RoaringBitmap rb = entry.getValue();
            if (start != null) {
                if (includeStart) {
                    if (check(dt, start, SqlKind.GREATER_THAN_OR_EQUAL, val)) {
                        result.or(rb);
                    }
                } else {
                    if (check(dt, start, SqlKind.GREATER_THAN, val)) {
                        result.or(rb);
                    }
                }
            }

            if (end != null) {
                if (includeEnd) {
                    if (check(dt, end, SqlKind.LESS_THAN_OR_EQUAL, val)) {
                        result.or(rb);
                    }
                } else {
                    if (check(dt, end, SqlKind.LESS_THAN, val)) {
                        result.or(rb);
                    }
                }
            }
        }
        cur.and(result);
    }

    /**
     * check if target value and source value satisfied sql kind(operator)
     *
     * @param dt datatype
     * @param target target value from request
     * @param sqlKind represent operator:GREATER_THAN, GREATER_THAN_OR_EQUAL,
     * LESS_THAN,LESS_THAN_OR_EQUAL
     * @param val source value from index
     */
    private boolean check(DataType dt, Object target, SqlKind sqlKind, Object val) {
        switch (sqlKind) {
        case GREATER_THAN:
            return dt.compare(val, target) > 0;
        case GREATER_THAN_OR_EQUAL:
            return dt.compare(val, target) >= 0;
        case LESS_THAN:
            return dt.compare(val, target) < 0;
        case LESS_THAN_OR_EQUAL:
            return dt.compare(val, target) <= 0;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_BITMAP_ROW_GROUP_INDEX,
                "not support operator for BitMapRowGroupIndex");
        }
    }

    public void pruneEquals(int colId, Object operand, RoaringBitmap cur) {
        Map<String, RoaringBitmap> bitmap = val.get(colId);
        if (bitmap == null) {
            return;
        }
        DataType dt = dtMap.get(colId);
        for (Map.Entry<String, RoaringBitmap> entry : bitmap.entrySet()) {
            if (dt.compare(operand, entry.getKey()) == 0) {
                cur.and(entry.getValue());
                return;
            }
        }
        cur.and(new RoaringBitmap());
    }

    @Override
    public boolean checkSupport(int columnId, SqlTypeName columnType) {
        return val.containsKey(columnId);
    }

    @Override
    public DataType getColumnDataType(int columnId) {
        return dtMap.get(columnId);
    }

    public String colIds() {
        return StringUtils.join(dtMap.keySet(), ",");
    }

    @Override
    public long getSizeInBytes() {
        if (this.sizeInBytes >= 0) {
            return this.sizeInBytes;
        }
        long size = 0;
        // ignore memory size of keys
        if (val != null) {
            for (Map<String, RoaringBitmap> value : val.values()) {
                if (value != null) {
                    for (RoaringBitmap bitmap : value.values()) {
                        size += bitmap.getLongSizeInBytes();
                    }
                }
            }
        }

        this.sizeInBytes = size;
        return sizeInBytes;
    }
}
