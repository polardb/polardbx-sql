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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.roaringbitmap.RoaringBitmap;

public abstract class SortKeyIndex extends BaseColumnIndex {

    /**
     * col index in the orc file, start with 0
     */
    protected final int colId;

    /**
     * column type
     */
    protected final DataType dt;

    protected SortKeyIndex(long rgNum, int colId, DataType dt) {
        super(rgNum);
        this.colId = colId;
        this.dt = dt;
    }

    abstract public void pruneEqual(Object param, RoaringBitmap cur);

    abstract public void pruneRange(Object startObj, Object endObj, RoaringBitmap cur);

    @Override
    public DataType getColumnDataType(int colId) {
        return dt;
    }

    public int getColId() {
        return colId;
    }

    public DataType getDt() {
        return dt;
    }

}
