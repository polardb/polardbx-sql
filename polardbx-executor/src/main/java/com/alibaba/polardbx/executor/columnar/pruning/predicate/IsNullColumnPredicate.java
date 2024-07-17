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

package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.BloomFilterIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;

/**
 * @author fangwu
 */
public class IsNullColumnPredicate extends ColumnPredicate {

    public IsNullColumnPredicate(int colId) {
        super(null, colId);
    }

    @Override
    public StringBuilder display(String[] columns, IndexPruneContext ipc) {
        Preconditions.checkArgument(columns != null && columns.length > colId, "error column meta");

        StringBuilder sb = new StringBuilder();
        sb.append(columns[colId])
            .append("_")
            .append(colId)
            .append(" IS NULL ");
        return sb;
    }

    @Override
    public void sortKey(@NotNull SortKeyIndex sortKeyIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
    }

    @Override
    public void bitmap(@Nonnull @NotNull BitMapRowGroupIndex bitMapIndex, IndexPruneContext ipc,
                       @NotNull RoaringBitmap cur) {
    }

    @Override
    public void zoneMap(@NotNull ZoneMapIndex zoneMapIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        if (!zoneMapIndex.checkSupport(colId, type)) {
            return;
        }

        zoneMapIndex.pruneNull(colId, cur);
    }

    @Override
    public void bloomFilter(@NotNull BloomFilterIndex bloomFilterIndex, IndexPruneContext ipc,
                            @NotNull RoaringBitmap cur) {
        // TODO support bloom filter
    }

}
