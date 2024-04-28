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

import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.BloomFilterIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ColumnIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author fangwu
 */
public abstract class MultiColumnPredicate implements ColumnPredicatePruningInf, Iterable<ColumnPredicatePruningInf> {

    private final List<ColumnPredicatePruningInf> columnPredicates = Lists.newArrayList();

    public void add(ColumnPredicatePruningInf cpp) {
        columnPredicates.add(cpp);
    }

    public void addAll(Collection<ColumnPredicatePruningInf> cpps) {
        columnPredicates.addAll(cpps);
    }

    protected List<ColumnPredicatePruningInf> children() {
        return columnPredicates;
    }

    public ColumnPredicatePruningInf flat() {
        switch (columnPredicates.size()) {
        case 0:
            return null;
        case 1:
            return columnPredicates.get(0);
        default:
            sort();
            return this;
        }
    }

    private void sort() {
        // TODO: move good prune performance expr to the front
    }

    @Override
    public @NotNull void sortKey(@NotNull SortKeyIndex sortKeyIndex, IndexPruneContext ipc,
                                 @NotNull RoaringBitmap cur) {
        handleMulti(sortKeyIndex, ipc, (p, c, i, r) -> p.sortKey(sortKeyIndex, ipc, r), cur);
    }

    @Override
    public @NotNull void bitmap(@Nonnull @NotNull BitMapRowGroupIndex bitMapIndex, IndexPruneContext ipc,
                                @NotNull RoaringBitmap cur) {
        handleMulti(bitMapIndex, ipc, (p, c, i, r) -> p.bitmap(bitMapIndex, ipc, r), cur);
    }

    @Override
    public void zoneMap(@NotNull ZoneMapIndex zoneMapIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        handleMulti(zoneMapIndex, ipc, (p, c, i, r) -> p.zoneMap(zoneMapIndex, ipc, r), cur);
    }

    @Override
    public void bloomFilter(@NotNull BloomFilterIndex bloomFilterIndex, IndexPruneContext ipc,
                            @NotNull RoaringBitmap cur) {
        handleMulti(bloomFilterIndex, ipc, (p, c, i, r) -> p.bloomFilter(bloomFilterIndex, ipc, r), cur);
    }

    protected abstract void handleMulti(ColumnIndex index, IndexPruneContext ipc,
                                        PruneUtils.FourFunction<ColumnPredicatePruningInf, ColumnIndex, IndexPruneContext, RoaringBitmap> f,
                                        RoaringBitmap cur);

    protected StringBuilder display(String delimiter, String[] columns, IndexPruneContext ipc) {
        StringBuilder sb = new StringBuilder();
        children().stream()
            .forEach(columnPredicatePruningInf ->
                sb.append("(" + columnPredicatePruningInf.display(columns, ipc) + ")")
                    .append(" " + delimiter + " "));
        if (sb.length() > 5) {
            sb.setLength(sb.length() - 5);
        }
        return sb;
    }

    @NotNull
    @Override
    public Iterator<ColumnPredicatePruningInf> iterator() {
        return columnPredicates.iterator();
    }

}
