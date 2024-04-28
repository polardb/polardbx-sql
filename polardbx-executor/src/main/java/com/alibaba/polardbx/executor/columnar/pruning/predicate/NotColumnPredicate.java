package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.BloomFilterIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;

/**
 * @author fangwu
 */
public class NotColumnPredicate implements ColumnPredicatePruningInf {
    ColumnPredicatePruningInf child;

    public NotColumnPredicate(ColumnPredicatePruningInf cpp) {
        this.child = cpp;
    }

    @Override
    public StringBuilder display(String[] columns, IndexPruneContext ipc) {
        StringBuilder sb = new StringBuilder();
        sb.append("NOT(")
            .append(child.display(columns, ipc))
            .append(")");
        return sb;
    }

    @Override
    public void sortKey(@NotNull SortKeyIndex sortKeyIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        return;
    }

    @Override
    public void bitmap(@Nonnull @NotNull BitMapRowGroupIndex bitMapIndex, IndexPruneContext ipc,
                       @NotNull RoaringBitmap cur) {
        return;
    }

    @Override
    public void zoneMap(@NotNull ZoneMapIndex zoneMapIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        return;
    }

    @Override
    public void bloomFilter(@NotNull BloomFilterIndex bloomFilterIndex, IndexPruneContext ipc,
                            @NotNull RoaringBitmap cur) {
        return;
    }
}
