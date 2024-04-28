package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.BloomFilterIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;

/**
 * @author fangwu
 */
public interface ColumnPredicatePruningInf {

    StringBuilder display(String[] columns, IndexPruneContext ipc);

    void sortKey(@Nonnull SortKeyIndex sortKeyIndex, IndexPruneContext ipc, @Nonnull RoaringBitmap cur);

    void bitmap(@Nonnull BitMapRowGroupIndex bitMapIndex, IndexPruneContext ipc, @Nonnull RoaringBitmap cur);

    void zoneMap(@Nonnull ZoneMapIndex zoneMapIndex, IndexPruneContext ipc, @Nonnull RoaringBitmap cur);

    void bloomFilter(@Nonnull BloomFilterIndex bloomFilterIndex, IndexPruneContext ipc,
                     @Nonnull RoaringBitmap cur);
}
