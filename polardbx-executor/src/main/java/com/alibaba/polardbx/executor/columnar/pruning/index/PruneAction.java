package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import org.roaringbitmap.RoaringBitmap;

/**
 * @author fangwu
 */
public enum PruneAction {
    LOAD_PRUNE_INDEX((pred, columnIndex, ipc, cur) -> {
    }),
    SORT_KEY_INDEX_PRUNE((pred, columnIndex, ipc, cur) -> pred.sortKey((SortKeyIndex) columnIndex, ipc, cur)),
    ZONE_MAP_INDEX_PRUNE((pred, columnIndex, ipc, cur) -> pred.zoneMap((ZoneMapIndex) columnIndex, ipc, cur)),
    BITMAP_INDEX_PRUNE((pred, columnIndex, ipc, cur) -> pred.bitmap((BitMapRowGroupIndex) columnIndex, ipc, cur)),
    BLOOM_FILTER_INDEX_PRUNE(
        (pred, columnIndex, ipc, cur) -> pred.bloomFilter((BloomFilterIndex) columnIndex, ipc, cur));

    private final PruneUtils.FourFunction<ColumnPredicatePruningInf,
        ColumnIndex,
        IndexPruneContext,
        RoaringBitmap> pruneAction;

    PruneAction(
        PruneUtils.FourFunction<ColumnPredicatePruningInf,
            ColumnIndex,
            IndexPruneContext,
            RoaringBitmap> pruneAction) {
        this.pruneAction = pruneAction;
    }

    public PruneUtils.FourFunction<ColumnPredicatePruningInf, ColumnIndex, IndexPruneContext, RoaringBitmap> getPruneAction() {
        return pruneAction;
    }
}
