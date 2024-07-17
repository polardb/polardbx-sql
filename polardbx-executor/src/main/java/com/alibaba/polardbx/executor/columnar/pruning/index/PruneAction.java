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
