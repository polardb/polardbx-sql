package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.ColumnIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import org.roaringbitmap.RoaringBitmap;

/**
 * @author fangwu
 */
public class AndColumnPredicate extends MultiColumnPredicate {

    @Override
    protected void handleMulti(ColumnIndex index, IndexPruneContext ipc,
                               PruneUtils.FourFunction<ColumnPredicatePruningInf, ColumnIndex, IndexPruneContext, RoaringBitmap> f,
                               RoaringBitmap cur) {
        children().stream().forEach(columnPredicatePruningInf -> f.apply(columnPredicatePruningInf, index, ipc, cur));
    }

    @Override
    public StringBuilder display(String[] columns, IndexPruneContext ipc) {
        return super.display("AND", columns, ipc);
    }
}
