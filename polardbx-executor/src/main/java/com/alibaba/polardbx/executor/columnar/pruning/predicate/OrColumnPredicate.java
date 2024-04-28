package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.ColumnIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import org.roaringbitmap.RoaringBitmap;

/**
 * @author fangwu
 */
public class OrColumnPredicate extends MultiColumnPredicate {

    @Override
    protected void handleMulti(ColumnIndex index, IndexPruneContext ipc,
                               PruneUtils.FourFunction<ColumnPredicatePruningInf, ColumnIndex, IndexPruneContext, RoaringBitmap> f,
                               RoaringBitmap cur) {
        RoaringBitmap rb = new RoaringBitmap();
        for (ColumnPredicatePruningInf columnPredicatePruningInf : children()) {
            RoaringBitmap tmp = cur.clone();
            f.apply(columnPredicatePruningInf, index, ipc, tmp);
            rb.or(tmp);
        }
        cur.and(rb);
    }

    @Override
    public StringBuilder display(String[] columns, IndexPruneContext ipc) {
        return super.display("OR", columns, ipc);
    }
}
