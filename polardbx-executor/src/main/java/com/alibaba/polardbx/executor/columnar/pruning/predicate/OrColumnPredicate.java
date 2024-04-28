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
