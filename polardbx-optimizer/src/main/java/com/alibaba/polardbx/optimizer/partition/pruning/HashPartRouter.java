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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;

/**
 * @author chenghui.lch
 */
public class HashPartRouter extends RangePartRouter {

    protected int partitionCount = 0;
    protected SearchDatumHasher hasher = null;

    public HashPartRouter(Object[] sortedBoundObjArr, SearchDatumHasher hasher) {
        super(sortedBoundObjArr, new LongComparator());
        this.partitionCount = sortedBoundObjArr.length;
        this.hasher = hasher;
    }

    @Override
    public RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object searchVal) {
        RouterResult rs = null;
        if (comp == ComparisonKind.EQUAL) {
            // Convert the searchVal from field space to hash space
            long hashVal = hasher.calcHashCode(ec, (SearchDatumInfo) searchVal);
            rs = super.routePartitions(ec, comp, hashVal);
        } else {
            rs = super.routePartitions(ec, ComparisonKind.NOT_EQUAL, searchVal);
        }
        rs.strategy = PartitionStrategy.HASH;
        return rs;
    }

}
