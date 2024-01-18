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
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */
public abstract class PartitionRouter {

    public static class RouterResult {

        public static final Integer NO_FOUND_PARTITION_IDX = -1;

        protected PartitionStrategy strategy;

        // for range /range columns, the route result is desc by part start position & part end position
        protected Integer partStartPosi = NO_FOUND_PARTITION_IDX;
        protected Integer pasrEndPosi = NO_FOUND_PARTITION_IDX;

        // for Hash/List/List columns/Key, the route result is desc by the set of part position
        protected Set<Integer> partPosiSet = new TreeSet<>();

        public RouterResult() {
        }
    }

    public static class LongComparator implements Comparator<Long> {

        public LongComparator() {
        }

        @Override
        public int compare(Long o1, Long o2) {

            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                if (o2 == null) {
                    return 1;
                } else {
                    return Long.compare(o1, o2);
                }
            }
        }
    }

    /**
     * Route a range query ( e.g (p1,p2,p3) >= (c1,c2,c3) ) and return target partitions
     */
    public abstract RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object searchVal);

    public abstract int getPartitionCount();

    public abstract String getDigest();

    // TODO(moyi) change type Object to SeaarchDatumInfo/PartitionField
    public static PartitionRouter createByHasher(PartitionStrategy strategy,
                                                 Object[] bounds,
                                                 SearchDatumHasher hasher,
                                                 Comparator comparator) {
        switch (strategy) {
        case HASH:
            return new HashPartRouter(bounds, hasher);
        case KEY:
            return new KeyPartRouter(bounds, hasher, comparator);
        case UDF_HASH:
            return new UdfHashPartRouter(bounds, hasher);
        default:
            throw new UnsupportedOperationException("partition strategy do not use hasher");
        }
    }

    // TODO(moyi) change type Object to SeaarchDatumInfo/PartitionField
    public static PartitionRouter createByComparator(PartitionStrategy strategy,
                                                     Object[] bounds,
                                                     Comparator comparator) {
        switch (strategy) {
        case RANGE:
        case RANGE_COLUMNS:
            return new RangePartRouter(bounds, comparator);
        default:
            throw new UnsupportedOperationException("partition strategy do not use comparator");
        }
    }

    public static PartitionRouter createByList(PartitionStrategy strategy,
                                               TreeMap<Object, Integer> sorted,
                                               Comparator comparator,
                                               boolean containDefaultPartition) {
        switch (strategy) {
        case LIST:
        case LIST_COLUMNS:
            return new ListPartRouter(sorted, comparator, containDefaultPartition);
        default:
            throw new UnsupportedOperationException("partition strategy do not use sorted map");
        }
    }
}
