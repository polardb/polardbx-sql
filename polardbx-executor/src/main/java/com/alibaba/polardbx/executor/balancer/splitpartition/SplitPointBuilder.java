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

package com.alibaba.polardbx.executor.balancer.splitpartition;

import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;

import java.util.List;

/**
 * Build split-point for partition
 *
 * @author moyi
 * @since 2021/03
 */
interface SplitPointBuilder {

    /**
     * Build split-points for a partition-group
     *
     * @param pg the partition-group
     * @param options the rebalance options
     * @return split-points
     */
    List<SplitPoint> buildSplitPoint(PartitionGroupStat pg,
                                     BalanceOptions options);

    /**
     * Estimate rows of split partitions
     * Eg. Split 100-row partition into 4 25-row partition,
     *
     * @param pg the partition-group
     * @param options the rebalance option
     * @return rows of splited partiion
     */
    default long estimateSplitRows(PartitionGroupStat pg, BalanceOptions options) {
        int partitionNum = pg.getFirstPartition().getPartitionCount();
        long partitionDiskSize = pg.getTotalDiskSize();
        long splitNumPartitions = options.estimateSplitCount(partitionNum, partitionDiskSize);
        long rowsFirstPartition = pg.partitions.get(0).getPartitionRows();
        long splitRowsEachPartition = Math.max(1, rowsFirstPartition / splitNumPartitions);
        return splitRowsEachPartition;
    }

}
