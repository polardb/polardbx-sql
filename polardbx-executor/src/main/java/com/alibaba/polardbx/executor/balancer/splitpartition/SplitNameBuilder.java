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

import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;

import java.util.List;

/**
 * Generate partition names for partition-split
 * Eg. partition p1 split into three partitions: [p1_0, p1_1, p1_2]
 *
 * @author moyi
 * @since 2021/04
 */
public class SplitNameBuilder {

    private final String originPartitionName;
    private int idx = 0;

    public SplitNameBuilder(String originPartitionName) {
        this.originPartitionName = originPartitionName;
    }

    public void build(SplitPoint sp) {
        sp.leftPartition = this.originPartitionName + "_" + idx++;
        sp.rightPartition = this.originPartitionName + "_" + idx;
    }

    public SplitPoint build(SearchDatumInfo value) {
        SplitPoint sp = new SplitPoint(value);
        build(sp);
        return sp;
    }

    /**
     * Generate partition name for merge-partition
     *
     * TODO(moyi) generate an elegant name
     *
     * @param toMerge partitions to merge
     * @return name of the merge partition
     */
    public static String buildMergeName(List<PartitionStat> toMerge) {
        return toMerge.get(0).getPartitionName() + "_1";
    }

}
