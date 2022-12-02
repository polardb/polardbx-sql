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

package com.alibaba.polardbx.executor.balancer.stats;

import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Stats of a partition-group
 *
 * @author moyi
 * @since 2021/04
 */
public class PartitionGroupStat {
    public PartitionGroupRecord pg;
    public List<PartitionStat> partitions = new ArrayList<>();

    public long getTotalDiskSize() {
        return this.partitions.stream().mapToLong(PartitionStat::getPartitionDiskSize).sum();
    }

    public PartitionStat getFirstPartition() {
        return this.partitions.get(0);
    }

    public Optional<PartitionStat> getLargestSizePartition() {
        if (CollectionUtils.isEmpty(partitions)) {
            return Optional.empty();
        }
        PartitionStat largest = this.partitions.get(0);
        for (PartitionStat p : partitions) {
            if (p.getPartitionDiskSize() > largest.getPartitionDiskSize()) {
                largest = p;
            }
        }
        return Optional.of(largest);
    }
}
