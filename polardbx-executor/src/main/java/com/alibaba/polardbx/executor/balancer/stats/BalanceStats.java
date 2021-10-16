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

import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Statistics for balancer
 *
 * @author moyi
 * @since 2021/03
 */
@Getter
public class BalanceStats {

    private String schema;

    /**
     * For database='partitioning'
     */
    private List<TableGroupStat> tableGroupStats;

    /**
     * For database='sharding'
     */
    private List<GroupStats.GroupsOfStorage> groups;

    private BalanceStats() {
    }

    public static BalanceStats createForSharding(String schema, List<GroupStats.GroupsOfStorage> groupStats) {
        BalanceStats res = new BalanceStats();
        res.schema = schema;
        res.groups = groupStats;
        return res;
    }

    public static BalanceStats createForPartition(String schema, List<TableGroupStat> tableGroupStats) {
        BalanceStats res = new BalanceStats();
        res.schema = schema;
        res.tableGroupStats = tableGroupStats;
        return res;
    }

    /**
     * Get stats of all partition-groups
     *
     * @return stats
     */
    public List<PartitionStat> getPartitionStats() {
        return tableGroupStats.stream()
            .flatMap(x -> x.getPartitions().stream())
            .collect(Collectors.toList());
    }

    public List<PartitionGroupStat> getPartitionGroupStats() {
        return this.tableGroupStats.stream()
            .flatMap(x -> x.getPartitionGroups().stream())
            .collect(Collectors.toList());
    }

    /**
     * All partitioning groups of this schema
     *
     * @return group list
     */
    public List<String> getAllGroups() {
        Set<String> result = new HashSet<>();
        for (List<GroupStats.GroupsOfStorage> entry : GroupStats.getGroupsOfPartition(this.schema).values()) {
            for (GroupStats.GroupsOfStorage storage : entry) {
                for (GroupDetailInfoExRecord groupDetail : storage.groups) {
                    result.add(groupDetail.groupName);
                }
            }
        }
        return new ArrayList<>(result);
    }

    @Override
    public String toString() {
        return "BalanceStats{" +
            "schema='" + schema + '\'' +
            ", tableGroupStats=" + tableGroupStats +
            ", groups=" + groups +
            '}';
    }
}
