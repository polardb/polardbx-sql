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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.ActionSplitPartition;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.policy.BalancePolicy;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.TableGroupStat;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.sql.SqlRebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Split partition if the partition-size exceed threshold
 *
 * @author moyi
 * @since 2021/03
 */
public class PolicySplitPartition implements BalancePolicy {

    @Override
    public String name() {
        return SqlRebalance.POLICY_SPLIT_PARTITION;
    }

    @Override
    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        List<BalanceAction> actions = new ArrayList<>();

        // iterate each partition-group
        for (TableGroupStat tgStat : stats.getTableGroupStats()) {
            for (PartitionGroupStat pg : tgStat.getPartitionGroups()) {
                PartitionStat largestSizePartition = pg.getLargestSizePartition().get();
                if (!supportAutoSplit(largestSizePartition.getPartitionStrategy())) {
                    continue;
                }
                if (!options.manually && !largestSizePartition.enableAutoSplit()) {
                    continue;
                }
                if (!needSplit(options, largestSizePartition)) {
                    continue;
                }
                if (actions.size() >= options.maxActions) {
                    break;
                }

                List<SplitPoint> spList = splitPartitionGroup(ec, options, pg);
                if (!spList.isEmpty()) {
                    actions.add(
                        new ActionSplitPartition(schemaName, pg.getLargestSizePartition().get(), spList, stats));
                }
            }
        }

        return actions;
    }

    @Override
    public List<BalanceAction> applyToTable(ExecutionContext ec,
                                            BalanceOptions options,
                                            BalanceStats stats,
                                            String schema,
                                            String tableName) {
        return applyToPartitionDb(ec, options, stats, schema);
    }

    @Override
    public List<BalanceAction> applyToTableGroup(ExecutionContext ec, BalanceOptions options, BalanceStats stats,
                                                 String schema, String tableGroupName) {
        List<BalanceAction> actions = new ArrayList<>();

        Optional<TableGroupStat> tableGroupStatOptional = stats.filterTableGroupStat(tableGroupName);
        if (!tableGroupStatOptional.isPresent()) {
            return actions;
        }

        TableGroupStat tableGroupStat = tableGroupStatOptional.get();

        //todo guxu, 这块代码跟上面重复了。applyToPartitionDb应该复用applyToTableGroup
        for (PartitionGroupStat pg : tableGroupStat.getPartitionGroups()) {
            PartitionStat largestSizePartition = pg.getLargestSizePartition().get();
            if (!supportAutoSplit(largestSizePartition.getPartitionStrategy())) {
                continue;
            }
            if (!options.manually && !largestSizePartition.enableAutoSplit()) {
                continue;
            }
            if (!needSplit(options, largestSizePartition)) {
                continue;
            }
            if (actions.size() >= options.maxActions) {
                break;
            }

            List<SplitPoint> spList = splitPartitionGroup(ec, options, pg);
            if (!spList.isEmpty()) {
                actions.add(new ActionSplitPartition(schema, pg.getLargestSizePartition().get(), spList, stats));
            }
        }

        return actions;
    }

    public static boolean supportAutoSplit(PartitionStrategy strategy) {
        return strategy.isRange() || strategy.isHashed();
    }

    public static boolean needSplit(BalanceOptions options, PartitionStat largestSizePartition) {
        return largestSizePartition.getPartitionDiskSize() > options.maxPartitionSize;
    }

    private List<SplitPoint> splitPartitionGroup(ExecutionContext ec,
                                                 BalanceOptions options,
                                                 PartitionGroupStat pg) {
        PartitionStat partition = pg.getLargestSizePartition().get();
        SplitPointBuilder builder;
        if (SplitPointUtils.supportStatistics(partition)) {
            builder = new StatisticsBasedSplitPointBuilder(ec);
        } else {
            throw new TddlNestableRuntimeException("split partition is not supported");
        }
        return builder.buildSplitPoint(pg, options);
    }

}
