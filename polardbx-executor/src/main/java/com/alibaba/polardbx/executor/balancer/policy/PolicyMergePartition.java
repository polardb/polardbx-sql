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

package com.alibaba.polardbx.executor.balancer.policy;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.ActionMergePartition;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.splitpartition.SplitNameBuilder;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.TableGroupStat;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.val;
import org.apache.calcite.sql.SqlRebalance;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Merge small partitions into expected size, to avoid fragmentation.
 *
 * @author moyi
 * @since 2021/03
 */
public class PolicyMergePartition implements BalancePolicy {

    final private static Logger LOG = LoggerFactory.getLogger(PolicyMergePartition.class);
    final private static double MERGED_PARTITION_LOW_RATIO = 0.5;

    @Override
    public String name() {
        return SqlRebalance.POLICY_MERGE_PARTITION;
    }

    /**
     * Algorithm:
     * 1. Group partitions by logical-table, and sort by ordinal
     * 2. Check if adjacent partition's disk-size is smaller than threshold
     * 3. Merge adjacent partitions into bigger one if needed
     */
    @Override
    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        List<BalanceAction> actions = new ArrayList<>();

        for (TableGroupStat tg : stats.getTableGroupStats()) {
            MergeContext group = new MergeContext(schemaName, tg);
            long splitSize = options.estimateSplitPartitionSize(tg.getPartitions().size());
            long maxSize = (long) (splitSize * MERGED_PARTITION_LOW_RATIO);

            group.merge(actions, maxSize);
            if (actions.size() > options.maxActions) {
                break;
            }
        }

        return actions;
    }

    @Override
    public List<BalanceAction> applyToTable(ExecutionContext ec,
                                            BalanceOptions options,
                                            BalanceStats stats,
                                            String schemaName,
                                            String tableName) {
        return applyToPartitionDb(ec, options, stats, schemaName);

    }

    static class MergeContext {

        private String schema;
        /**
         * Partition-Groups used by this context
         */
        private final List<PartitionGroupStat> partitionGroupStats;

        public MergeContext(String schema, TableGroupStat tableGroupStat) {
            this.schema = schema;
            this.partitionGroupStats = new ArrayList<>(tableGroupStat.getPartitionGroups());
            this.organize();
        }

        private void organize() {
            this.partitionGroupStats.sort(Comparator.comparingInt(x -> x.getFirstPartition().getPosition()));
        }

        private void validate() {
            for (int i = 0; i < this.partitionGroupStats.size(); i++) {
                if (i + 1 != this.partitionGroupStats.get(i).getFirstPartition().getPosition()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "position unmatched");
                }
            }
        }

        private ActionMergePartition mergePartitions(List<PartitionStat> accumulated) {
            if (accumulated.size() <= 1) {
                return null;
            }
            String name = SplitNameBuilder.buildMergeName(accumulated);
            ActionMergePartition action = new ActionMergePartition(schema, accumulated, name);
            LOG.debug(String.format("merge %s into new partition %s", accumulated, name));
            return action;
        }

        /**
         * Merge adjacent small partitions into a larger one
         */
        public void merge(List<BalanceAction> actions, long maxSize) {
            validate();
            if (this.partitionGroupStats.size() <= BalanceOptions.DEFAULT_MIN_PARTITIONS) {
                return;
            }

            int remainPartitions = this.partitionGroupStats.size();
            long accumulatedSize = 0;
            List<PartitionStat> accumulated = new ArrayList<>();
            for (val part : this.partitionGroupStats) {
                long candidateSize = accumulatedSize + part.getTotalDiskSize();

                if (candidateSize >= maxSize) {
                    ActionMergePartition action = mergePartitions(accumulated);
                    if (action != null) {
                        remainPartitions -= accumulated.size();
                        actions.add(action);
                    }

                    // Make sure the least number of partitions
                    if (remainPartitions <= BalanceOptions.DEFAULT_MIN_PARTITIONS) {
                        return;
                    }
                    accumulated = new ArrayList<>();
                    accumulated.add(part.getFirstPartition());
                    accumulatedSize = part.getTotalDiskSize();
                } else {
                    accumulated.add(part.getFirstPartition());
                    accumulatedSize = candidateSize;
                }
            }

            // merge remain partitions
            ActionMergePartition action = mergePartitions(accumulated);
            if (action != null) {
                actions.add(action);
            }
        }

    }
}
