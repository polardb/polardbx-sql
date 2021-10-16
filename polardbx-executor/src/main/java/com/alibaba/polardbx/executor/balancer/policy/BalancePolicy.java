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

import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Balance Policy compute actions to balance data
 *
 * @author moyi
 * @since 2021/03
 */
public interface BalancePolicy {

    /**
     * Name of this policy. It could be used as parameters in rebalance command
     */
    String name();

    /**
     * Apply to multiple database
     */
    default List<BalanceAction> applyToMultiDb(ExecutionContext ec,
                                               Map<String, BalanceStats> stats,
                                               BalanceOptions options,
                                               List<String> schemaNameList) {
        return schemaNameList.stream()
            .flatMap(schema -> applyToDb(ec, stats.get(schema), options, schema).stream())
            .collect(Collectors.toList());
    }

    default List<BalanceAction> applyToDb(ExecutionContext ec,
                                          BalanceStats stats,
                                          BalanceOptions options,
                                          String schema) {
        return DbInfoManager.getInstance().isNewPartitionDb(schema) ?
            applyToPartitionDb(ec, options, stats, schema) :
            applyToShardingDb(ec, options, stats, schema);
    }

    /**
     * Apply to database which partition_mode='partitioning'
     */
    default List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                   BalanceOptions options,
                                                   BalanceStats stats,
                                                   String schemaName) {
        throw new UnsupportedOperationException("TODO");
    }

    /**
     * Apply to database which partition_mode='sharding'
     */
    default List<BalanceAction> applyToShardingDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        throw new UnsupportedOperationException("TODO");
    }
}
