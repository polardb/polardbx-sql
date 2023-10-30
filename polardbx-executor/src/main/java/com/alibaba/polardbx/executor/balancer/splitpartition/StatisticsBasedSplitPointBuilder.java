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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.datatype.IntPartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

/**
 * @author guxu
 * @since 2022/06
 */
class StatisticsBasedSplitPointBuilder implements SplitPointBuilder {

    final private static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    private final ExecutionContext ec;

    public StatisticsBasedSplitPointBuilder(ExecutionContext ec) {
        this.ec = ec;
    }

    @Override
    public List<SplitPoint> buildSplitPoint(PartitionGroupStat pg, BalanceOptions options) {
        PartitionStat largestSizePartition = pg.getLargestSizePartition().get();
        if (!PolicySplitPartition.needSplit(options, largestSizePartition)) {
            return new ArrayList<>();
        }
        int splitCount = (int) (largestSizePartition.getPartitionDiskSize() / options.maxPartitionSize);
        splitCount = Math.max(BalanceOptions.SPLIT_PARTITION_MIN_COUNT,
            Math.min(BalanceOptions.SPLIT_PARTITION_MAX_COUNT, splitCount));

        return buildSplitPoint(largestSizePartition, splitCount, options.maxPartitionSize);
    }

    private List<SplitPoint> buildSplitPoint(PartitionStat partition, int expectedSplitCount, long maxPartitionSize) {
        List<SplitPoint> result = new ArrayList<>();

        final String tableSchema = partition.getPartitionRecord().getTableSchema();
        final String tableName = partition.getPartitionRecord().getTableName();
        final String partName = partition.getPartitionRecord().getPartName();
        final String tableGroupName = partition.getTableGroupName();

        //calculate split points based on sample data
        List<SearchDatumInfo> splitBounds =
            SplitPointUtils.generateSplitBounds(tableSchema, tableName, partName, expectedSplitCount, maxPartitionSize);
        LOG.info(
            "generate split point:" + splitBounds.toString() + String.format(" : for table %s partition %s", tableName,
                partName));
        if (CollectionUtils.isEmpty(splitBounds)) {
            return result;
        }

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(tableSchema).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        List<String> newPartitionNames =
            PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, splitBounds.size() + 1,
                new TreeSet<>(String::compareToIgnoreCase), false);

        if (newPartitionNames.size() != splitBounds.size() + 1) {
            throw new TddlNestableRuntimeException("auto generate partition name error");
        }

        for (int i = 0; i < splitBounds.size(); i++) {
            SplitPoint sp = new SplitPoint(splitBounds.get(i));
            sp.leftPartition = newPartitionNames.get(i);
            sp.rightPartition = newPartitionNames.get(i + 1);
            result.add(sp);
        }

        return result;
    }

}
