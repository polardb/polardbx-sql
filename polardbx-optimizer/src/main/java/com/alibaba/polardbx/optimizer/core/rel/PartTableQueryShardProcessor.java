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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartTableQueryShardProcessor extends ShardProcessor {

    protected PartitionPruneStep pruneStepInfo;

    protected PartTableQueryShardProcessor(PartitionPruneStep pruneStepInfo) {
        super(null);
        this.pruneStepInfo = pruneStepInfo;
    }

    @Override
    Pair<String, String> shard(Map<Integer, ParameterContext> param,
                               ExecutionContext executionContext) {
        PartPrunedResult prunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, executionContext);
        List<PhysicalPartitionInfo> phyPartInfos = prunedResult.getPrunedParttions();

        String grpKey = null;
        String phyTbl = null;
        if (prunedResult.isEmpty()) {
            /**
             * When no found any partitions in SingleTableOperation, use last partition as default.
             * It can be optimized to zero scan later
             */
            int partCnt = prunedResult.getPartInfo().getPartitionBy().getPartitions().size();
            PartitionLocation location =
                prunedResult.getPartInfo().getPartitionBy().getPartitions().get(partCnt - 1).getLocation();
            grpKey = location.getGroupKey();
            phyTbl = location.getPhyTableName();
        } else {
            grpKey = phyPartInfos.get(0).getGroupKey();
            phyTbl = phyPartInfos.get(0).getPhyTable();
        }

        Pair<String, String> grpAndPhy = new Pair<>(grpKey, phyTbl);
        return grpAndPhy;
    }

}
