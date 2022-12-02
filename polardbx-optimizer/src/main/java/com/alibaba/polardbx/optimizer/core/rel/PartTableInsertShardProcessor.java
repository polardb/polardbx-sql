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
import com.alibaba.polardbx.optimizer.partition.exception.NoFoundPartitionsException;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartTableInsertShardProcessor extends ShardProcessor {

    public PartitionTupleRouteInfo getTupleRouteInfo() {
        return tupleRouteInfo;
    }

    protected PartitionTupleRouteInfo tupleRouteInfo;

    protected PartTableInsertShardProcessor(PartitionTupleRouteInfo tupleRouteInfo) {
        super(null);
        this.tupleRouteInfo = tupleRouteInfo;
    }

    @Override
    Pair<String, String> shard(Map<Integer, ParameterContext> param,
                               ExecutionContext executionContext) {
        PartPrunedResult prunedResult = PartitionPruner.doPruningByTupleRouteInfo(tupleRouteInfo, 0,  executionContext);
        if (prunedResult.isEmpty()) {
            throw new NoFoundPartitionsException();
        }

        List<PhysicalPartitionInfo> phyPartInfos = prunedResult.getPrunedPartitions();
        assert phyPartInfos.size() == 1;
        String grpKey = phyPartInfos.get(0).getGroupKey();
        String phyTbl = phyPartInfos.get(0).getPhyTable();
        Pair<String, String> grpAndPhy = new Pair<>(grpKey, phyTbl);
        return grpAndPhy;
    }
}
