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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartTableQueryShardProcessor extends ShardProcessor {

    private static final Logger logger = LoggerFactory.getLogger(PartTableQueryShardProcessor.class);

    protected PartitionPruneStep pruneStepInfo;
    protected PartitionTupleRouteInfo tupleRouteInfo;

    protected PartTableQueryShardProcessor(PartitionPruneStep pruneStepInfo) {
        super(null);
        this.pruneStepInfo = pruneStepInfo;
        /**
         * When a plan do sharding by using PartTableQueryShardProcessor,
         * the query must be a point-query that all the predicates of all partition columns is equal-expr.
         * so the query can be converted into tuple route for tow purpose:
         * 1. tuple route has better performance on routing;
         * 2. tuple route can make sure that only return one partition
         *     even if the expr-value has been truncated ( such datetime_col='9999-99-99 99:99:99').
         */
        this.tupleRouteInfo = tryConvertPruneStepToTupleRouteIfNeed();
    }

    protected PartitionTupleRouteInfo tryConvertPruneStepToTupleRouteIfNeed() {
        try {
            PartitionTupleRouteInfo tupleRouteInfo =
                PartitionTupleRouteInfoBuilder.genTupleRoutingInfoFromPruneStep(pruneStepInfo);
            return tupleRouteInfo;
        } catch (Throwable ex) {
            logger.warn("Failed to convert point query to tuple route, exception is " + ex.getMessage(), ex);
        }
        return null;
    }

    @Override
    Pair<String, String> shard(Map<Integer, ParameterContext> param,
                               ExecutionContext executionContext) {
        List<PhysicalPartitionInfo> phyPartInfos = null;
        PartPrunedResult prunedResult = null;
        if (tupleRouteInfo != null) {
            prunedResult = PartitionPruner.doPruningByTupleRouteInfo(tupleRouteInfo, 0, executionContext);
            phyPartInfos = prunedResult.getPrunedParttions();
        } else {
            prunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, executionContext);
            phyPartInfos = prunedResult.getPrunedParttions();
        }

        String grpKey = null;
        String phyTbl = null;
        if (phyPartInfos.isEmpty()) {
            /**
             * When no found any partitions in SingleTableOperation, use last partition as default.
             * It can be optimized to zero scan later
             */
            int partCnt = prunedResult.getPartInfo().getPartitionBy().getPhysicalPartitions().size();
            PartitionLocation location =
                prunedResult.getPartInfo().getPartitionBy().getPhysicalPartitions().get(partCnt - 1).getLocation();
            grpKey = location.getGroupKey();
            phyTbl = location.getPhyTableName();
        } else if (phyPartInfos.size() == 1) {
            /**
             * PartTableQueryShardProcessor must return only one partition after pruning
             */
            grpKey = phyPartInfos.get(0).getGroupKey();
            phyTbl = phyPartInfos.get(0).getPhyTable();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "ShardProcessor is NOT allow to return more than one partitions after pruning");
        }

        Pair<String, String> grpAndPhy = new Pair<>(grpKey, phyTbl);
        return grpAndPhy;
    }

}
