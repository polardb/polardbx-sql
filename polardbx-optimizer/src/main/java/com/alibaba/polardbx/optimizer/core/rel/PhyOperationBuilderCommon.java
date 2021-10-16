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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PhyOperationBuilderCommon {
    protected class ShardPlanMemoryContext {

        public MemoryAllocatorCtx memoryAllocator = null;
        public long phyOpMemSize = 0;
        public long allShardCount = 0;
    }

    public PhyOperationBuilderCommon() {
    }

    protected ShardPlanMemoryContext buildShardPlanMemoryContext(RelNode plan, String sqlTemplateStr,
                                                                 AbstractRelNode parentRel,
                                                                 Map<Integer, ParameterContext> paramsOfBuilder,
                                                                 Map<String, List<List<String>>> targetTables,
                                                                 ExecutionContext executionContext) {

        ShardPlanMemoryContext shardPlanMemoryContext = new ShardPlanMemoryContext();
        MemoryAllocatorCtx maOfPlanBuildingPool = null;

        int allShardCount = 0;
        int maxShardCountOfOneGroup = 0;
        String maxShardCountGroup = "";
        List<List<String>> maxShardCountTbList = new ArrayList<>();
        for (Map.Entry<String, List<List<String>>> t : targetTables.entrySet()) {
            String group = t.getKey();
            List<List<String>> tableNames = t.getValue();
            int shardCount = tableNames.size();
            if (shardCount > maxShardCountOfOneGroup) {
                maxShardCountGroup = group;
                maxShardCountTbList = tableNames;
                maxShardCountOfOneGroup = shardCount;
            }
            allShardCount += shardCount;
        }
        shardPlanMemoryContext.allShardCount = allShardCount;
        if (executionContext.getExplain() != null && executionContext.getTraceId() != null && !executionContext
            .isOnlyUseTmpTblPool()) {
            MemoryPool planBuilderPool = MemoryPoolUtils.getPlanBuilderPool(plan, executionContext);
            if (planBuilderPool != null) {
                String planBuildingPoolName = parentRel.getRelName();
                MemoryPool planBuildingPoolOfRel = planBuilderPool.getOrCreatePool(planBuildingPoolName);
                maOfPlanBuildingPool = planBuildingPoolOfRel.getMemoryAllocatorCtx();

                /**
                 * allocate the memory for logical sql template and the logical
                 * params
                 */
                long memOfSqlAndParams = MemoryEstimator.calcPhyTableScanAndModifyViewBuilderMemory(sqlTemplateStr,
                    paramsOfBuilder,
                    targetTables);
                maOfPlanBuildingPool.allocateReservedMemory(memOfSqlAndParams);
            }
            shardPlanMemoryContext.memoryAllocator = maOfPlanBuildingPool;

            // NOTE: for error code compatibility
            // For sharded-table: skip target tables check
            // For partition-table: check if target tables are empty
            boolean needMemCost = plan instanceof LogicalView &&
                ((LogicalView) plan).isNewPartDbTbl() ?
                GeneralUtil.isNotEmpty(targetTables) : true;

            if (plan instanceof LogicalModifyView && needMemCost) {
                shardPlanMemoryContext.phyOpMemSize = MemoryEstimator.calcPhyModifyViewScanMemCost(maxShardCountGroup,
                    maxShardCountTbList,
                    paramsOfBuilder);
            } else if (plan instanceof LogicalView && needMemCost) {
                shardPlanMemoryContext.phyOpMemSize =
                    MemoryEstimator.calcPhyTableScanMemCost(maxShardCountGroup, maxShardCountTbList, executionContext);
            }
        }

        return shardPlanMemoryContext;
    }
}
