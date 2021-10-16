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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;

public class FetchPlanCacheCapacitySyncAction implements ISyncAction {

    private String schemaName = null;

    public FetchPlanCacheCapacitySyncAction() {
    }

    public FetchPlanCacheCapacitySyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    public FetchPlanCacheCapacitySyncAction(String schemaName, boolean withPlan) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public ResultCursor sync() {
        PlanCache.CapacityInfo capacityInfo =
            OptimizerContext.getContext(schemaName).getPlanManager().getPlanCache().getCurrentCapacityInfo();

        ArrayResultCursor result = new ArrayResultCursor("PLAN_CACHE");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("CACHE_KEY_CNT", DataTypes.LongType);
        result.addColumn("CAPACITY", DataTypes.LongType);

        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            capacityInfo.getKeyCount(),
            capacityInfo.getCapacity()
        });

        return result;
    }
}

