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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FetchPlanCacheCapacitySyncAction implements ISyncAction {

    public FetchPlanCacheCapacitySyncAction() {
    }

    @Override
    public ResultCursor sync() {
        PlanCache.CapacityInfo capacityInfo = PlanCache.getInstance().getCurrentCapacityInfo();

        Map<String, AtomicInteger> m = PlanCache.getInstance().getCacheKeyCountGroupBySchema();

        ArrayResultCursor result = new ArrayResultCursor("PLAN_CACHE");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("CACHE_KEY_CNT", DataTypes.LongType);
        result.addColumn("CAPACITY", DataTypes.LongType);

        for (Map.Entry<String, AtomicInteger> e : m.entrySet()) {
            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                e.getKey(),
                e.getValue().get(),
                capacityInfo.getCapacity()
            });
        }

        return result;
    }
}

