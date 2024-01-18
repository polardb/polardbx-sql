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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;

public class ResizePlanCacheSyncAction implements ISyncAction {

    private int newSize;

    public ResizePlanCacheSyncAction() {

    }

    public ResizePlanCacheSyncAction(int newSize) {
        this.newSize = newSize;
    }

    @Override
    public ResultCursor sync() {
        Pair<PlanCache.CapacityInfo, PlanCache.CapacityInfo> infoPair = PlanCache.getInstance().resize(newSize);
        ArrayResultCursor result = new ArrayResultCursor("plancache_capacity");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("OLD_CNT", DataTypes.LongType);
        result.addColumn("OLD_CAPACITY", DataTypes.LongType);
        result.addColumn("NEW_CNT", DataTypes.LongType);
        result.addColumn("NEW_CAPACITY", DataTypes.LongType);

        PlanCache.CapacityInfo oldInfo = infoPair.getKey();
        PlanCache.CapacityInfo newInfo = infoPair.getValue();
        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            oldInfo.getKeyCount(), oldInfo.getCapacity(),
            newInfo.getKeyCount(), newInfo.getCapacity()});
        return result;
    }

    public int getNewSize() {
        return newSize;
    }

    public void setNewSize(int newSize) {
        this.newSize = newSize;
    }
}
