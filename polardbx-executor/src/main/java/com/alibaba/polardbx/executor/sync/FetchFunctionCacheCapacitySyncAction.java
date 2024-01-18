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
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class FetchFunctionCacheCapacitySyncAction implements ISyncAction {
    public FetchFunctionCacheCapacitySyncAction() {

    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("FUNCTION_CACHE_CAPACITY");
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("USED_SIZE", DataTypes.LongType);
        result.addColumn("TOTAL_SIZE", DataTypes.LongType);

        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            StoredFunctionManager.getInstance().getUsedSize(),
            StoredFunctionManager.getInstance().getTotalSize()
        });

        return result;
    }
}
