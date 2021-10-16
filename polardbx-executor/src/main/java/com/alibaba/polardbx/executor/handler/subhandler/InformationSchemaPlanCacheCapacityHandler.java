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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchPlanCacheCapacitySyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPlanCacheCapacity;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 获取每个schema下plan cache的entry数量与总容量
 */
public class InformationSchemaPlanCacheCapacityHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaPlanCacheCapacityHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaPlanCacheCapacity;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        Set<String> schemaNames = OptimizerContext.getActiveSchemaNames();
        for (String schemaName : schemaNames) {

            if (SystemDbHelper.CDC_DB_NAME.equalsIgnoreCase(schemaName)) {
                continue;
            }

            List<List<Map<String, Object>>> results =
                SyncManagerHelper.sync(new FetchPlanCacheCapacitySyncAction(schemaName),
                    schemaName);

            for (List<Map<String, Object>> nodeRows : results) {
                if (nodeRows == null) {
                    continue;
                }

                for (Map<String, Object> row : nodeRows) {

                    final String host = DataTypes.StringType.convertFrom(row.get("COMPUTE_NODE"));
                    final String cacheKeyCount = DataTypes.StringType.convertFrom(row.get("CACHE_KEY_CNT"));
                    final Long capacity = DataTypes.LongType.convertFrom(row.get("CAPACITY"));

                    cursor.addRow(new Object[] {
                        host,
                        schemaName,
                        cacheKeyCount,
                        capacity
                    });
                }
            }
        }
        return cursor;
    }
}

