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
import com.alibaba.polardbx.executor.sync.StoragePropertiesSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStorageProperties;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yaozhili
 */
public class InformationSchemaStoragePropertiesHandler extends BaseVirtualViewSubClassHandler {
    final private String TRUE = "TRUE";
    final private String FALSE = "FALSE";
    final private String BOTH = "SOME TRUE, SOME FALSE";
    final private String UNKNOWN = "UNKNOWN";

    public InformationSchemaStoragePropertiesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaStorageProperties;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        final String schema = executionContext.getSchemaName();
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(new StoragePropertiesSyncAction(), schema);

        Map<String, Integer> functionStatus = new HashMap<>();
        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }

            for (Map<String, Object> row : rs) {
                final String function = (String) row.get("PROPERTIES");
                final boolean status = (Boolean) row.get("STATUS");
                functionStatus.compute(function, (k, v) -> {
                    int oldStatus = (null == v) ? 0 : v;
                    return status ? (oldStatus | 0b01) : (oldStatus | 0b10);
                });
            }
        }

        for (Map.Entry<String, Integer> kv : functionStatus.entrySet()) {
            switch (kv.getValue()) {
            case 0b01:
                cursor.addRow(new Object[] {kv.getKey(), TRUE});
                break;
            case 0b10:
                cursor.addRow(new Object[] {kv.getKey(), FALSE});
                break;
            case 0b11:
                cursor.addRow(new Object[] {kv.getKey(), BOTH});
                break;
            default:
                cursor.addRow(new Object[] {kv.getKey(), UNKNOWN});
            }
        }

        return cursor;
    }
}
