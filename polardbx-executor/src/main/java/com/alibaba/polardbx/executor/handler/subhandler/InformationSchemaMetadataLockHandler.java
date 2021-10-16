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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaMetadataLock;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 */
public class InformationSchemaMetadataLockHandler extends BaseVirtualViewSubClassHandler {

    private static final Class FETCH_METADATA_LOCK_SYNC_ACTION_CLASS;

    static {
        try {
            FETCH_METADATA_LOCK_SYNC_ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.executor.sync.FetchMetadataLockSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaMetadataLockHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaMetadataLock;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        final ISyncAction syncAction;
        try {
            syncAction = (ISyncAction) FETCH_METADATA_LOCK_SYNC_ACTION_CLASS.getConstructor().newInstance();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(syncAction);

        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }
            for (Map<String, Object> row : rs) {
                cursor.addRow(new Object[] {
                    DataTypes.StringType.convertFrom(row.get("NODE")),
                    DataTypes.StringType.convertFrom(row.get("CONN_ID")),
                    DataTypes.LongType.convertFrom(row.get("TRX_ID")),
                    DataTypes.StringType.convertFrom(row.get("TRACE_ID")),
                    DataTypes.StringType.convertFrom(row.get("SCHEMA")),
                    DataTypes.StringType.convertFrom(row.get("TABLE")),
                    DataTypes.StringType.convertFrom(row.get("TYPE")),
                    DataTypes.StringType.convertFrom(row.get("DURATION")),
                    DataTypes.IntegerType.convertFrom(row.get("VALIDATE")),
                    DataTypes.StringType.convertFrom(row.get("FRONTEND")),
                    DataTypes.StringType.convertFrom(row.get("SQL"))
                });
            }
        }
        return cursor;
    }
}
