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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaWorkload;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author dylan
 */
public class InformationSchemaWorkloadHandler extends BaseVirtualViewSubClassHandler {

    private static final Class SHOW_PROCESSLIST_SYNC_ACTION_CLASS;

    static {
        try {
            SHOW_PROCESSLIST_SYNC_ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.server.response.ShowProcesslistSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaWorkloadHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaWorkload;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        if (SHOW_PROCESSLIST_SYNC_ACTION_CLASS == null) {
            throw new NotSupportException();
        }

        ISyncAction showProcesslistSyncAction;
        try {
            showProcesslistSyncAction = (ISyncAction) SHOW_PROCESSLIST_SYNC_ACTION_CLASS
                .getConstructor(boolean.class)
                .newInstance(true);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(showProcesslistSyncAction);

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }

            for (Map<String, Object> row : nodeRows) {
                if (row.get("COMMAND") == null || "SLEEP".equalsIgnoreCase(row.get("COMMAND").toString())) {
                    continue;
                }
                cursor.addRow(new Object[] {
                    DataTypes.LongType.convertFrom(row.get("ID")),
                    DataTypes.StringType.convertFrom(row.get("USER")),
                    DataTypes.StringType.convertFrom(row.get("HOST")),
                    DataTypes.StringType.convertFrom(row.get("DB")),
                    DataTypes.StringType.convertFrom(row.get("COMMAND")),
                    DataTypes.LongType.convertFrom(row.get("TIME")),
                    DataTypes.StringType.convertFrom(row.get("STATE")),
                    DataTypes.StringType.convertFrom(row.get("INFO")),
                    DataTypes.LongType.convertFrom(row.get("CPU")),
                    DataTypes.LongType.convertFrom(row.get("MEMORY")),
                    DataTypes.LongType.convertFrom(row.get("IO")),
                    DataTypes.DoubleType.convertFrom(row.get("NET")),
                    DataTypes.StringType.convertFrom(row.get("TYPE")),
                    DataTypes.StringType.convertFrom(row.get("ROUTE")),
                    DataTypes.StringType.convertFrom(row.get("COMPUTE_NODE"))
                });
            }
        }
        return cursor;
    }
}
