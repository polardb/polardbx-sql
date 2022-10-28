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
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaProcesslist;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * @author shengyu
 */
public class InformationSchemaProcesslistHandler extends BaseVirtualViewSubClassHandler {

    private static final Class SHOW_PROCESSLIST_SYNC_ACTION_CLASS;

    static {
        try {
            SHOW_PROCESSLIST_SYNC_ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.server.response.ShowProcesslistSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaProcesslistHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaProcesslist;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        ISyncAction showProcesslistSyncAction;
        try {
            showProcesslistSyncAction = (ISyncAction) SHOW_PROCESSLIST_SYNC_ACTION_CLASS
                .getConstructor(boolean.class)
                .newInstance(true);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(showProcesslistSyncAction,
            executionContext.getSchemaName(), SyncScope.CURRENT_ONLY);
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }

            for (Map<String, Object> row : nodeRows) {
                cursor.addRow(new Object[] {
                    DataTypes.LongType.convertFrom(row.get("ID")),
                    DataTypes.StringType.convertFrom(row.get("USER")),
                    DataTypes.StringType.convertFrom(row.get("HOST")),
                    DataTypes.StringType.convertFrom(row.get("DB")),
                    DataTypes.StringType.convertFrom(row.get("COMMAND")),
                    DataTypes.LongType.convertFrom(row.get("TIME")),
                    DataTypes.StringType.convertFrom(row.get("STATE")),
                    DataTypes.StringType.convertFrom(row.get("INFO")),
                    DataTypes.StringType.convertFrom(row.get("SQL_TEMPLATE_ID"))});
            }
        }
        return cursor;
    }
}
