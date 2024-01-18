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
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaModule;
import com.alibaba.polardbx.optimizer.view.InformationSchemaModuleEvent;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author ljl
 */
public class InformationSchemaModuleEventHandler extends BaseVirtualViewSubClassHandler {

    private static final Class ACTION_CLASS;

    static {
        try {
            ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.server.response.ShowModuleEventSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaModuleEventHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaModuleEvent;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        if (ACTION_CLASS == null) {
            throw new NotSupportException();
        }

        ISyncAction showSyncAction;
        try {
            showSyncAction = (ISyncAction) ACTION_CLASS.getConstructor(boolean.class).newInstance(true);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(showSyncAction, SystemDbHelper.INFO_SCHEMA_DB_NAME);

        List<Object[]> rows = Lists.newArrayList();
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }

            for (Map<String, Object> row : nodeRows) {
                rows.add(new Object[] {
                    DataTypes.StringType.convertFrom(row.get("MODULE_NAME")),
                    DataTypes.StringType.convertFrom(row.get("HOST")),
                    DataTypes.TimestampType.convertFrom(row.get("TIMESTAMP")),
                    DataTypes.StringType.convertFrom(row.get("LOG_PATTERN")),
                    DataTypes.StringType.convertFrom(row.get("LEVEL")),
                    DataTypes.StringType.convertFrom(row.get("EVENT")),
                    DataTypes.StringType.convertFrom(row.get("TRACE_INFO"))
                });
            }
        }
        rows.sort(new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                return DataTypes.TimestampType.compare(o1[2], o2[2]);
            }
        });
        rows.stream().forEach(r -> cursor.addRow(r));
        return cursor;
    }
}
