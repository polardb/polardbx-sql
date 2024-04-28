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
import com.alibaba.polardbx.optimizer.view.InformationSchemaTcpPerf;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 */
public class InformationSchemaTcpPerfHandler extends BaseVirtualViewSubClassHandler {

    private static final Class FETCH_TCP_PERF_SYNC_ACTION_CLASS;

    static {
        try {
            FETCH_TCP_PERF_SYNC_ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.executor.sync.FetchTcpPerfSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaTcpPerfHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTcpPerf;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        final ISyncAction syncAction;
        try {
            syncAction = (ISyncAction) FETCH_TCP_PERF_SYNC_ACTION_CLASS.getConstructor().newInstance();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(syncAction, SyncScope.CURRENT_ONLY);

        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }
            for (Map<String, Object> row : rs) {
                cursor.addRow(new Object[] {
                    DataTypes.StringType.convertFrom(row.get("CN")),
                    DataTypes.StringType.convertFrom(row.get("TCP")),
                    DataTypes.StringType.convertFrom(row.get("DN")),

                    DataTypes.LongType.convertFrom(row.get("SEND_MSG_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("SEND_FLUSH_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("SEND_SIZE")),
                    DataTypes.LongType.convertFrom(row.get("RECV_MSG_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("RECV_NET_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("RECV_SIZE")),

                    DataTypes.LongType.convertFrom(row.get("SESSION_CREATE_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("SESSION_DROP_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("SESSION_CREATE_SUCCESS_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("SESSION_CREATE_FAIL_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("DN_CONCURRENT_COUNT")),

                    DataTypes.LongType.convertFrom(row.get("SESSION_COUNT")),

                    DataTypes.StringType.convertFrom(row.get("CLIENT_STATE")),
                    DataTypes.DoubleType.convertFrom(row.get("TIME_SINCE_LAST_RECV")),
                    DataTypes.DoubleType.convertFrom(row.get("LIVE_TIME")),
                    DataTypes.StringType.convertFrom(row.get("FATAL_ERROR")),
                    DataTypes.LongType.convertFrom(row.get("AUTH_ID")),
                    DataTypes.DoubleType.convertFrom(row.get("TIME_SINCE_VARIABLES_REFRESH")),

                    DataTypes.StringType.convertFrom(row.get("TCP_STATE")),

                    DataTypes.LongType.convertFrom(row.get("SOCKET_SEND_BUFFER_SIZE")),
                    DataTypes.LongType.convertFrom(row.get("SOCKET_RECV_BUFFER_SIZE")),
                    DataTypes.LongType.convertFrom(row.get("READ_DIRECT_BUFFERS")),
                    DataTypes.LongType.convertFrom(row.get("READ_HEAP_BUFFERS")),
                    DataTypes.LongType.convertFrom(row.get("WRITE_DIRECT_BUFFERS")),
                    DataTypes.LongType.convertFrom(row.get("WRITE_HEAP_BUFFERS")),
                    DataTypes.BooleanType.convertFrom(row.get("REACTOR_REGISTERED")),
                    DataTypes.BooleanType.convertFrom(row.get("SOCKET_CLOSED"))
                });
            }
        }
        return cursor;
    }
}
