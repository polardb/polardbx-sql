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
import com.alibaba.polardbx.optimizer.view.InformationSchemaSessionPerf;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 */
public class InformationSchemaSessionPerfHandler extends BaseVirtualViewSubClassHandler {

    private static final Class FETCH_SESSION_PERF_SYNC_ACTION_CLASS;

    static {
        try {
            FETCH_SESSION_PERF_SYNC_ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.executor.sync.FetchSessionPerfSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaSessionPerfHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaSessionPerf;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        final ISyncAction syncAction;
        try {
            syncAction = (ISyncAction) FETCH_SESSION_PERF_SYNC_ACTION_CLASS.getConstructor().newInstance();
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
                    DataTypes.StringType.convertFrom(row.get("DN")),
                    DataTypes.StringType.convertFrom(row.get("TCP")),
                    DataTypes.LongType.convertFrom(row.get("SESSION")),
                    DataTypes.StringType.convertFrom(row.get("STATUS")),

                    DataTypes.LongType.convertFrom(row.get("PLAN_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("QUERY_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("UPDATE_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("TSO_COUNT")),

                    DataTypes.DoubleType.convertFrom(row.get("LIVE_TIME")),
                    DataTypes.DoubleType.convertFrom(row.get("TIME_SINCE_LAST_RECV")),

                    DataTypes.StringType.convertFrom(row.get("CHARSET_CLIENT")),
                    DataTypes.StringType.convertFrom(row.get("CHARSET_RESULT")),
                    DataTypes.StringType.convertFrom(row.get("TIMEZONE")),
                    DataTypes.StringType.convertFrom(row.get("ISOLATION")),
                    DataTypes.BooleanType.convertFrom(row.get("AUTO_COMMIT")),
                    DataTypes.StringType.convertFrom(row.get("VARIABLES_CHANGED")),
                    DataTypes.StringType.convertFrom(row.get("LAST_DB")),

                    DataTypes.LongType.convertFrom(row.get("QUEUED_REQUEST_DEPTH")),

                    DataTypes.StringType.convertFrom(row.get("TRACE_ID")),
                    DataTypes.StringType.convertFrom(row.get("SQL")),
                    DataTypes.StringType.convertFrom(row.get("EXTRA")),
                    DataTypes.StringType.convertFrom(row.get("TYPE")),
                    DataTypes.StringType.convertFrom(row.get("REQUEST_STATUS")),
                    DataTypes.LongType.convertFrom(row.get("FETCH_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("TOKEN_SIZE")),
                    DataTypes.DoubleType.convertFrom(row.get("TIME_SINCE_REQUEST")),
                    DataTypes.DoubleType.convertFrom(row.get("DATA_PKT_RESPONSE_TIME")),
                    DataTypes.DoubleType.convertFrom(row.get("RESPONSE_TIME")),
                    DataTypes.DoubleType.convertFrom(row.get("FINISH_TIME")),
                    DataTypes.LongType.convertFrom(row.get("TOKEN_DONE_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("ACTIVE_OFFER_TOKEN_COUNT")),
                    DataTypes.StringType.convertFrom(row.get("START_TIME")),
                    DataTypes.BooleanType.convertFrom(row.get("RESULT_CHUNK")),
                    DataTypes.BooleanType.convertFrom(row.get("RETRANSMIT")),
                    DataTypes.BooleanType.convertFrom(row.get("USE_CACHE"))
                });
            }
        }
        return cursor;
    }
}
