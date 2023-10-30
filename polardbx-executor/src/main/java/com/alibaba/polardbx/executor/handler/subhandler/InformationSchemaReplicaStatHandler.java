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
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.RplStatMetrics;
import com.alibaba.polardbx.gms.metadb.cdc.RplStatMetricsAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaReplicaStat;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.util.List;

/**
 * @version 1.0
 */
public class InformationSchemaReplicaStatHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaReplicaStatHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaReplicaStat;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            RplStatMetricsAccessor rplStatMetricsAccessor = new RplStatMetricsAccessor();
            rplStatMetricsAccessor.setConnection(metaDbConn);
            List<RplStatMetrics> rplStatMetrics = rplStatMetricsAccessor.getAllMetrics();
            for (RplStatMetrics state : rplStatMetrics) {
                cursor.addRow(new Object[] {
                    state.getTaskId(),
                    state.getTaskType(),
                    state.getChannel(),
                    state.getSubChannel(),
                    state.getInEps(),
                    state.getOutRps(),
                    state.getInBps(),
                    state.getOutBps(),
                    state.getOutInsertRps(),
                    state.getOutUpdateRps(),
                    state.getOutDeleteRps(),
                    state.getApplyCount(),
                    state.getReceiveDelay(),
                    state.getProcessDelay(),
                    state.getMergeBatchSize(),
                    state.getRt(),
                    state.getSkipCounter(),
                    state.getSkipExceptionCounter(),
                    state.getPersistMsgCounter(),
                    state.getMsgCacheSize(),
                    state.getCpuUseRatio(),
                    state.getMemUseRatio(),
                    state.getFullGcCount(),
                    state.getWorkerIp(),
                    state.getGmtModified()
                });
            }

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex);
        }

        return cursor;
    }
}
