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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.rpc.perf.TcpPerfItem;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class FetchTcpPerfSyncAction implements ISyncAction {

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();
        String serverInfo =
            TddlNode.getNodeIndex() + ":" + TddlNode.getNodeId() + ":" + TddlNode.getHost() + ":" + TddlNode.getPort();

        List<TcpPerfItem> items = new ArrayList<>();
        XConnectionManager.getInstance().gatherTcpPerf(items);

        for (TcpPerfItem item : items) {
            resultCursor.addRow(new Object[] {
                serverInfo, item.getTcpTag(), item.getDnTag(),
                item.getSendMsgCount(), item.getSendFlushCount(), item.getSendSize(),
                item.getRecvMsgCount(), item.getRecvNetCount(), item.getRecvSize(),
                item.getSessionCreateCount(), item.getSessionDropCount(),
                item.getSessionCreateSuccessCount(), item.getSessionCreateFailCount(),
                item.getSessionActiveCount(), item.getSessionCount(),
                item.getClientState().name(), item.getIdleNanosSinceLastPacket() / 1e9d, item.getLiveNanos() / 1e9d,
                item.getFatalError(), item.getAuthId(), item.getNanosSinceLastVariablesFlush() / 1e9d,
                item.getTcpState().name(),
                item.getSocketSendBufferSize(), item.getSocketRecvBufferSize(),
                item.getReadDirectBuffers(), item.getReadHeapBuffers(),
                item.getWriteDirectBuffers(), item.getWriteHeapBuffers(),
                item.isReactorRegistered(), item.isSocketClosed()
            });
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("TCP_PERF");

        resultCursor.addColumn("CN", DataTypes.StringType);
        resultCursor.addColumn("TCP", DataTypes.StringType);
        resultCursor.addColumn("DN", DataTypes.StringType);

        resultCursor.addColumn("SEND_MSG_COUNT", DataTypes.LongType);
        resultCursor.addColumn("SEND_FLUSH_COUNT", DataTypes.LongType);
        resultCursor.addColumn("SEND_SIZE", DataTypes.LongType);
        resultCursor.addColumn("RECV_MSG_COUNT", DataTypes.LongType);
        resultCursor.addColumn("RECV_NET_COUNT", DataTypes.LongType);
        resultCursor.addColumn("RECV_SIZE", DataTypes.LongType);

        resultCursor.addColumn("SESSION_CREATE_COUNT", DataTypes.LongType);
        resultCursor.addColumn("SESSION_DROP_COUNT", DataTypes.LongType);
        resultCursor.addColumn("SESSION_CREATE_SUCCESS_COUNT", DataTypes.LongType);
        resultCursor.addColumn("SESSION_CREATE_FAIL_COUNT", DataTypes.LongType);
        resultCursor.addColumn("DN_CONCURRENT_COUNT", DataTypes.LongType);

        resultCursor.addColumn("SESSION_COUNT", DataTypes.LongType);

        resultCursor.addColumn("CLIENT_STATE", DataTypes.StringType);
        resultCursor.addColumn("TIME_SINCE_LAST_RECV", DataTypes.DoubleType);
        resultCursor.addColumn("LIVE_TIME", DataTypes.DoubleType);
        resultCursor.addColumn("FATAL_ERROR", DataTypes.StringType);
        resultCursor.addColumn("AUTH_ID", DataTypes.LongType);
        resultCursor.addColumn("TIME_SINCE_VARIABLES_REFRESH", DataTypes.DoubleType);

        resultCursor.addColumn("TCP_STATE", DataTypes.StringType);

        resultCursor.addColumn("SOCKET_SEND_BUFFER_SIZE", DataTypes.LongType);
        resultCursor.addColumn("SOCKET_RECV_BUFFER_SIZE", DataTypes.LongType);
        resultCursor.addColumn("READ_DIRECT_BUFFERS", DataTypes.LongType);
        resultCursor.addColumn("READ_HEAP_BUFFERS", DataTypes.LongType);
        resultCursor.addColumn("WRITE_DIRECT_BUFFERS", DataTypes.LongType);
        resultCursor.addColumn("WRITE_HEAP_BUFFERS", DataTypes.LongType);
        resultCursor.addColumn("REACTOR_REGISTERED", DataTypes.BooleanType);
        resultCursor.addColumn("SOCKET_CLOSED", DataTypes.BooleanType);

        resultCursor.initMeta();

        return resultCursor;
    }

}
