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
import com.alibaba.polardbx.rpc.perf.DnPerfItem;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class FetchDnPerfSyncAction implements ISyncAction {

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();
        String serverInfo = TddlNode.getNodeIndex() + ":" + TddlNode.getNodeId() + ":" + TddlNode.getHost();

        List<DnPerfItem> items = new ArrayList<>();
        XConnectionManager.getInstance().gatherDnPerf(items);

        for (DnPerfItem item : items) {
            resultCursor.addRow(new Object[] {
                serverInfo, item.getDnTag(),
                item.getSendMsgCount(), item.getSendFlushCount(), item.getSendSize(),
                item.getRecvMsgCount(), item.getRecvNetCount(), item.getRecvSize(),
                item.getSessionCreateCount(), item.getSessionDropCount(),
                item.getSessionCreateSuccessCount(), item.getSessionCreateFailCount(),
                item.getSessionActiveCount(), item.getGetConnectionCount(),
                item.getSendMsgRate(), item.getSendFlushRate(), item.getSendRate(),
                item.getRecvMsgRate(), item.getRecvNetRate(), item.getRecvRate(),
                item.getSessionCreateRate(), item.getSessionDropRate(),
                item.getSessionCreateSuccessRate(), item.getSessionCreateFailRate(),
                item.getGatherTimeDelta(),
                item.getTcpTotalCount(), item.getTcpAgingCount(), item.getSessionCount(), item.getSessionIdleCount(),
                item.getRefCount()
            });
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("DN_PERF");

        resultCursor.addColumn("CN", DataTypes.StringType);
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
        resultCursor.addColumn("WAIT_CONNECTION_COUNT", DataTypes.LongType);

        resultCursor.addColumn("SEND_MSG_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("SEND_FLUSH_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("SEND_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("RECV_MSG_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("RECV_NET_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("RECV_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("SESSION_CREATE_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("SESSION_DROP_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("SESSION_CREATE_SUCCESS_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("SESSION_CREATE_FAIL_RATE", DataTypes.DoubleType);
        resultCursor.addColumn("TIME_DELTA", DataTypes.DoubleType);

        resultCursor.addColumn("TCP_COUNT", DataTypes.LongType);
        resultCursor.addColumn("AGING_TCP_COUNT", DataTypes.LongType);
        resultCursor.addColumn("SESSION_COUNT", DataTypes.LongType);
        resultCursor.addColumn("SESSION_IDLE_COUNT", DataTypes.LongType);

        resultCursor.addColumn("REF_COUNT", DataTypes.LongType);

        resultCursor.initMeta();

        return resultCursor;
    }

}
