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
import com.alibaba.polardbx.rpc.perf.SessionPerfItem;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @version 1.0
 */
public class FetchSessionPerfSyncAction implements ISyncAction {

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();
        String serverInfo =
            TddlNode.getNodeIndex() + ":" + TddlNode.getNodeId() + ":" + TddlNode.getHost() + ":" + TddlNode.getPort();

        List<SessionPerfItem> items = new ArrayList<>();
        XConnectionManager.getInstance().gatherSessionPerf(items);

        for (SessionPerfItem item : items) {
            if (item.getLastRequestStatus() != null) {
                resultCursor.addRow(new Object[] {
                    serverInfo, item.getDnTag(), item.getTcpTag(), item.getSessionId(), item.getStatus().name(),
                    item.getPlanCount(), item.getQueryCount(), item.getUpdateCount(), item.getTsoCount(),
                    item.getLiveNanos() / 1e9d, item.getIdleNanosSinceLastPacket() / 1e9d,
                    item.getCharsetClient(), item.getCharsetResult(), item.getTimezone(), item.getIsolation(),
                    item.isAutoCommit(), item.getVariablesChanged(), item.getLastRequestDB(),
                    item.getQueuedRequestDepth(),
                    item.getLastRequestTraceId(), item.getLastRequestSql(), item.getLastRequestExtra(),
                    item.getLastRequestType(), item.getLastRequestStatus(), item.getLastRequestFetchCount(),
                    item.getLastRequestTokenCount(), item.getLastRequestWorkingNanos() / 1e9d,
                    item.getLastRequestDataPktResponseNanos() > 0 ? item.getLastRequestDataPktResponseNanos() / 1e9d :
                        null,
                    item.getLastRequestResponseNanos() > 0 ? item.getLastRequestResponseNanos() / 1e9d : null,
                    item.getLastRequestFinishNanos() > 0 ? item.getLastRequestFinishNanos() / 1e9d : null,
                    item.getLastRequestTokenDoneCount(), item.getLastRequestActiveOfferTokenCount(),
                    item.getLastRequestStartMillis() > 0 ? (new Date(item.getLastRequestStartMillis())).toString() :
                        null,
                    item.isLastRequestResultChunk(), item.isLastRequestRetrans(), item.isLastRequestGoCache()
                });
            } else {
                resultCursor.addRow(new Object[] {
                    serverInfo, item.getDnTag(), item.getTcpTag(), item.getSessionId(), item.getStatus().name(),
                    item.getPlanCount(), item.getQueryCount(), item.getUpdateCount(), item.getTsoCount(),
                    item.getLiveNanos() / 1e9d, item.getIdleNanosSinceLastPacket() / 1e9d,
                    item.getCharsetClient(), item.getCharsetResult(), item.getTimezone(), item.getIsolation(),
                    item.isAutoCommit(), item.getVariablesChanged(), item.getLastRequestDB(),
                    item.getQueuedRequestDepth(),
                    null, null, null,
                    null, null, null,
                    null, null,
                    null,
                    null,
                    null,
                    null, null,
                    null,
                    null, null, null
                });
            }
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("SESSION_PERF");

        resultCursor.addColumn("CN", DataTypes.StringType);
        resultCursor.addColumn("DN", DataTypes.StringType);
        resultCursor.addColumn("TCP", DataTypes.StringType);
        resultCursor.addColumn("SESSION", DataTypes.LongType);
        resultCursor.addColumn("STATUS", DataTypes.StringType);

        resultCursor.addColumn("PLAN_COUNT", DataTypes.LongType);
        resultCursor.addColumn("QUERY_COUNT", DataTypes.LongType);
        resultCursor.addColumn("UPDATE_COUNT", DataTypes.LongType);
        resultCursor.addColumn("TSO_COUNT", DataTypes.LongType);

        resultCursor.addColumn("LIVE_TIME", DataTypes.DoubleType);
        resultCursor.addColumn("TIME_SINCE_LAST_RECV", DataTypes.DoubleType);

        resultCursor.addColumn("CHARSET_CLIENT", DataTypes.StringType);
        resultCursor.addColumn("CHARSET_RESULT", DataTypes.StringType);
        resultCursor.addColumn("TIMEZONE", DataTypes.StringType);
        resultCursor.addColumn("ISOLATION", DataTypes.StringType);
        resultCursor.addColumn("AUTO_COMMIT", DataTypes.BooleanType);
        resultCursor.addColumn("VARIABLES_CHANGED", DataTypes.StringType);
        resultCursor.addColumn("LAST_DB", DataTypes.StringType);

        resultCursor.addColumn("QUEUED_REQUEST_DEPTH", DataTypes.LongType);

        resultCursor.addColumn("TRACE_ID", DataTypes.StringType);
        resultCursor.addColumn("SQL", DataTypes.StringType);
        resultCursor.addColumn("EXTRA", DataTypes.StringType);
        resultCursor.addColumn("TYPE", DataTypes.StringType);
        resultCursor.addColumn("REQUEST_STATUS", DataTypes.StringType);
        resultCursor.addColumn("FETCH_COUNT", DataTypes.LongType);
        resultCursor.addColumn("TOKEN_COUNT", DataTypes.LongType);
        resultCursor.addColumn("TIME_SINCE_REQUEST", DataTypes.DoubleType);
        resultCursor.addColumn("DATA_PKT_RESPONSE_TIME", DataTypes.DoubleType);
        resultCursor.addColumn("RESPONSE_TIME", DataTypes.DoubleType);
        resultCursor.addColumn("FINISH_TIME", DataTypes.DoubleType);
        resultCursor.addColumn("TOKEN_DONE_COUNT", DataTypes.LongType);
        resultCursor.addColumn("ACTIVE_OFFER_TOKEN_COUNT", DataTypes.LongType);
        resultCursor.addColumn("START_TIME", DataTypes.StringType);
        resultCursor.addColumn("RESULT_CHUNK", DataTypes.BooleanType);
        resultCursor.addColumn("RETRANSMIT", DataTypes.BooleanType);
        resultCursor.addColumn("USE_CACHE", DataTypes.BooleanType);

        resultCursor.initMeta();

        return resultCursor;
    }

}
