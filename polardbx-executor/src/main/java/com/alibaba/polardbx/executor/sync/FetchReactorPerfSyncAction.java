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
import com.alibaba.polardbx.rpc.perf.ReactorPerfItem;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class FetchReactorPerfSyncAction implements ISyncAction {

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();
        String serverInfo =
            TddlNode.getNodeIndex() + ":" + TddlNode.getNodeId() + ":" + TddlNode.getHost() + ":" + TddlNode.getPort();

        List<ReactorPerfItem> items = new ArrayList<>();
        XConnectionManager.getInstance().gatherReactorPerf(items);

        for (ReactorPerfItem item : items) {
            resultCursor.addRow(new Object[] {
                serverInfo, item.getName(),
                item.getSocketCount(),
                item.getEventLoopCount(), item.getRegisterCount(), item.getReadCount(), item.getWriteCount(),
                item.getBufferSize(), item.getBufferChunkSize(), item.getPooledBufferCount()
            });
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("REACTOR_PERF");

        resultCursor.addColumn("CN", DataTypes.StringType);
        resultCursor.addColumn("NAME", DataTypes.StringType);

        resultCursor.addColumn("SOCKET_COUNT", DataTypes.LongType);

        resultCursor.addColumn("EVENT_LOOP_COUNT", DataTypes.LongType);
        resultCursor.addColumn("REGISTER_COUNT", DataTypes.LongType);
        resultCursor.addColumn("READ_COUNT", DataTypes.LongType);
        resultCursor.addColumn("WRITE_COUNT", DataTypes.LongType);

        resultCursor.addColumn("BUFFER_SIZE", DataTypes.LongType);
        resultCursor.addColumn("BUFFER_CHUNK_SIZE", DataTypes.LongType);
        resultCursor.addColumn("POOLED_BUFFER_COUNT", DataTypes.LongType);

        resultCursor.initMeta();

        return resultCursor;
    }

}
