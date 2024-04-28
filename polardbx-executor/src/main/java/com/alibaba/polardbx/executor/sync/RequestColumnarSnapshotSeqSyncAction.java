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

import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Iterator;
import java.util.Map;

public class RequestColumnarSnapshotSeqSyncAction implements IGmsSyncAction {
    @Override
    public Object sync() {
        long minSnapshotTime = ColumnarManager.getInstance().latestTso();
        Map<String, ExecutorContext> executorContextMap = ExecutorContext.getExecutorContextMap();
        for (Map.Entry<String, ExecutorContext> entry : executorContextMap.entrySet()) {
            if (SystemDbHelper.isDBBuildInExceptCdc(entry.getKey())) {
                continue;
            }
            ITransactionManager manager = entry.getValue().getTransactionManager();
            long minTsoOfCurrentDb = manager.getColumnarMinSnapshotSeq();
            minSnapshotTime = Math.min(minSnapshotTime, minTsoOfCurrentDb);
        }
        // Notice that minSnapshotTime may be null if there are no DBs
        ArrayResultCursor resultCursor = buildResultCursor();
        resultCursor.addRow(new Object[] {minSnapshotTime});
        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("Request Columnar Snapshot");
        resultCursor.addColumn("TSO", DataTypes.LongType);
        resultCursor.initMeta();
        return resultCursor;
    }
}
