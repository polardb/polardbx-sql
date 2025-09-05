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

package com.alibaba.polardbx.transaction.sync;

import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.util.Iterator;
import java.util.Map;

public class RequestSnapshotSeqSyncAction implements IGmsSyncAction {

    public RequestSnapshotSeqSyncAction() {
    }

    @Override
    public Object sync() {
        Long minSnapshotTime = null;
        Map<String, ExecutorContext> executorContextMap = ExecutorContext.getExecutorContextMap();
        for (Iterator<Map.Entry<String, ExecutorContext>> iterator =
             executorContextMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, ExecutorContext> entry = iterator.next();
            ITransactionManager manager = entry.getValue().getTransactionManager();
            if (manager instanceof TransactionManager) {
                if (minSnapshotTime == null) {
                    minSnapshotTime = ((TransactionManager) manager).getMinSnapshotSeq();
                } else {
                    minSnapshotTime = Math.min(minSnapshotTime, ((TransactionManager) manager).getMinSnapshotSeq());
                }

            }
        }
        //return
        ArrayResultCursor resultCursor = buildResultCursor();
        resultCursor.addRow(new Object[] {minSnapshotTime});
        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("Request Snapshot");
        resultCursor.addColumn("TSO", DataTypes.LongType);
        resultCursor.initMeta();
        return resultCursor;
    }
}
