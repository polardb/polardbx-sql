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
            if (SystemDbHelper.isDBBuildInExceptCdc(entry.getKey())) {
                continue;
            }
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
