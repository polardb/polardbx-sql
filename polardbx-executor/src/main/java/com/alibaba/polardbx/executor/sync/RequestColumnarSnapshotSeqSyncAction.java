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
