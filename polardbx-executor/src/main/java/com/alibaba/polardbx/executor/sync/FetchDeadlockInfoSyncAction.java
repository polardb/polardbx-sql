package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.GLOBAL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.MDL_DEADLOCK;

/**
 * @author wuzhe
 */
public class FetchDeadlockInfoSyncAction implements ISyncAction {
    /**
     * schema name is only used to check leadership for PolarDB-X 1.0
     */
    private String schemaName;

    public FetchDeadlockInfoSyncAction() {

    }

    public FetchDeadlockInfoSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public ResultCursor sync() {
        // Only leader has the deadlock information
        if (ExecUtils.hasLeadership(schemaName)) {
            final ArrayResultCursor result = new ArrayResultCursor("DEADLOCK INFO");
            result.addColumn("TYPE", DataTypes.StringType);
            result.addColumn("LOG", DataTypes.StringType);
            final String deadlockInfo = StorageInfoManager.getDeadlockInfo();
            final String mdlDeadlockInfo = StorageInfoManager.getMdlDeadlockInfo();
            if (null != deadlockInfo && null != mdlDeadlockInfo) {
                result.addRow(new Object[] {GLOBAL_DEADLOCK, deadlockInfo});
                result.addRow(new Object[] {MDL_DEADLOCK, mdlDeadlockInfo});
                return result;
            }
        }
        return null;
    }
}
