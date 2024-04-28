package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.sync.ColumnarSnapshotUpdateSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.SyncUtil;

public class UpdateColumnarTsoTimerTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger("COLUMNAR_TRANS");

    @Override
    public void run() {
        try {
            if (!SyncUtil.isNodeWithSmallestId()) {
                return;
            }

            Long latestTso = ColumnarTransactionUtils.getLatestTsoFromGms();

            logger.warn("update the columnar tso: " + latestTso);

            if (latestTso != null) {
                try {
                    SyncManagerHelper.sync(new ColumnarSnapshotUpdateSyncAction(latestTso),
                        SystemDbHelper.DEFAULT_DB_NAME, SyncScope.CURRENT_ONLY);
                } catch (Throwable t) {
                    logger.error(String.format("Failed to update columnar tso: %d", latestTso), t);
                }
            }
        } catch (Throwable t) {
            logger.error("Columnar tso update task failed unexpectedly!", t);
        }
    }
}
