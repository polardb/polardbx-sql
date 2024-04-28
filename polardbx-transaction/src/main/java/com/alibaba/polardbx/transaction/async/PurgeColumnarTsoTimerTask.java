package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.sync.ColumnarMinSnapshotPurgeSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

public class PurgeColumnarTsoTimerTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger("COLUMNAR_TRANS");

    @Override
    public void run() {
        try {
            // NOTICE：This task will do sync twice: collect tso and sync min tso
            // Columnar purge will run in leader node of primary PolarDB-X instance, rather than readonly instance
            if (!ExecUtils.hasLeadership(null)) {
                return;
            }

            Long minSnapshotTime;
            try {
                minSnapshotTime = ColumnarTransactionUtils.getMinColumnarSnapshotTime();
            } catch (Throwable t) {
                logger.error("Failed to get min columnar tso!", t);
                return;
            }

            logger.warn("get the columnar purge tso: " + minSnapshotTime);

            try {
                SyncManagerHelper.sync(new ColumnarMinSnapshotPurgeSyncAction(minSnapshotTime),
                    SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);

            } catch (Throwable t) {
                logger.error(String.format("Failed to purge columnar tso: %d", minSnapshotTime), t);
            }
        } catch (Throwable t) {
            logger.error("Columnar tso purge task failed unexpectedly!", t);
        }
    }
}
