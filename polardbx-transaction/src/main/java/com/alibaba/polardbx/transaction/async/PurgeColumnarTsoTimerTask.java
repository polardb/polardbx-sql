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

            logger.warn("Start to fetch columnar purge tso");

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

            try {
                int updateRows = ColumnarTransactionUtils.updateColumnarPurgeWatermark(minSnapshotTime);
                if (updateRows > 0) {
                    logger.warn(String.format("Update columnar purge watermark: %d", minSnapshotTime));
                } else {
                    logger.warn("Columnar purge watermark remains the same");
                }
            } catch (Throwable t) {
                logger.error(String.format("Failed to update columnar purge watermark: %d", minSnapshotTime), t);
            }
        } catch (Throwable t) {
            logger.error("Columnar tso purge task failed unexpectedly!", t);
        }
    }
}
