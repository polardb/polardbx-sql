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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.sync.ColumnarSnapshotUpdateSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
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
            Long latestTso;
            int tsoUpdateDelay = InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_DELAY);
            if (tsoUpdateDelay > 0) {
                latestTso = ColumnarTransactionUtils.getLatestTsoFromGmsWithDelay(
                    1000L * tsoUpdateDelay // convert milliseconds to microseconds
                );
            } else {
                latestTso = ColumnarTransactionUtils.getLatestTsoFromGms();
            }
            logger.warn("update the columnar tso: " + latestTso);

            if (latestTso != null) {
                try {
                    ColumnarManager.getInstance().setLatestTso(latestTso);
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
