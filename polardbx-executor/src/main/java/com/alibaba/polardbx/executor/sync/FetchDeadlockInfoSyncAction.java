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
