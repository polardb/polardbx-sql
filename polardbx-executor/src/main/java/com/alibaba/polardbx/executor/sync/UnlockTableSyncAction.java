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

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.text.MessageFormat;

public class UnlockTableSyncAction implements ISyncAction {

    protected String schemaName;
    protected String primaryTableName;

    // This will not conflict, because it is unique in clusters(ClusterAcceptIdGenerator).
    protected long connId;

    protected String traceId;

    public UnlockTableSyncAction(String schemaName, String primaryTableName, long connId, String traceId) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.connId = connId;
        this.traceId = traceId;
    }

    @Override
    public ResultCursor sync() {
        syncForLockTable();
        return null;
    }

    protected void syncForLockTable() {
        synchronized (OptimizerContext.getContext(schemaName)) {
            final MdlContext context = MdlManager.addContext(connId);
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "{0}  {1}.addContext({2}) (UNLOCK)", Thread.currentThread().getName(),
                this.hashCode(), connId));

            context.releaseAllTransactionalLocks();
            MdlManager.removeContext(context);

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[Mdl write lock release table[{0}]]",
                primaryTableName));

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "{0}  {1}.removeContext({2}) (UNLOCK)", Thread.currentThread().getName(),
                this.hashCode(), connId));
        }
    }

    public String getTraceId() {
        return this.traceId;
    }

    public void setTraceId(final String traceId) {
        this.traceId = traceId;
    }

    public long getConnId() {
        return this.connId;
    }

    public void setConnId(final long connId) {
        this.connId = connId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }
}
