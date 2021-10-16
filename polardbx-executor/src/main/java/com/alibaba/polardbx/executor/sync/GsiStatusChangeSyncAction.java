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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.mdl.MdlDuration;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlType;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

public class GsiStatusChangeSyncAction implements ISyncAction {
    protected final static Logger logger = LoggerFactory.getLogger(GsiStatusChangeSyncAction.class);

    private String schemaName;
    private String primaryTableName;
    private String gsiTableName;
    private String traceId;

    // This will not conflict, because it is unique in clusters(ClusterAcceptIdGenerator).
    private long connId;

    public GsiStatusChangeSyncAction() {

    }

    public GsiStatusChangeSyncAction(String schemaName, String primaryTableName, String gsiTableName, long connId,
                                     String traceId) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.gsiTableName = gsiTableName;
        this.connId = connId;
        this.traceId = traceId;
    }

    private MdlRequest writeRequest(Long trxId) {
        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(schemaName, primaryTableName),
            MdlType.MDL_EXCLUSIVE,
            MdlDuration.MDL_TRANSACTION);
    }

    @Override
    public ResultCursor sync() {
        syncForPolarDbX();
        return null;
    }

    private void syncForPolarDbX() {
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        ((GmsTableMetaManager) oldSchemaManager).tonewversion(primaryTableName);
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

    public String getGsiTableName() {
        return gsiTableName;
    }

    public void setGsiTableName(String gsiTableName) {
        this.gsiTableName = gsiTableName;
    }
}
