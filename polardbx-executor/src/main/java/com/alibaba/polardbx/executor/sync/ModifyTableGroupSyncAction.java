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
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.mdl.MdlDuration;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlType;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ModifyTableGroupSyncAction implements ISyncAction {

    private String schemaName;
    private List<String> logicalTableNames;
    private String traceId;
    private long connId;

    public long getConnId() {
        return connId;
    }

    public void setConnId(long connId) {
        this.connId = connId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<String> getLogicalTableNames() {
        return logicalTableNames;
    }

    public void setLogicalTableNames(List<String> logicalTableNames) {
        this.logicalTableNames = logicalTableNames;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    private MdlRequest writeRequest(Long trxId, String logicalTableName) {
        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(schemaName, logicalTableName),
            MdlType.MDL_EXCLUSIVE,
            MdlDuration.MDL_TRANSACTION);
    }

    public ModifyTableGroupSyncAction() {
        logicalTableNames = null;
    }

    public ModifyTableGroupSyncAction(String schemaName, List<String> logicalTableNames, long connId, String traceId) {
        this.schemaName = schemaName;
        this.logicalTableNames = logicalTableNames;
        this.connId = connId;
        this.traceId = traceId;
    }

    @Override
    public ResultCursor sync() {
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();

        if (logicalTableNames == null) {
            return null;
        }
        for (String logicalTableName : logicalTableNames) {
            if (logicalTableName != null) {
                ((GmsTableMetaManager) oldSchemaManager).tonewversion(logicalTableName);
            }
        }
        return null;
    }
}
