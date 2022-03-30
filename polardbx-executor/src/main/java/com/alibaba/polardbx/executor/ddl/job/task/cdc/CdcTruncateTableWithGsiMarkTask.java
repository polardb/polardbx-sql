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

package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TruncateUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

@TaskName(name = "CdcTruncateTableWithGsiMarkTask")
@Getter
@Setter
public class CdcTruncateTableWithGsiMarkTask extends BaseDdlTask {
    private String logicalTableName;
    private String tmpTableName;

    @JSONCreator
    public CdcTruncateTableWithGsiMarkTask(String schemaName, String logicalTableName, String tmpTableName) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.tmpTableName = tmpTableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        mark4TruncateTableWithGsi(executionContext);
    }

    private void mark4TruncateTableWithGsi(ExecutionContext executionContext) {
        // 由于目前的 Truncate 实现无法做到一个明确的 Commit Point，所以打标需要发生在元数据切换之前，此时可能漏掉部分写入原表的数据
        DdlContext ddlContext = executionContext.getDdlContext();
        Map<String, Object> params = Maps.newHashMap();

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            String tmpTbNamePattern = TruncateUtil.getTmpTbNamePattern(schemaName, tmpTableName);
            params.put(ICdcManager.TABLE_NEW_PATTERN, tmpTbNamePattern);
            params.putAll(executionContext.getExtraCmds());
            CdcManagerHelper.getInstance()
                    .notifyDdlNew(schemaName, logicalTableName, SqlKind.TRUNCATE_TABLE.name(), ddlContext.getDdlStmt(),
                            ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(), DdlVisibility.Public, params,
                            true, Maps.newHashMap());
        } else {
            Map<String, Set<String>> tmpTableTopology = TruncateUtil.getTmpTableTopology(schemaName, tmpTableName);
            CdcManagerHelper.getInstance()
                    .notifyDdlNew(schemaName, logicalTableName, SqlKind.TRUNCATE_TABLE.name(), ddlContext.getDdlStmt(),
                            ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(), DdlVisibility.Public,
                            executionContext.getExtraCmds(), true, tmpTableTopology);
        }
    }
}
