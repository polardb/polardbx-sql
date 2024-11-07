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
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * created by wumu
 **/
@TaskName(name = "CdcModifyPartitionKeyMarkTask")
@Getter
@Setter
public class CdcModifyPartitionKeyMarkTask extends BaseCdcTask {
    private String logicalTableName;
    private String indexName;
    private SqlKind sqlKind;
    private Map<String, String> exchangeNamesMapping;
    private long versionId;

    @JSONCreator
    public CdcModifyPartitionKeyMarkTask(String schemaName, String logicalTableName, String indexName,
                                         SqlKind sqlKind, Map<String, String> exchangeNamesMapping, long versionId) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.sqlKind = sqlKind;
        this.exchangeNamesMapping = exchangeNamesMapping;
        this.versionId = versionId;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        mark4RepartitionTable(executionContext);
    }

    private void mark4RepartitionTable(ExecutionContext executionContext) {
        // 主表和目标表之间未完成物理表的Switch操作，目标表以GSI的形式存在，依靠分布式事务，双边数据是强一致的
        // 在交换前进行打标，保证最终一致性
        DdlContext ddlContext = executionContext.getDdlContext();
        Map<String, Object> param = buildExtendParameter(executionContext);
        param.put(ICdcManager.ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG, "");
        param.put(ICdcManager.REFRESH_CREATE_SQL_4_PHY_TABLE, "true");
        param.put(ICdcManager.EXCHANGE_NAMES_MAPPING, exchangeNamesMapping);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        TableMeta indexTableMeta = executionContext.getSchemaManager().getTable(indexName);
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, sqlKind.name(), getDdlStmt(ddlContext.getDdlStmt(), versionId),
                ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(), CdcDdlMarkVisibility.Public, param,
                true, indexTableMeta.getLatestTopology());
    }

    private String getDdlStmt(String ddl, Long versionId) {
        if (CdcMarkUtil.isVersionIdInitialized(versionId)) {
            return CdcMarkUtil.buildVersionIdHint(versionId) + ddl;
        }
        return ddl;
    }
}
