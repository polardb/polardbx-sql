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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

@Getter
@TaskName(name = "CdcDropColumnarIndexTask")
public class CdcDropColumnarIndexTask extends BaseDdlTask {
    private final String logicalTableName;
    private final String indexName;
    private final Long versionId;

    public CdcDropColumnarIndexTask(String schemaName, String logicalTableName, String indexName, Long versionId) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.versionId = versionId;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        SQLDropIndexStatement stmt = new SQLDropIndexStatement();
        stmt.setTableName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(logicalTableName)));
        stmt.setIndexName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(indexName)));

        String markSql = CdcMarkUtil.buildVersionIdHint(versionId) + stmt;
        CdcMarkUtil.useDdlVersionId(executionContext, versionId);
        CdcMarkUtil.useOriginalDDL(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, SqlKind.DROP_INDEX.name(),
                markSql, DdlType.DROP_INDEX, ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Protected, buildExtendParameter(executionContext));
    }
}
