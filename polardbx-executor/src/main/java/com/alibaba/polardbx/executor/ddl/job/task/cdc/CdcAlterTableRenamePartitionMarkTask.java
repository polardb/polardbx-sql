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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_IS_GSI;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-28 18:52
 **/
@TaskName(name = "CdcAlterTableRenamePartitionMarkTask")
@Getter
@Setter
@Slf4j
public class CdcAlterTableRenamePartitionMarkTask extends BaseDdlTask {

    private final String tableName;
    private final boolean placeHolder;

    @JSONCreator
    public CdcAlterTableRenamePartitionMarkTask(String schemaName, String tableName, boolean placeHolder) {
        super(schemaName);
        this.tableName = tableName;
        this.placeHolder = placeHolder;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        if (placeHolder) {
            return;
        }

        DdlContext ddlContext = executionContext.getDdlContext();
        Map<String, Object> param = buildExtendParameter(executionContext);

        // alter index ... on table ... , 使用主表的名字进行打标
        String markTableName = tableName;
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(ddlContext.getDdlStmt()));
        List<SQLStatement> parseResult = parser.parseStatementList();
        if (!parseResult.isEmpty() && parseResult.get(0) instanceof SQLAlterTableStatement) {
            SQLAlterTableStatement stmt = (SQLAlterTableStatement) parseResult.get(0);
            if (stmt.getAlterIndexName() != null) {
                markTableName = SQLUtils.normalize(stmt.getTableName());
                param.put(CDC_IS_GSI, true);
            }
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(
                schemaName,
                markTableName,
                SqlKind.ALTER_TABLE.name(),
                ddlContext.getDdlStmt(),
                ddlContext.getDdlType(),
                ddlContext.getJobId(),
                getTaskId(),
                CdcDdlMarkVisibility.Protected,
                param);
    }

}
