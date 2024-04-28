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
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
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
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter.unwrapGsiName;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-28 18:52
 **/
@TaskName(name = "CdcAlterTableSetTableGroupMarkTask")
@Getter
@Setter
public class CdcAlterTableSetTableGroupMarkTask extends BaseDdlTask {

    private final String primaryTableName;
    private final String gsiTableName;
    private final boolean gsi;

    @JSONCreator
    public CdcAlterTableSetTableGroupMarkTask(String schemaName, String primaryTableName, String gsiTableName,
                                              boolean gsi) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.gsiTableName = gsiTableName;
        this.gsi = gsi;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        String ddl = ddlContext.getDdlStmt();
        if (gsi) {
            executionContext.getExtraCmds().put(ICdcManager.CDC_IS_GSI, true);
            ddl = tryRewriteTableName(ddl);
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(
                schemaName,
                primaryTableName,
                SqlKind.ALTER_TABLE_SET_TABLEGROUP.name(),
                ddl,
                ddlContext.getDdlType(),
                ddlContext.getJobId(),
                getTaskId(),
                CdcDdlMarkVisibility.Protected,
                buildExtendParameter(executionContext));

    }

    private String tryRewriteTableName(String ddl) {
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(ddl));
        List<SQLStatement> parseResult = parser.parseStatementList();
        if (!parseResult.isEmpty() && parseResult.get(0) instanceof SQLAlterTableStatement) {
            SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) parseResult.get(0);
            String tableName = SQLUtils.normalize(alterTableStatement.getTableName());
            if (StringUtils.equalsIgnoreCase(gsiTableName, tableName)) {
                String newTableName = primaryTableName + "." + unwrapGsiName(gsiTableName);
                alterTableStatement.setName(new SQLIdentifierExpr(newTableName));
                return SQLUtils.toSQLString(alterTableStatement, DbType.mysql, new SQLUtils.FormatOption(true, false));
            }
        }
        return ddl;
    }
}
