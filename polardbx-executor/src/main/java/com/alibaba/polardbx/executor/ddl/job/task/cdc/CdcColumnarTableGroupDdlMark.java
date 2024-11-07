/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_IS_CCI;
import static com.alibaba.polardbx.common.cdc.ICdcManager.DDL_ID;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

@TaskName(name = "CdcColumnarTableGroupDdlMark")
@Getter
@Setter
@Slf4j
public class CdcColumnarTableGroupDdlMark extends BaseDdlTask {
    private String tableGroup;
    private List<String> tableNames;
    private Long versionId;

    @JSONCreator
    public CdcColumnarTableGroupDdlMark(String tableGroup, String schemaName, List<String> tableNames,
                                        Long versionId) {
        super(schemaName);
        this.tableGroup = tableGroup;
        this.tableNames = tableNames;
        this.versionId = versionId;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        mark4ColumnarTableGroupChange(executionContext);
    }

    public void mark4ColumnarTableGroupChange(ExecutionContext executionContext) {
        for (String tableName : tableNames) {
            ExecutionContext ecCopy = executionContext.copy();
            Map<String, Object> param = buildExtendParameter(ecCopy);

            DdlContext ddlContext = ecCopy.getDdlContext();
            String ddlStmt = ddlContext.getDdlStmt();

            String markTableName = tableName;
            // alter index ... on table ... , 使用主表的名字进行打标
            List<SQLStatement> parseResult = SQLUtils.parseStatements(ddlStmt, DbType.mysql, SQL_PARSE_FEATURES);
            if (!parseResult.isEmpty() && parseResult.get(0) instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement stmt = (SQLAlterTableStatement) parseResult.get(0);
                if (stmt.getAlterIndexName() != null) {
                    markTableName = SQLUtils.normalize(stmt.getTableName());
                    param.put(CDC_IS_CCI, true);
                }
            }

            // set DDL_ID to original ddl
            param.put(DDL_ID, versionId);
            param.put(ICdcManager.CDC_ORIGINAL_DDL, getDdlStmt(ddlStmt));
            CdcManagerHelper.getInstance()
                .notifyDdlNew(schemaName, markTableName, SqlKind.ALTER_TABLE.name(), getDdlStmt(ddlStmt),
                    DdlType.ALTER_TABLEGROUP,
                    ddlContext.getJobId(), getTaskId(), CdcDdlMarkVisibility.Protected, param);
        }
    }

    private String getDdlStmt(String ddl) {
        if (CdcMarkUtil.isVersionIdInitialized(versionId)) {
            return CdcMarkUtil.buildVersionIdHint(versionId) + ddl;
        }
        return ddl;
    }
}
