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
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_IS_GSI;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

/**
 * created by ziyang.lb
 **/
@TaskName(name = "CdcTableGroupDdlMarkTask")
@Getter
@Setter
@Slf4j
public class CdcTableGroupDdlMarkTask extends BaseCdcTask {

    private String tableGroup;
    private String tableName;
    private SqlKind sqlKind;
    private Map<String, Set<String>> targetTableTopology;
    private String ddlStmt;
    private CdcDdlMarkVisibility cdcDdlMarkVisibility;
    private boolean isColumnarIndex;

    @JSONCreator
    public CdcTableGroupDdlMarkTask(String tableGroup, String schemaName, String tableName, SqlKind sqlKind,
                                    Map<String, Set<String>> targetTableTopology, String ddlStmt,
                                    CdcDdlMarkVisibility cdcDdlMarkVisibility, boolean isColumnarIndex) {
        super(schemaName);
        this.tableGroup = tableGroup;
        this.tableName = tableName;
        this.sqlKind = sqlKind;
        this.targetTableTopology = targetTableTopology;

        this.ddlStmt = ddlStmt;
        this.cdcDdlMarkVisibility = cdcDdlMarkVisibility;
        this.isColumnarIndex = isColumnarIndex;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        if (!isColumnarIndex) {
            mark4TableGroupChange(executionContext);
        }
    }

    private void mark4TableGroupChange(ExecutionContext executionContext) {
        Map<String, Object> param = buildExtendParameter(executionContext);

        boolean isAlterIndex = false;
        String markTableName = tableName;
        // alter index ... on table ... , 使用主表的名字进行打标
        List<SQLStatement> parseResult = SQLUtils.parseStatements(ddlStmt, DbType.mysql, SQL_PARSE_FEATURES);
        if (!parseResult.isEmpty() && parseResult.get(0) instanceof SQLAlterTableStatement) {
            SQLAlterTableStatement stmt = (SQLAlterTableStatement) parseResult.get(0);
            if (stmt.getAlterIndexName() != null) {
                isAlterIndex = true;
                markTableName = SQLUtils.normalize(stmt.getTableName());
                param.put(CDC_IS_GSI, true);
            }
        }

        if (!isAlterIndex &&
            (TableGroupNameUtil.isOssTg(tableGroup) || (CBOUtil.isGsi(schemaName, markTableName)))) {
            return;
        }

        log.info("new topology for table {} is {}, isAlterIndex {}", markTableName, targetTableTopology, isAlterIndex);
        DdlContext ddlContext = executionContext.getDdlContext();

        CdcManagerHelper.getInstance().notifyDdlNew(schemaName, markTableName, sqlKind.name(), ddlStmt,
            DdlType.ALTER_TABLEGROUP, ddlContext.getJobId(), getTaskId(), cdcDdlMarkVisibility, param, true,
            targetTableTopology);
    }
}
