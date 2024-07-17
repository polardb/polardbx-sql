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
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

@TaskName(name = "CdcDropTableIfExistsMarkTask")
@Getter
@Setter
@Slf4j
public class CdcDropTableIfExistsMarkTask extends BaseDdlTask {

    private String tableName;

    @JSONCreator
    public CdcDropTableIfExistsMarkTask(String schemaName, String tableName) {
        super(schemaName);
        this.tableName = tableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        boolean tableExists = TableValidator.checkIfTableExists(schemaName, tableName);

        //如果表不存在，说明和prepare阶段的判断结果是一致的，正常打标即可
        //如表表存在，说明和prepare阶段的判断结果是不一致的，则不能打标，否则会引发线性一致性问题，如：
        //表t1并不存在，线程1执行drop，线程2执行create t1 ，线程1发现表不存在准备打标，但在打标前，线程2先执行成功，随后线程1才完成打标
        //同步到下游的顺序则为 create -> drop，最终的结果是上游存在t1，但下游不存在t1
        if (!tableExists) {
            checkTableName(ddlContext.getDdlStmt());
            // 需要考虑历史兼容问题
            // 历史上cdc下游依据job_id是否为空来判断是否需要对打标sql进行apply，如果不为空则进行apply，如果为空则不进行apply
            // 所以此处需要继续保持job_id为空，来解决兼容性问题。否则，当只升级CN、没有升级CDC时，老版本的CDC无法识别是真实建表，还是单纯打标，会触发问题
            CdcManagerHelper.getInstance().notifyDdlNew(schemaName, tableName, SqlKind.DROP_TABLE.name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), null, getTaskId(),
                CdcDdlMarkVisibility.Public, buildExtendParameter(executionContext));
        } else {
            log.warn("table {} is currently present, cdc ddl mark for drop table with if exits is ignored, sql {}",
                tableName, ddlContext.getDdlStmt());
        }
    }

    public static void checkTableName(String sql) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLDropTableStatement dropTableStatement = (SQLDropTableStatement) statementList.get(0);
        for (SQLExprTableSource tableSource : dropTableStatement.getTableSources()) {
            SQLExpr sqlName = tableSource.getExpr();
            if (sqlName instanceof SQLPropertyExpr) {
                SQLExpr owner = ((SQLPropertyExpr) sqlName).getOwner();
                if (owner instanceof SQLPropertyExpr) {
                    throw new RuntimeException("duplicate schema name in drop table sql.");
                }
            }
        }
    }
}
