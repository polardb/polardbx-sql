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
package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.druid.sql.ast.SQLDeclareItem;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByList;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionValue;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLWindow;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLArrayExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCurrentOfCursorExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSequenceExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAnalyzePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableArchivePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableCheckPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableCoalescePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableConvertCharSet;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDiscardPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropForeignKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableEnableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableExchangePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableImportPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableOptimizePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRebuildPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableReorgPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRepairPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSetOption;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableTruncatePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTypeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAnalyzeTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCloseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCommentStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCopyFromStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTriggerStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDefault;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDescribeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropCatalogStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropRoleStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropSynonymStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableSpaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTriggerStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTypeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropUserStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDumpStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExplainStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExportTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExternalRecordFormat;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLFetchStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLForeignKeyImpl;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLImportTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLMergeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLOpenStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRefreshMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTableGroupsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowViewsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSyncMetaStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.prepare.CalciteCatalogReader;

import java.util.Locale;
import java.util.Objects;

public class SQLExprTypeVisitor extends MySqlASTVisitorAdapter {
    protected CalciteCatalogReader catalogReader;

    public SQLExprTypeVisitor(ExecutionContext context) {
        String schemaName = context.getSchemaName();
        if (schemaName == null) {
            schemaName = DefaultSchema.getSchemaName();
        }
        this.catalogReader = RelUtils.buildCatalogReader(schemaName, context);
    }

    @Override
    public boolean visit(SQLDeleteStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLInListExpr x) {
        return false;
    }

    @Override
    public boolean visit(SQLInSubQueryExpr x) {
        return false;
    }

    @Override
    public void endVisit(SQLDeleteStatement x) {

    }

    @Override
    public void endVisit(SQLUpdateStatement x) {
    }

    @Override
    public boolean visit(SQLCreateTableStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLColumnDefinition x) {
        return false;
    }

    @Override
    public boolean visit(SQLCallStatement x) {
        return false;
    }

    @Override
    public void endVisit(SQLCommentStatement x) {

    }

    @Override
    public boolean visit(SQLCommentStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCurrentOfCursorExpr x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAddColumn x) {
        return false;
    }

    @Override
    public void endVisit(SQLAlterTableAddColumn x) {

    }

    @Override
    public boolean visit(SQLRollbackStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropForeignKey x) {
        return false;
    }

    @Override
    public boolean visit(SQLUseStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDisableConstraint x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableEnableConstraint x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropConstraint x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropIndexStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateIndexStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLForeignKeyImpl x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropSequenceStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropTriggerStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropUserStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLGrantStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLRevokeStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropDatabaseStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAddIndex x) {
        return false;
    }

    @Override
    public boolean visit(SQLCheck x) {
        return false;
    }

    @Override
    public boolean visit(SQLDefault x) {
        x.getExpr().accept(this);
        x.getColumn().accept(this);
        return false;
    }

    @Override
    public boolean visit(SQLCreateTriggerStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropFunctionStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropTableSpaceStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropProcedureStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableRename x) {
        return false;
    }

    @Override
    public boolean visit(SQLArrayExpr x) {
        return false;
    }

    @Override
    public boolean visit(SQLOpenStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLFetchStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropMaterializedViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowMaterializedViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLRefreshMaterializedViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCloseStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateProcedureStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateFunctionStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLBlockStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowTablesStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDeclareItem x) {
        return false;
    }

    @Override
    public boolean visit(SQLPartitionByHash x) {
        return false;
    }

    @Override
    public boolean visit(SQLPartitionByRange x) {
        return false;
    }

    @Override
    public boolean visit(SQLPartitionByList x) {
        return false;
    }

    @Override
    public boolean visit(SQLPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLSubPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLSubPartitionByHash x) {
        return false;
    }

    @Override
    public boolean visit(SQLPartitionValue x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterDatabaseStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableConvertCharSet x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableReorgPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableCoalescePartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableTruncatePartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDiscardPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableImportPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAnalyzePartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableCheckPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableOptimizePartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableRebuildPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableRepairPartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLSequenceExpr x) {
        return false;
    }

    @Override
    public boolean visit(SQLMergeStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLSetStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateSequenceStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAddConstraint x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropIndex x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropPrimaryKey x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropKey x) {
        return false;
    }

    @Override
    public boolean visit(SQLDescribeStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLExplainStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateMaterializedViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLReplaceStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterFunctionStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropSynonymStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTypeStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterProcedureStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLExprStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropTypeStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLExternalRecordFormat x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateDatabaseStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateTableGroupStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropTableGroupStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowDatabasesStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowColumnsStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowCreateTableStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowTableGroupsStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableSetOption x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowCreateViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateRoleStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropRoleStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowViewsStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableExchangePartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropCatalogStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLUnionQuery x) {
        return true;
    }

    @Override
    public boolean visit(SQLValuesTableSource x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterIndexStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLShowIndexesStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAnalyzeTableStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLExportTableStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLImportTableStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLCreateOutlineStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDumpStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLDropOutlineStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterOutlineStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableArchivePartition x) {
        return false;
    }

    @Override
    public boolean visit(SQLCopyFromStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLSyncMetaStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLAllColumnExpr x) {
        return false;
    }

    @Override
    public void endVisit(SQLSelectStatement x) {
    }

    protected void accept(SQLObject x) {
        if (x != null) {
            x.accept(this);
        }
    }

    @Override
    public boolean visit(SQLWindow x) {
        SQLOver over = x.getOver();
        if (over != null) {
            visit(over);
        }
        return false;
    }

    public static class Name {
        private final String schema;
        private final String table;

        public Name(String schema, String table) {
            this.schema = schema.toLowerCase(Locale.ROOT);
            this.table = table.toLowerCase(Locale.ROOT);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Name name = (Name) o;
            return Objects.equals(schema, name.schema) && Objects.equals(table, name.table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, table);
        }
    }
}
