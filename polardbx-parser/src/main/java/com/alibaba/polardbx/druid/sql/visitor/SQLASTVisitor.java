/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.visitor;

import com.alibaba.polardbx.druid.sql.ast.SQLAdhocTableSource;
import com.alibaba.polardbx.druid.sql.ast.SQLAnnIndex;
import com.alibaba.polardbx.druid.sql.ast.SQLArgument;
import com.alibaba.polardbx.druid.sql.ast.SQLArrayDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentUserExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeRefExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLDeclareItem;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexOptions;
import com.alibaba.polardbx.druid.sql.ast.SQLKeep;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLMapDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByList;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByValue;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionValue;
import com.alibaba.polardbx.druid.sql.ast.SQLRecordDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLRowDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLStructDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByList;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLUnionDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLWindow;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLArrayExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBigIntExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCaseStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLContainsExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCurrentOfCursorExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDateTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDbLinkExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDecimalExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDoubleExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExtractExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLFlashbackExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLFloatExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLGroupingSetExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLJSONExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMatchAgainstExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNotExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLRealExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSequenceExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSizeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSmallIntExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSomeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTinyIntExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLValuesExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableAllocateLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableExpireLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsExtractHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsMergePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsMovePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsRenamePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupReorgPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetPartitionsLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterCharacter;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterJoinGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSystemGetConfigStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSystemRefreshStorageStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSystemReloadStorageStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSystemSetConfigStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddClusteringKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddExtPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddSupplemental;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAlterColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAnalyzePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableArchivePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableBlockSize;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableCheckPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableCoalescePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableCompression;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableConvertCharSet;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDeleteByCondition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableKeys;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableLifecycle;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDiscardPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropClusteringKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropExtPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropFile;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropForeignKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropSubpartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableEnableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableEnableKeys;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableEnableLifecycle;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableExchangePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupAddTable;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableImportPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableModifyClusteredBy;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableModifyPartitionValues;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableOptimizePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTablePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTablePartitionCount;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTablePartitionLifecycle;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTablePartitionSetProperties;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableReOrganizePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRebuildPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRecoverPartitions;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenamePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRepairPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableReplaceColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSetComment;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSetLifecycle;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSetOption;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSubpartitionAvailablePartitionNum;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSubpartitionLifecycle;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableTouch;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableTruncatePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableUnarchivePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTypeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterViewRenameStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAnalyzeTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLArchiveTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBackupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBeginStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBuildTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCancelJobStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLChangeRoleStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCloseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCommentStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCopyFromStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateJoinGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTriggerStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateUserStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeclareStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDefault;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDescribeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropCatalogStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropEventStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropJoinGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropLogFileGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropResourceGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropResourceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropRoleStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropServerStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropSynonymStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableSpaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTriggerStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTypeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropUserStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDumpStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLErrorLoggingClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExplainAnalyzeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExplainStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExportDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExportTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprHint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExternalRecordFormat;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLFetchStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLForStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLForeignKeyImpl;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLImportDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLImportTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLLateralViewTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLLoopStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLMergeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLMergeTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLOpenStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPartitionRef;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPrimaryKeyImpl;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPrivilegeItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPurgeLogsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPurgeRecyclebinStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPurgeTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRebalanceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRefreshMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRenameUserStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRestoreStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReturnStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSavePointStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLScriptCommitStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCatalogsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowErrorsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowFunctionsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowOutlinesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowPackagesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowPartitionsStmt;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowProcessListStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowQueryTaskStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowRecyclebinStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowSessionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatisticListStmt;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatisticStmt;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTableGroupsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowUsersStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowViewsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubmitJobStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSyncMetaStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableLike;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSampling;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnique;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnnestTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWhileStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWhoamiStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRefreshTopology;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowStorage;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeMasterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeReplicationFilterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlResetSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStartSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStopSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.SQLAlterResourceGroupStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.SQLAlterTableAddRoute;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.SQLCreateResourceGroupStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.SQLListResourceGroupStatement;

public interface SQLASTVisitor {

    void endVisit(SQLAllColumnExpr x);

    void endVisit(SQLBetweenExpr x);

    void endVisit(SQLBinaryOpExpr x);

    void endVisit(SQLCaseExpr x);

    void endVisit(SQLCaseExpr.Item x);

    void endVisit(SQLCaseStatement x);

    void endVisit(SQLCaseStatement.Item x);

    void endVisit(SQLCharExpr x);

    void endVisit(SQLIdentifierExpr x);

    void endVisit(SQLInListExpr x);

    void endVisit(SQLIntegerExpr x);

    void endVisit(SQLSmallIntExpr x);

    void endVisit(SQLBigIntExpr x);

    void endVisit(SQLTinyIntExpr x);

    void endVisit(SQLExistsExpr x);

    void endVisit(SQLNCharExpr x);

    void endVisit(SQLNotExpr x);

    void endVisit(SQLNullExpr x);

    void endVisit(SQLNumberExpr x);

    void endVisit(SQLRealExpr x);

    void endVisit(SQLPropertyExpr x);

    void endVisit(SQLSelectGroupByClause x);

    void endVisit(SQLSelectItem x);

    void endVisit(SQLSelectStatement selectStatement);

    void postVisit(SQLObject x);

    void preVisit(SQLObject x);

    boolean visit(SQLAllColumnExpr x);

    boolean visit(SQLBetweenExpr x);

    boolean visit(SQLBinaryOpExpr x);

    boolean visit(SQLCaseExpr x);

    boolean visit(SQLCaseExpr.Item x);

    boolean visit(SQLCaseStatement x);

    boolean visit(SQLCaseStatement.Item x);

    boolean visit(SQLCastExpr x);

    boolean visit(SQLCharExpr x);

    boolean visit(SQLExistsExpr x);

    boolean visit(SQLIdentifierExpr x);

    boolean visit(SQLInListExpr x);

    boolean visit(SQLIntegerExpr x);

    boolean visit(SQLSmallIntExpr x);

    boolean visit(SQLBigIntExpr x);

    boolean visit(SQLTinyIntExpr x);

    boolean visit(SQLNCharExpr x);

    boolean visit(SQLNotExpr x);

    boolean visit(SQLNullExpr x);

    boolean visit(SQLNumberExpr x);

    boolean visit(SQLRealExpr x);

    boolean visit(SQLPropertyExpr x);

    boolean visit(SQLSelectGroupByClause x);

    boolean visit(SQLSelectItem x);

    void endVisit(SQLCastExpr x);

    boolean visit(SQLSelectStatement astNode);

    void endVisit(SQLAggregateExpr astNode);

    boolean visit(SQLAggregateExpr astNode);

    boolean visit(SQLVariantRefExpr x);

    void endVisit(SQLVariantRefExpr x);

    boolean visit(SQLQueryExpr x);

    void endVisit(SQLQueryExpr x);

    boolean visit(SQLUnaryExpr x);

    void endVisit(SQLUnaryExpr x);

    boolean visit(SQLHexExpr x);

    void endVisit(SQLHexExpr x);

    boolean visit(SQLSelect x);

    void endVisit(SQLSelect select);

    boolean visit(SQLSelectQueryBlock x);

    void endVisit(SQLSelectQueryBlock x);

    boolean visit(SQLExprTableSource x);

    void endVisit(SQLExprTableSource x);

    boolean visit(SQLOrderBy x);

    void endVisit(SQLOrderBy x);

    boolean visit(SQLSelectOrderByItem x);

    void endVisit(SQLSelectOrderByItem x);

    boolean visit(SQLDropTableStatement x);

    void endVisit(SQLDropTableStatement x);

    boolean visit(SQLCreateTableStatement x);

    void endVisit(SQLCreateTableStatement x);

    boolean visit(SQLColumnDefinition x);

    void endVisit(SQLColumnDefinition x);

    boolean visit(SQLColumnDefinition.Identity x);

    void endVisit(SQLColumnDefinition.Identity x);

    boolean visit(SQLDataType x);

    void endVisit(SQLDataType x);

    boolean visit(SQLCharacterDataType x);

    void endVisit(SQLCharacterDataType x);

    boolean visit(SQLDeleteStatement x);

    void endVisit(SQLDeleteStatement x);

    boolean visit(SQLCurrentOfCursorExpr x);

    void endVisit(SQLCurrentOfCursorExpr x);

    boolean visit(SQLInsertStatement x);

    void endVisit(SQLInsertStatement x);

    boolean visit(SQLInsertStatement.ValuesClause x);

    void endVisit(SQLInsertStatement.ValuesClause x);

    boolean visit(SQLUpdateSetItem x);

    void endVisit(SQLUpdateSetItem x);

    boolean visit(SQLUpdateStatement x);

    void endVisit(SQLUpdateStatement x);

    boolean visit(SQLCreateViewStatement x);

    void endVisit(SQLCreateViewStatement x);

    boolean visit(SQLCreateViewStatement.Column x);

    void endVisit(SQLCreateViewStatement.Column x);

    boolean visit(SQLNotNullConstraint x);

    void endVisit(SQLNotNullConstraint x);

    void endVisit(SQLMethodInvokeExpr x);

    boolean visit(SQLMethodInvokeExpr x);

    void endVisit(SQLUnionQuery x);

    boolean visit(SQLUnionQuery x);

    void endVisit(SQLSetStatement x);

    boolean visit(SQLSetStatement x);

    void endVisit(SQLAssignItem x);

    boolean visit(SQLAssignItem x);

    void endVisit(SQLCallStatement x);

    boolean visit(SQLCallStatement x);

    void endVisit(SQLJoinTableSource x);

    boolean visit(SQLJoinTableSource x);

    void endVisit(SQLJoinTableSource.UDJ x);

    boolean visit(SQLJoinTableSource.UDJ x);

    void endVisit(SQLSomeExpr x);

    boolean visit(SQLSomeExpr x);

    void endVisit(SQLAnyExpr x);

    boolean visit(SQLAnyExpr x);

    void endVisit(SQLAllExpr x);

    boolean visit(SQLAllExpr x);

    void endVisit(SQLInSubQueryExpr x);

    boolean visit(SQLInSubQueryExpr x);

    void endVisit(SQLListExpr x);

    boolean visit(SQLListExpr x);

    void endVisit(SQLSubqueryTableSource x);

    boolean visit(SQLSubqueryTableSource x);

    void endVisit(SQLTruncateStatement x);

    boolean visit(SQLTruncateStatement x);

    void endVisit(SQLDefaultExpr x);

    boolean visit(SQLDefaultExpr x);

    void endVisit(SQLCommentStatement x);

    boolean visit(SQLCommentStatement x);

    void endVisit(SQLUseStatement x);

    boolean visit(SQLUseStatement x);

    boolean visit(SQLAlterTableAddColumn x);

    void endVisit(SQLAlterTableAddColumn x);

    boolean visit(SQLAlterTableAddRoute x);

    void endVisit(SQLAlterTableAddRoute x);

    boolean visit(SQLAlterTableDeleteByCondition x);

    void endVisit(SQLAlterTableDeleteByCondition x);

    boolean visit(SQLAlterTableModifyClusteredBy x);

    void endVisit(SQLAlterTableModifyClusteredBy x);

    boolean visit(SQLAlterTableDropColumnItem x);

    void endVisit(SQLAlterTableDropColumnItem x);

    boolean visit(SQLAlterTableDropIndex x);

    void endVisit(SQLAlterTableDropIndex x);

    boolean visit(SQLAlterTableGroupStatement x);

    void endVisit(SQLAlterTableGroupStatement x);

    boolean visit(SQLAlterSystemSetConfigStatement x);

    void endVisit(SQLAlterSystemSetConfigStatement x);

    boolean visit(SQLAlterSystemGetConfigStatement x);

    void endVisit(SQLAlterSystemGetConfigStatement x);

    boolean visit(SQLChangeRoleStatement x);

    void endVisit(SQLChangeRoleStatement x);

    boolean visit(SQLAlterSystemRefreshStorageStatement x);

    void endVisit(SQLAlterSystemRefreshStorageStatement x);

    boolean visit(SQLAlterSystemReloadStorageStatement x);

    void endVisit(SQLAlterSystemReloadStorageStatement x);

    boolean visit(SQLDropIndexStatement x);

    void endVisit(SQLDropIndexStatement x);

    boolean visit(SQLDropViewStatement x);

    void endVisit(SQLDropViewStatement x);

    boolean visit(SQLSavePointStatement x);

    void endVisit(SQLSavePointStatement x);

    boolean visit(SQLRollbackStatement x);

    void endVisit(SQLRollbackStatement x);

    boolean visit(SQLReleaseSavePointStatement x);

    void endVisit(SQLReleaseSavePointStatement x);

    void endVisit(SQLCommentHint x);

    boolean visit(SQLCommentHint x);

    void endVisit(SQLCreateDatabaseStatement x);

    boolean visit(SQLCreateDatabaseStatement x);

    void endVisit(SQLOver x);

    boolean visit(SQLOver x);

    void endVisit(SQLKeep x);

    boolean visit(SQLKeep x);

    void endVisit(SQLColumnPrimaryKey x);

    boolean visit(SQLColumnPrimaryKey x);

    boolean visit(SQLColumnUniqueKey x);

    void endVisit(SQLColumnUniqueKey x);

    void endVisit(SQLWithSubqueryClause x);

    boolean visit(SQLWithSubqueryClause x);

    void endVisit(SQLWithSubqueryClause.Entry x);

    boolean visit(SQLWithSubqueryClause.Entry x);

    void endVisit(SQLAlterTableAlterColumn x);

    boolean visit(SQLAlterTableAlterColumn x);

    boolean visit(SQLCheck x);

    void endVisit(SQLCheck x);

    boolean visit(SQLDefault x);

    void endVisit(SQLDefault x);

    boolean visit(SQLAlterTableDropForeignKey x);

    void endVisit(SQLAlterTableDropForeignKey x);

    boolean visit(SQLAlterTableDropPrimaryKey x);

    void endVisit(SQLAlterTableDropPrimaryKey x);

    boolean visit(SQLAlterTableDisableKeys x);

    void endVisit(SQLAlterTableDisableKeys x);

    boolean visit(SQLAlterTableEnableKeys x);

    void endVisit(SQLAlterTableEnableKeys x);

    boolean visit(SQLAlterTableStatement x);

    void endVisit(SQLAlterTableStatement x);

    boolean visit(SQLAlterTableDisableConstraint x);

    void endVisit(SQLAlterTableDisableConstraint x);

    boolean visit(SQLAlterTableEnableConstraint x);

    void endVisit(SQLAlterTableEnableConstraint x);

    boolean visit(SQLColumnCheck x);

    void endVisit(SQLColumnCheck x);

    boolean visit(SQLExprHint x);

    void endVisit(SQLExprHint x);

    boolean visit(SQLAlterTableDropConstraint x);

    void endVisit(SQLAlterTableDropConstraint x);

    boolean visit(SQLUnique x);

    void endVisit(SQLUnique x);

    boolean visit(SQLPrimaryKeyImpl x);

    void endVisit(SQLPrimaryKeyImpl x);

    boolean visit(SQLCreateIndexStatement x);

    void endVisit(SQLCreateIndexStatement x);

    boolean visit(SQLAlterTableRenameColumn x);

    void endVisit(SQLAlterTableRenameColumn x);

    boolean visit(SQLColumnReference x);

    void endVisit(SQLColumnReference x);

    boolean visit(SQLForeignKeyImpl x);

    void endVisit(SQLForeignKeyImpl x);

    boolean visit(SQLDropSequenceStatement x);

    void endVisit(SQLDropSequenceStatement x);

    boolean visit(SQLDropTriggerStatement x);

    void endVisit(SQLDropTriggerStatement x);

    void endVisit(SQLDropUserStatement x);

    boolean visit(SQLDropUserStatement x);

    void endVisit(SQLExplainStatement x);

    boolean visit(SQLExplainStatement x);

    void endVisit(SQLGrantStatement x);

    boolean visit(SQLGrantStatement x);

    void endVisit(SQLDropDatabaseStatement x);

    boolean visit(SQLDropDatabaseStatement x);

    void endVisit(SQLIndexOptions x);

    boolean visit(SQLIndexOptions x);

    void endVisit(SQLIndexDefinition x);

    boolean visit(SQLIndexDefinition x);

    void endVisit(SQLAlterTableAddIndex x);

    boolean visit(SQLAlterTableAddIndex x);

    void endVisit(SQLAlterTableAddConstraint x);

    boolean visit(SQLAlterTableAddConstraint x);

    void endVisit(SQLCreateTriggerStatement x);

    boolean visit(SQLCreateTriggerStatement x);

    void endVisit(SQLDropFunctionStatement x);

    boolean visit(SQLDropFunctionStatement x);

    void endVisit(SQLDropTableSpaceStatement x);

    boolean visit(SQLDropTableSpaceStatement x);

    void endVisit(SQLDropProcedureStatement x);

    boolean visit(SQLDropProcedureStatement x);

    void endVisit(SQLBooleanExpr x);

    boolean visit(SQLBooleanExpr x);

    void endVisit(SQLUnionQueryTableSource x);

    boolean visit(SQLUnionQueryTableSource x);

    void endVisit(SQLTimestampExpr x);

    boolean visit(SQLTimestampExpr x);

    void endVisit(SQLDateTimeExpr x);

    boolean visit(SQLDateTimeExpr x);

    void endVisit(SQLDoubleExpr x);

    boolean visit(SQLDoubleExpr x);

    void endVisit(SQLFloatExpr x);

    boolean visit(SQLFloatExpr x);

    void endVisit(SQLRevokeStatement x);

    boolean visit(SQLRevokeStatement x);

    void endVisit(SQLBinaryExpr x);

    boolean visit(SQLBinaryExpr x);

    void endVisit(SQLAlterTableRename x);

    boolean visit(SQLAlterTableRename x);

    void endVisit(SQLAlterViewRenameStatement x);

    boolean visit(SQLAlterViewRenameStatement x);

    void endVisit(SQLShowTablesStatement x);

    boolean visit(SQLShowTablesStatement x);

    void endVisit(SQLAlterTableAddPartition x);

    boolean visit(SQLAlterTableAddPartition x);

    void endVisit(SQLAlterTableAddExtPartition x);

    boolean visit(SQLAlterTableAddExtPartition x);

    void endVisit(SQLAlterTableDropExtPartition x);

    boolean visit(SQLAlterTableDropExtPartition x);

    void endVisit(SQLAlterTableDropPartition x);

    boolean visit(SQLAlterTableDropPartition x);

    void endVisit(SQLAlterTableRenamePartition x);

    boolean visit(SQLAlterTableRenamePartition x);

    void endVisit(SQLAlterTableSetComment x);

    boolean visit(SQLAlterTableSetComment x);

    void endVisit(SQLAlterTableGroupAddTable x);

    boolean visit(SQLAlterTableGroupAddTable x);

    void endVisit(SQLAlterTableSetLifecycle x);

    boolean visit(SQLPrivilegeItem x);

    void endVisit(SQLPrivilegeItem x);

    boolean visit(SQLAlterTableSetLifecycle x);

    void endVisit(SQLAlterTableEnableLifecycle x);

    boolean visit(SQLAlterTableEnableLifecycle x);

    void endVisit(SQLAlterTablePartition x);

    boolean visit(SQLAlterTablePartition x);

    void endVisit(SQLAlterTablePartitionSetProperties x);

    boolean visit(SQLAlterTablePartitionSetProperties x);

    void endVisit(SQLAlterTableDisableLifecycle x);

    boolean visit(SQLAlterTableDisableLifecycle x);

    void endVisit(SQLAlterTableTouch x);

    boolean visit(SQLAlterTableTouch x);

    void endVisit(SQLArrayExpr x);

    boolean visit(SQLArrayExpr x);

    void endVisit(SQLOpenStatement x);

    boolean visit(SQLOpenStatement x);

    void endVisit(SQLFetchStatement x);

    boolean visit(SQLFetchStatement x);

    void endVisit(SQLCloseStatement x);

    boolean visit(SQLCloseStatement x);

    boolean visit(SQLGroupingSetExpr x);

    void endVisit(SQLGroupingSetExpr x);

    boolean visit(SQLIfStatement x);

    void endVisit(SQLIfStatement x);

    boolean visit(SQLIfStatement.ElseIf x);

    void endVisit(SQLIfStatement.ElseIf x);

    boolean visit(SQLIfStatement.Else x);

    void endVisit(SQLIfStatement.Else x);

    boolean visit(SQLLoopStatement x);

    void endVisit(SQLLoopStatement x);

    boolean visit(SQLParameter x);

    void endVisit(SQLParameter x);

    boolean visit(SQLCreateProcedureStatement x);

    void endVisit(SQLCreateProcedureStatement x);

    boolean visit(SQLCreateFunctionStatement x);

    void endVisit(SQLCreateFunctionStatement x);

    boolean visit(SQLBeginStatement x);

    void endVisit(SQLBeginStatement x);

    boolean visit(SQLBlockStatement x);

    void endVisit(SQLBlockStatement x);

    boolean visit(SQLAlterTableDropKey x);

    void endVisit(SQLAlterTableDropKey x);

    boolean visit(SQLDeclareItem x);

    void endVisit(SQLDeclareItem x);

    boolean visit(SQLPartitionValue x);

    void endVisit(SQLPartitionValue x);

    boolean visit(SQLPartition x);

    void endVisit(SQLPartition x);

    boolean visit(SQLPartitionByRange x);

    void endVisit(SQLPartitionByRange x);

    boolean visit(SQLPartitionByHash x);

    void endVisit(SQLPartitionByHash x);

    boolean visit(SQLPartitionByList x);

    void endVisit(SQLPartitionByList x);

    boolean visit(SQLSubPartition x);

    void endVisit(SQLSubPartition x);

    boolean visit(SQLSubPartitionByHash x);

    void endVisit(SQLSubPartitionByHash x);

    boolean visit(SQLSubPartitionByRange x);

    void endVisit(SQLSubPartitionByRange x);

    boolean visit(SQLSubPartitionByList x);

    void endVisit(SQLSubPartitionByList x);

    boolean visit(SQLAlterDatabaseStatement x);

    void endVisit(SQLAlterDatabaseStatement x);

    boolean visit(SQLAlterTableConvertCharSet x);

    void endVisit(SQLAlterTableConvertCharSet x);

    boolean visit(SQLAlterTableReOrganizePartition x);

    void endVisit(SQLAlterTableReOrganizePartition x);

    boolean visit(SQLAlterTableCoalescePartition x);

    void endVisit(SQLAlterTableCoalescePartition x);

    boolean visit(SQLAlterTableTruncatePartition x);

    void endVisit(SQLAlterTableTruncatePartition x);

    boolean visit(SQLAlterTableDiscardPartition x);

    void endVisit(SQLAlterTableDiscardPartition x);

    boolean visit(SQLAlterTableImportPartition x);

    void endVisit(SQLAlterTableImportPartition x);

    boolean visit(SQLAlterTableAnalyzePartition x);

    void endVisit(SQLAlterTableAnalyzePartition x);

    boolean visit(SQLAlterTableCheckPartition x);

    void endVisit(SQLAlterTableCheckPartition x);

    boolean visit(SQLAlterTableOptimizePartition x);

    void endVisit(SQLAlterTableOptimizePartition x);

    boolean visit(SQLAlterTableRebuildPartition x);

    void endVisit(SQLAlterTableRebuildPartition x);

    boolean visit(SQLAlterTableRepairPartition x);

    void endVisit(SQLAlterTableRepairPartition x);

    boolean visit(SQLSequenceExpr x);

    void endVisit(SQLSequenceExpr x);

    boolean visit(SQLMergeStatement x);

    void endVisit(SQLMergeStatement x);

    boolean visit(SQLMergeStatement.MergeUpdateClause x);

    void endVisit(SQLMergeStatement.MergeUpdateClause x);

    boolean visit(SQLMergeStatement.MergeInsertClause x);

    void endVisit(SQLMergeStatement.MergeInsertClause x);

    boolean visit(SQLErrorLoggingClause x);

    void endVisit(SQLErrorLoggingClause x);

    boolean visit(SQLNullConstraint x);

    void endVisit(SQLNullConstraint x);

    boolean visit(SQLCreateSequenceStatement x);

    void endVisit(SQLCreateSequenceStatement x);

    boolean visit(SQLDateExpr x);

    void endVisit(SQLDateExpr x);

    boolean visit(SQLLimit x);

    void endVisit(SQLLimit x);

    void endVisit(SQLStartTransactionStatement x);

    boolean visit(SQLStartTransactionStatement x);

    boolean visit(MySqlChangeMasterStatement x);

    void endVisit(MySqlChangeMasterStatement x);

    boolean visit(MySqlStartSlaveStatement x);

    void endVisit(MySqlStartSlaveStatement x);

    boolean visit(MySqlStopSlaveStatement x);

    void endVisit(MySqlStopSlaveStatement x);

    boolean visit(MySqlResetSlaveStatement x);

    void endVisit(MySqlResetSlaveStatement x);

    boolean visit(MySqlChangeReplicationFilterStatement x);

    void endVisit(MySqlChangeReplicationFilterStatement x);

    void endVisit(SQLDescribeStatement x);

    boolean visit(SQLDescribeStatement x);

    /**
     * support procedure
     */
    boolean visit(SQLWhileStatement x);

    void endVisit(SQLWhileStatement x);

    boolean visit(SQLDeclareStatement x);

    void endVisit(SQLDeclareStatement x);

    boolean visit(SQLReturnStatement x);

    void endVisit(SQLReturnStatement x);

    boolean visit(SQLArgument x);

    void endVisit(SQLArgument x);

    boolean visit(SQLCommitStatement x);

    void endVisit(SQLCommitStatement x);

    boolean visit(SQLFlashbackExpr x);

    void endVisit(SQLFlashbackExpr x);

    boolean visit(SQLCreateMaterializedViewStatement x);

    void endVisit(SQLCreateMaterializedViewStatement x);

    boolean visit(SQLShowCreateMaterializedViewStatement x);

    void endVisit(SQLShowCreateMaterializedViewStatement x);

    boolean visit(SQLBinaryOpExprGroup x);

    void endVisit(SQLBinaryOpExprGroup x);

    boolean visit(SQLScriptCommitStatement x);

    void endVisit(SQLScriptCommitStatement x);

    boolean visit(SQLReplaceStatement x);

    void endVisit(SQLReplaceStatement x);

    boolean visit(SQLCreateUserStatement x);

    void endVisit(SQLCreateUserStatement x);

    boolean visit(SQLAlterFunctionStatement x);

    void endVisit(SQLAlterFunctionStatement x);

    boolean visit(SQLAlterTypeStatement x);

    void endVisit(SQLAlterTypeStatement x);

    boolean visit(SQLIntervalExpr x);

    void endVisit(SQLIntervalExpr x);

    boolean visit(SQLLateralViewTableSource x);

    void endVisit(SQLLateralViewTableSource x);

    boolean visit(SQLShowErrorsStatement x);

    void endVisit(SQLShowErrorsStatement x);

    boolean visit(SQLShowGrantsStatement x);

    void endVisit(SQLShowGrantsStatement x);

    boolean visit(SQLShowPackagesStatement x);

    void endVisit(SQLShowPackagesStatement x);

    boolean visit(SQLShowRecyclebinStatement x);

    void endVisit(SQLShowRecyclebinStatement x);

    boolean visit(SQLAlterCharacter x);

    void endVisit(SQLAlterCharacter x);

    boolean visit(SQLExprStatement x);

    void endVisit(SQLExprStatement x);

    boolean visit(SQLAlterProcedureStatement x);

    void endVisit(SQLAlterProcedureStatement x);

    boolean visit(SQLAlterViewStatement x);

    void endVisit(SQLAlterViewStatement x);

    boolean visit(SQLDropEventStatement x);

    void endVisit(SQLDropEventStatement x);

    boolean visit(SQLDropLogFileGroupStatement x);

    void endVisit(SQLDropLogFileGroupStatement x);

    boolean visit(SQLDropServerStatement x);

    void endVisit(SQLDropServerStatement x);

    boolean visit(SQLDropSynonymStatement x);

    void endVisit(SQLDropSynonymStatement x);

    boolean visit(SQLRecordDataType x);

    void endVisit(SQLRecordDataType x);

    boolean visit(SQLDropTypeStatement x);

    void endVisit(SQLDropTypeStatement x);

    boolean visit(SQLExternalRecordFormat x);

    void endVisit(SQLExternalRecordFormat x);

    boolean visit(SQLArrayDataType x);

    void endVisit(SQLArrayDataType x);

    boolean visit(SQLMapDataType x);

    void endVisit(SQLMapDataType x);

    boolean visit(SQLStructDataType x);

    void endVisit(SQLStructDataType x);

    boolean visit(SQLRowDataType x);

    void endVisit(SQLRowDataType x);

    boolean visit(SQLStructDataType.Field x);

    void endVisit(SQLStructDataType.Field x);

    boolean visit(SQLDropMaterializedViewStatement x);

    void endVisit(SQLDropMaterializedViewStatement x);

    boolean visit(SQLShowMaterializedViewStatement x);

    void endVisit(SQLShowMaterializedViewStatement x);

    boolean visit(SQLRefreshMaterializedViewStatement x);

    void endVisit(SQLRefreshMaterializedViewStatement x);

    boolean visit(SQLAlterMaterializedViewStatement x);

    void endVisit(SQLAlterMaterializedViewStatement x);

    boolean visit(SQLCreateTableGroupStatement x);

    void endVisit(SQLCreateTableGroupStatement x);

    boolean visit(SQLDropTableGroupStatement x);

    void endVisit(SQLDropTableGroupStatement x);

    boolean visit(SQLAlterTableSubpartitionAvailablePartitionNum x);

    void endVisit(SQLAlterTableSubpartitionAvailablePartitionNum x);

    void endVisit(SQLShowDatabasesStatement x);

    boolean visit(SQLShowDatabasesStatement x);

    void endVisit(DrdsShowStorage x);

    boolean visit(DrdsShowStorage x);

    void endVisit(SQLShowTableGroupsStatement x);

    boolean visit(SQLShowTableGroupsStatement x);

    void endVisit(SQLShowColumnsStatement x);

    boolean visit(SQLShowColumnsStatement x);

    void endVisit(SQLShowCreateTableStatement x);

    boolean visit(SQLShowCreateTableStatement x);

    void endVisit(SQLShowCreateDatabaseStatement x);

    boolean visit(SQLShowCreateDatabaseStatement x);

    void endVisit(SQLShowProcessListStatement x);

    boolean visit(SQLShowProcessListStatement x);

    void endVisit(SQLAlterTableSetOption x);

    boolean visit(SQLAlterTableSetOption x);

    boolean visit(SQLShowCreateViewStatement x);

    void endVisit(SQLShowCreateViewStatement x);

    boolean visit(SQLShowViewsStatement x);

    void endVisit(SQLShowViewsStatement x);

    boolean visit(SQLAlterTableRenameIndex x);

    void endVisit(SQLAlterTableRenameIndex x);

    boolean visit(SQLAlterSequenceStatement x);

    void endVisit(SQLAlterSequenceStatement x);

    boolean visit(SQLAlterTableExchangePartition x);

    void endVisit(SQLAlterTableExchangePartition x);

    boolean visit(SQLCreateRoleStatement x);

    void endVisit(SQLCreateRoleStatement x);

    boolean visit(SQLDropRoleStatement x);

    void endVisit(SQLDropRoleStatement x);

    boolean visit(SQLAlterTableReplaceColumn x);

    void endVisit(SQLAlterTableReplaceColumn x);

    boolean visit(SQLMatchAgainstExpr x);

    void endVisit(SQLMatchAgainstExpr x);

    boolean visit(SQLTimeExpr x);

    void endVisit(SQLTimeExpr x);

    boolean visit(SQLDropCatalogStatement x);

    void endVisit(SQLDropCatalogStatement x);

    void endVisit(SQLShowPartitionsStmt x);

    boolean visit(SQLShowPartitionsStmt x);

    void endVisit(SQLValuesExpr x);

    boolean visit(SQLValuesExpr x);

    void endVisit(SQLContainsExpr x);

    boolean visit(SQLContainsExpr x);

    void endVisit(SQLDumpStatement x);

    boolean visit(SQLDumpStatement x);

    void endVisit(SQLValuesTableSource x);

    boolean visit(SQLValuesTableSource x);

    void endVisit(SQLExtractExpr x);

    boolean visit(SQLExtractExpr x);

    void endVisit(SQLWindow x);

    boolean visit(SQLWindow x);

    void endVisit(SQLJSONExpr x);

    boolean visit(SQLJSONExpr x);

    void endVisit(SQLDecimalExpr x);

    boolean visit(SQLDecimalExpr x);

    void endVisit(SQLAnnIndex x);

    boolean visit(SQLAnnIndex x);

    void endVisit(SQLUnionDataType x);

    boolean visit(SQLUnionDataType x);

    void endVisit(SQLAlterTableRecoverPartitions x);

    boolean visit(SQLAlterTableRecoverPartitions x);

    void endVisit(SQLAlterIndexStatement x);

    boolean visit(SQLAlterIndexStatement x);

    boolean visit(SQLAlterIndexStatement.Rebuild x);

    void endVisit(SQLAlterIndexStatement.Rebuild x);

    boolean visit(SQLShowIndexesStatement x);

    void endVisit(SQLShowIndexesStatement x);

    boolean visit(SQLAnalyzeTableStatement x);

    void endVisit(SQLAnalyzeTableStatement x);

    boolean visit(SQLExportTableStatement x);

    void endVisit(SQLExportTableStatement x);

    boolean visit(SQLImportTableStatement x);

    void endVisit(SQLImportTableStatement x);

    boolean visit(SQLTableSampling x);

    void endVisit(SQLTableSampling x);

    boolean visit(SQLSizeExpr x);

    void endVisit(SQLSizeExpr x);

    boolean visit(SQLAlterTableArchivePartition x);

    void endVisit(SQLAlterTableArchivePartition x);

    boolean visit(SQLAlterTableUnarchivePartition x);

    void endVisit(SQLAlterTableUnarchivePartition x);

    boolean visit(SQLCreateOutlineStatement x);

    void endVisit(SQLCreateOutlineStatement x);

    boolean visit(SQLDropOutlineStatement x);

    void endVisit(SQLDropOutlineStatement x);

    boolean visit(SQLAlterOutlineStatement x);

    void endVisit(SQLAlterOutlineStatement x);

    boolean visit(SQLShowOutlinesStatement x);

    void endVisit(SQLShowOutlinesStatement x);

    boolean visit(SQLPurgeTableStatement x);

    void endVisit(SQLPurgeTableStatement x);

    boolean visit(SQLPurgeLogsStatement x);

    void endVisit(SQLPurgeLogsStatement x);

    boolean visit(SQLPurgeRecyclebinStatement x);

    void endVisit(SQLPurgeRecyclebinStatement x);

    boolean visit(SQLShowStatisticStmt x);

    void endVisit(SQLShowStatisticStmt x);

    boolean visit(SQLShowStatisticListStmt x);

    void endVisit(SQLShowStatisticListStmt x);

    boolean visit(SQLAlterTableAddSupplemental x);

    void endVisit(SQLAlterTableAddSupplemental x);

    boolean visit(SQLShowCatalogsStatement x);

    void endVisit(SQLShowCatalogsStatement x);

    boolean visit(SQLShowFunctionsStatement x);

    void endVisit(SQLShowFunctionsStatement x);

    boolean visit(SQLShowSessionStatement x);

    void endVisit(SQLShowSessionStatement x);

    boolean visit(SQLDbLinkExpr x);

    void endVisit(SQLDbLinkExpr x);

    boolean visit(SQLCurrentTimeExpr x);

    void endVisit(SQLCurrentTimeExpr x);

    boolean visit(SQLCurrentUserExpr x);

    void endVisit(SQLCurrentUserExpr x);

    boolean visit(SQLShowQueryTaskStatement x);

    void endVisit(SQLShowQueryTaskStatement x);

    boolean visit(SQLAdhocTableSource x);

    void endVisit(SQLAdhocTableSource x);

    boolean visit(SQLExplainAnalyzeStatement x);

    void endVisit(SQLExplainAnalyzeStatement x);

    boolean visit(SQLPartitionRef x);

    void endVisit(SQLPartitionRef x);

    boolean visit(SQLPartitionRef.Item x);

    void endVisit(SQLPartitionRef.Item x);

    boolean visit(SQLWhoamiStatement x);

    void endVisit(SQLWhoamiStatement x);

    boolean visit(SQLDropResourceStatement x);

    void endVisit(SQLDropResourceStatement x);

    boolean visit(SQLForStatement x);

    void endVisit(SQLForStatement x);

    boolean visit(SQLUnnestTableSource x);

    void endVisit(SQLUnnestTableSource x);

    boolean visit(SQLCopyFromStatement x);

    void endVisit(SQLCopyFromStatement x);

    boolean visit(SQLShowUsersStatement x);

    void endVisit(SQLShowUsersStatement x);

    boolean visit(SQLSubmitJobStatement x);

    void endVisit(SQLSubmitJobStatement x);

    boolean visit(SQLTableLike x);

    void endVisit(SQLTableLike x);

    boolean visit(SQLSyncMetaStatement x);

    void endVisit(SQLSyncMetaStatement x);

    void endVisit(SQLValuesQuery x);

    boolean visit(SQLValuesQuery x);

    void endVisit(SQLDataTypeRefExpr x);

    boolean visit(SQLDataTypeRefExpr x);

    void endVisit(SQLArchiveTableStatement x);

    boolean visit(SQLArchiveTableStatement x);

    void endVisit(SQLBackupStatement x);

    boolean visit(SQLBackupStatement x);

    void endVisit(SQLRestoreStatement x);

    boolean visit(SQLRestoreStatement x);

    void endVisit(SQLBuildTableStatement x);

    boolean visit(SQLBuildTableStatement x);

    void endVisit(SQLCancelJobStatement x);

    boolean visit(SQLCancelJobStatement x);

    void endVisit(SQLExportDatabaseStatement x);

    boolean visit(SQLExportDatabaseStatement x);

    void endVisit(SQLImportDatabaseStatement x);

    boolean visit(SQLImportDatabaseStatement x);

    void endVisit(SQLRenameUserStatement x);

    boolean visit(SQLRenameUserStatement x);

    void endVisit(SQLPartitionByValue x);

    boolean visit(SQLPartitionByValue x);

    void endVisit(SQLAlterTablePartitionCount x);

    boolean visit(SQLAlterTablePartitionCount x);

    void endVisit(SQLAlterTableBlockSize x);

    boolean visit(SQLAlterTableBlockSize x);

    void endVisit(SQLAlterTableCompression x);

    boolean visit(SQLAlterTableCompression x);

    void endVisit(SQLAlterTablePartitionLifecycle x);

    boolean visit(SQLAlterTablePartitionLifecycle x);

    void endVisit(SQLAlterTableSubpartitionLifecycle x);

    boolean visit(SQLAlterTableSubpartitionLifecycle x);

    void endVisit(SQLAlterTableDropSubpartition x);

    boolean visit(SQLAlterTableDropSubpartition x);

    void endVisit(SQLAlterTableDropClusteringKey x);

    boolean visit(SQLAlterTableDropClusteringKey x);

    void endVisit(SQLAlterTableAddClusteringKey x);

    boolean visit(SQLAlterTableAddClusteringKey x);

    void endVisit(MySqlKillStatement x);

    boolean visit(MySqlKillStatement x);

    boolean visit(SQLCreateResourceGroupStatement x);

    void endVisit(SQLCreateResourceGroupStatement x);

    boolean visit(SQLAlterResourceGroupStatement x);

    void endVisit(SQLAlterResourceGroupStatement x);

    void endVisit(SQLDropResourceGroupStatement x);

    boolean visit(SQLDropResourceGroupStatement x);

    void endVisit(SQLListResourceGroupStatement x);

    boolean visit(SQLListResourceGroupStatement x);

    void endVisit(SQLAlterTableDropCheck x);

    boolean visit(SQLAlterTableDropCheck x);

    void endVisit(DrdsSplitPartition x);

    boolean visit(DrdsSplitPartition x);

    void endVisit(DrdsMergePartition x);

    boolean visit(DrdsMergePartition x);

    void endVisit(DrdsMovePartition x);

    boolean visit(DrdsMovePartition x);

    void endVisit(DrdsExtractHotKey x);

    boolean visit(DrdsExtractHotKey x);

    void endVisit(DrdsSplitHotKey x);

    boolean visit(DrdsSplitHotKey x);

    void endVisit(DrdsAlterTableGroupReorgPartition x);

    boolean visit(DrdsAlterTableGroupReorgPartition x);

    void endVisit(SQLAlterTableModifyPartitionValues x);

    boolean visit(SQLAlterTableModifyPartitionValues x);

    void endVisit(DrdsRenamePartition x);

    boolean visit(DrdsRenamePartition x);

    void endVisit(DrdsAlterTableGroupSetLocality x);

    boolean visit(DrdsAlterTableGroupSetLocality x);

    void endVisit(DrdsAlterTableGroupSetPartitionsLocality x);

    boolean visit(DrdsAlterTableGroupSetPartitionsLocality x);

    void endVisit(DrdsAlterTableAllocateLocalPartition x);

    boolean visit(DrdsAlterTableAllocateLocalPartition x);

    void endVisit(DrdsAlterTableExpireLocalPartition x);

    boolean visit(DrdsAlterTableExpireLocalPartition x);

    void endVisit(DrdsRefreshTopology x);

    boolean visit(DrdsRefreshTopology x);

    void endVisit(SQLRebalanceStatement x);

    boolean visit(SQLRebalanceStatement x);

    void endVisit(SQLAlterTableDropFile x);

    boolean visit(SQLAlterTableDropFile x);

    boolean visit(SQLCreateJoinGroupStatement x);

    void endVisit(SQLCreateJoinGroupStatement x);

    boolean visit(SQLDropJoinGroupStatement x);

    void endVisit(SQLDropJoinGroupStatement x);

    boolean visit(SQLAlterJoinGroupStatement x);

    void endVisit(SQLAlterJoinGroupStatement x);

    boolean visit(SQLMergeTableGroupStatement x);

    void endVisit(SQLMergeTableGroupStatement x);

}
