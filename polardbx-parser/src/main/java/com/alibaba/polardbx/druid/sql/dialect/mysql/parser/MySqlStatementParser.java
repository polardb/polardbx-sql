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
package com.alibaba.polardbx.druid.sql.dialect.mysql.parser;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentUserExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLDeclareItem;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter.ParameterType;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableAllocateLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableExpireLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsExtractHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsMergePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsMovePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsRenamePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupReorgPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetPartitionsLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetPartitionsLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.MySQLRotateInnodbMasterKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterCharacter;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterJoinGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSystemGetConfigStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSystemSetConfigStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddExtPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAnalyzePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableBlockSize;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableCheckPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableCoalescePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableConvertCharSet;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableKeys;
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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableExchangePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupAddTable;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableImportPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableModifyClusteredBy;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableModifyPartitionValues;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableOptimizePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTablePartitionCount;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTablePartitionLifecycle;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableReOrganizePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRebuildPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRepairPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSetOption;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSubpartitionAvailablePartitionNum;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSubpartitionLifecycle;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableTruncatePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLArchiveTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBackupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBeginStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBuildTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCancelJobStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLChangeRoleStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCopyFromStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateOutlineStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExportDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExportTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLImportDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLImportTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLLoopStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRebalanceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRenameUserStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRestoreStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCatalogsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowFunctionsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowOutlinesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowPartitionsStmt;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowQueryTaskStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowRecyclebinStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowSessionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatisticStmt;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTableGroupsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowUsersStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubmitJobStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSyncMetaStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWhileStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SqlDataAccess;
import com.alibaba.polardbx.druid.sql.ast.statement.SqlSecurity;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.AnalyzerIndexType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.FullTextType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.ConditionValue;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.ConditionValue.ConditionType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement.MySqlWhenStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCursorDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareConditionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareHandlerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlHandlerType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlIterateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlLeaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlSelectIntoStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.CobarShowStatus;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.CreateFileStorageStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlignToTableGroup;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterFileStorageStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTableAsOfTimeStamp;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTableBroadcast;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTablePartition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTablePurgeBeforeTimeStamp;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTableSingle;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsBaselineStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCancelDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsChangeDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsChangeRuleVersionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCheckGlobalIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsClearCclRulesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsClearCclTriggersStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsClearDDLJobCache;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsClearSeqCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsContinueDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsConvertAllSequencesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsContinueScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsConvertAllSequencesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsFireScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsGenericDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectDDLJobCache;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectGroupSeqRangeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectRuleVersionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsMoveDataBase;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPushDownUdfStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRecoverDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRefreshLocalRulesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRemoveDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRollbackDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowDDLJobs;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowDDLResults;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowGlobalDeadlocks;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowGlobalIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowLocalDeadlocks;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowLocality;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowMetadataLock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowMoveDatabaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowRebalanceBackFill;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowScheduleResultStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTableGroup;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTransStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsUnArchiveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySql8ShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterDatabaseKillJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterDatabaseSetOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterEventStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterInstanceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterLogFileGroupStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterServerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAlterColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAlterFullTextIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableCheckConstraint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableDiscardTablespace;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableForce;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableImportTablespace;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableLock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOrderBy;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableValidation;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTablespaceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlBinlogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeMasterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeReplicationFilterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCheckTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChecksumTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlClearPartitionsHeatmapCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlClearPlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateAddLogFileGroupStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateEventStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateServerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableSpaceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDisabledPlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlEventSchedule;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExecuteForAdsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExtPartition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlFlashbackStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlGrantRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHelpStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement.LockType;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.EOFParserException;
import com.alibaba.polardbx.druid.sql.parser.InsertColumnsCache;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Lexer.SavePoint;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLSelectParser;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.druid.sql.parser.Token.ALL;
import static com.alibaba.polardbx.druid.sql.parser.Token.BINARY;
import static com.alibaba.polardbx.druid.sql.parser.Token.COLUMN;
import static com.alibaba.polardbx.druid.sql.parser.Token.COMMA;
import static com.alibaba.polardbx.druid.sql.parser.Token.CONTINUE;
import static com.alibaba.polardbx.druid.sql.parser.Token.DATABASE;
import static com.alibaba.polardbx.druid.sql.parser.Token.DEFAULT;
import static com.alibaba.polardbx.druid.sql.parser.Token.DROP;
import static com.alibaba.polardbx.druid.sql.parser.Token.EQ;
import static com.alibaba.polardbx.druid.sql.parser.Token.EXCEPT;
import static com.alibaba.polardbx.druid.sql.parser.Token.FALSE;
import static com.alibaba.polardbx.druid.sql.parser.Token.FOR;
import static com.alibaba.polardbx.druid.sql.parser.Token.FROM;
import static com.alibaba.polardbx.druid.sql.parser.Token.GRANT;
import static com.alibaba.polardbx.druid.sql.parser.Token.GROUP;
import static com.alibaba.polardbx.druid.sql.parser.Token.IDENTIFIER;
import static com.alibaba.polardbx.druid.sql.parser.Token.LITERAL_CHARS;
import static com.alibaba.polardbx.druid.sql.parser.Token.LPAREN;
import static com.alibaba.polardbx.druid.sql.parser.Token.ON;
import static com.alibaba.polardbx.druid.sql.parser.Token.PARTITION;
import static com.alibaba.polardbx.druid.sql.parser.Token.PROCEDURE;
import static com.alibaba.polardbx.druid.sql.parser.Token.REVOKE;
import static com.alibaba.polardbx.druid.sql.parser.Token.ROLE;
import static com.alibaba.polardbx.druid.sql.parser.Token.RPAREN;
import static com.alibaba.polardbx.druid.sql.parser.Token.TABLE;
import static com.alibaba.polardbx.druid.sql.parser.Token.TO;
import static com.alibaba.polardbx.druid.sql.parser.Token.TRUE;
import static com.alibaba.polardbx.druid.sql.parser.Token.VIEW;
import static com.alibaba.polardbx.druid.sql.parser.Token.WHERE;
import static com.alibaba.polardbx.druid.sql.parser.Token.WITH;

public class MySqlStatementParser extends SQLStatementParser {

    private static final String AUTO_INCREMENT = "AUTO_INCREMENT";
    private static final String AVG_ROW_LENGTH = "AVG_ROW_LENGTH";
    private static final String CHECKSUM2 = "CHECKSUM";
    private static final String DELAY_KEY_WRITE = "DELAY_KEY_WRITE";
    private static final String ENCRYPTION2 = "ENCRYPTION";
    private static final String INSERT_METHOD = "INSERT_METHOD";
    private static final String KEY_BLOCK_SIZE2 = "KEY_BLOCK_SIZE";
    private static final String MAX_ROWS2 = "MAX_ROWS";
    private static final String MIN_ROWS2 = "MIN_ROWS";
    private static final String PASSWORD2 = "PASSWORD";
    private static final String STATS_AUTO_RECALC = "STATS_AUTO_RECALC";
    private static final String STATS_PERSISTENT = "STATS_PERSISTENT";
    private static final String STATS_SAMPLE_PAGES = "STATS_SAMPLE_PAGES";
    private static final String TABLESPACE2 = "TABLESPACE";
    private static final String CHAIN = "CHAIN";
    private static final String ENGINES = "ENGINES";
    private static final String ENGINE = "ENGINE";
    private static final String BINLOG = "BINLOG";
    private static final String EVENTS = "EVENTS";
    private static final String GLOBAL = "GLOBAL";
    private static final String VARIABLES = "VARIABLES";
    private static final String STATUS = "STATUS";
    private static final String DBLOCK = "DBLOCK";
    private static final String RESET = "RESET";
    private static final String DESCRIBE = "DESCRIBE";
    private static final String WRITE = "WRITE";
    private static final String READ = "READ";
    private static final String LOCAL = "LOCAL";
    private static final String TABLES = "TABLES";
    private static final String CONNECTION = "CONNECTION";
    private static final String INFO = "INFO";
    private static final String DEADLOCKS = "DEADLOCKS";
    private static final String TASK = "TASK";
    private static final String PARTITIONS = "PARTITIONS";
    private static final String HEATMAP = "HEATMAP";

    private int maxIntoClause = -1;

    public MySqlStatementParser(ByteString sql) {
        super(new MySqlExprParser(sql));
    }

    @Deprecated
    public MySqlStatementParser(String sql) {
        this(ByteString.from(sql));
    }

    public MySqlStatementParser(ByteString sql, SQLParserFeature... features) {
        super(new MySqlExprParser(sql, features));
    }

    @Deprecated
    public MySqlStatementParser(String sql, SQLParserFeature... features) {
        this(ByteString.from(sql), features);
    }

    public MySqlStatementParser(ByteString sql, boolean keepComments) {
        super(new MySqlExprParser(sql, keepComments));
    }

    @Deprecated
    public MySqlStatementParser(String sql, boolean keepComments) {
        this(ByteString.from(sql), keepComments);
    }

    public MySqlStatementParser(ByteString sql, boolean skipComment, boolean keepComments) {
        super(new MySqlExprParser(sql, skipComment, keepComments));
    }

    @Deprecated
    public MySqlStatementParser(String sql, boolean skipComment, boolean keepComments) {
        this(ByteString.from(sql), skipComment, keepComments);
    }

    public MySqlStatementParser(Lexer lexer) {
        super(new MySqlExprParser(lexer));
    }

    public int getMaxIntoClause() {
        return maxIntoClause;
    }

    public void setMaxIntoClause(int maxIntoClause) {
        this.maxIntoClause = maxIntoClause;
    }

    public SQLCreateTableStatement parseCreateTable() {
        MySqlCreateTableParser parser = new MySqlCreateTableParser(this.exprParser);
        return parser.parseCreateTable();
    }

    public SQLStatement parseSelect() {
        MySqlSelectParser selectParser = createSQLSelectParser();

        SQLSelect select = selectParser.select();

        if (selectParser.returningFlag) {
            return selectParser.updateStmt;
        }

        return new SQLSelectStatement(select, DbType.mysql);
    }

    public SQLUpdateStatement parseUpdateStatement() {
        return new MySqlSelectParser(this.exprParser, selectListCache).parseUpdateStatment();
    }

    protected MySqlUpdateStatement createUpdateStatement() {
        return new MySqlUpdateStatement();
    }

    private boolean checkDeleteTableSource(SQLTableSource tableSource, AtomicInteger tableCount) {
        if (tableSource instanceof SQLExprTableSource) {
            tableCount.getAndIncrement();
            return true;
        } else if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) tableSource;
            if (joinTableSource.getJoinType() != SQLJoinTableSource.JoinType.COMMA) {
                return false;
            }
            if (!checkDeleteTableSource(joinTableSource.getLeft(), tableCount)) {
                return false;
            }
            return checkDeleteTableSource(joinTableSource.getRight(), tableCount);
        }
        return false;
    }

    public MySqlDeleteStatement parseDeleteStatement() {
        MySqlDeleteStatement deleteStatement = new MySqlDeleteStatement();

        if (lexer.token() == Token.DELETE) {
            lexer.nextToken();

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.HINT) {
                this.getExprParser().parseHints(deleteStatement.getHints());
            }

            if (lexer.identifierEquals(FnvHash.Constants.LOW_PRIORITY)) {
                deleteStatement.setLowPriority(true);
                lexer.nextToken();
            }

            if (lexer.identifierEquals("QUICK")) {
                deleteStatement.setQuick(true);
                lexer.nextToken();
            }

            if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
                deleteStatement.setIgnore(true);
                lexer.nextToken();
            }

            if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
                Lexer.SavePoint savePoint = lexer.mark();
                lexer.nextToken();

                if (lexer.token() == Token.ALL) {
                    lexer.nextToken();
                    acceptIdentifier("PARTITIONS");
                    deleteStatement.setForceAllPartitions(true);
                } else if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
                    lexer.nextToken();
                    deleteStatement.setForceAllPartitions(true);
                } else if (lexer.token() == Token.PARTITION) {
                    lexer.nextToken();
                    SQLName partition = this.exprParser.name();
                    deleteStatement.setForcePartition(partition);
                } else {
                    lexer.reset(savePoint);
                }
            }

            if (lexer.token() == Token.IDENTIFIER) {
                deleteStatement.setTableSource(createSQLSelectParser().parseTableSource());

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();
                    SQLTableSource tableSource = createSQLSelectParser().parseTableSource();
                    deleteStatement.setFrom(tableSource);
                }
            } else if (lexer.token() == Token.FROM) {
                lexer.nextToken();

                if (lexer.token() == Token.FULLTEXT) {
                    lexer.nextToken();
                    if (lexer.identifierEquals(FnvHash.Constants.DICTIONARY)) {
                        lexer.nextToken();
                        deleteStatement.setFulltextDictionary(true);
                    }
                }

                deleteStatement.setTableSource(createSQLSelectParser().parseTableSource());
            } else {
                throw new ParserException("syntax error. " + lexer.info());
            }

            final AtomicInteger tableCount = new AtomicInteger(0);
            if (!checkDeleteTableSource(deleteStatement.getTableSource(), tableCount)) {
                throw new ParserException("syntax error, bad table name for delete. " + lexer.info());
            }

            if (lexer.identifierEquals(FnvHash.Constants.USING)) {
                lexer.nextToken();

                SQLTableSource tableSource = createSQLSelectParser().parseTableSource();
                deleteStatement.setUsing(tableSource);
            } else if (null == deleteStatement.getFrom() && tableCount.get() > 1) {
                throw new ParserException("syntax error, expect using. " + lexer.info());
            }
        }

        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            SQLExpr where = this.exprParser.expr();
            deleteStatement.setWhere(where);
        }

        if (lexer.token() == (Token.ORDER)) {
            SQLOrderBy orderBy = exprParser.parseOrderBy();
            deleteStatement.setOrderBy(orderBy);
        }

        if (lexer.token() == Token.LIMIT) {
            deleteStatement.setLimit(this.exprParser.parseLimit());
        }

        if (lexer.token() != Token.EOF && lexer.token() != Token.SEMI) {
            throw new ParserException("syntax error. " + lexer.info());
        }

        return deleteStatement;
    }

    public SQLStatement parseCreate() {

        List<String> comments = null;
        if (lexer.isKeepComments() && lexer.hasComment()) {
            comments = lexer.readAndResetComments();
        }

        Lexer.SavePoint mark = lexer.mark();

        accept(Token.CREATE);

        boolean replace = false;
        if (lexer.token() == Token.OR) {
            lexer.nextToken();
            accept(Token.REPLACE);
            replace = true;
        }

        boolean physical = false;
        if (lexer.identifierEquals(FnvHash.Constants.PHYSICAL)) {
            lexer.nextToken();
            physical = true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.NEW)) {
            lexer.nextToken();
        } else if (lexer.token() == Token.GROUP) {
            lexer.nextToken();
        } else if (lexer.identifierEquals(FnvHash.Constants.SIMPLE)) {
            lexer.nextToken();
            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                accept(Token.CACHE);
            }
        } else if (lexer.identifierEquals(FnvHash.Constants.TIME)) {
            lexer.nextToken();
            if (lexer.token() == Token.SEQUENCE) {
                lexer.reset(mark);
                return parseCreateSequence(true);
            }
        }

        List<SQLCommentHint> hints = this.exprParser.parseHints();

        boolean isExternal = false;
        if (lexer.identifierEquals(FnvHash.Constants.EXTERNAL)) {
            isExternal = true;

            lexer.nextToken();
            if (lexer.identifierEquals(FnvHash.Constants.CATALOG)) {
                lexer.reset(mark);
                return parseCreateExternalCatalog();
            }
        }

        if (lexer.token() == Token.TABLE ||
            (isEnabled(SQLParserFeature.DrdsMisc) && (lexer.token() == PARTITION || lexer
                .identifierEquals(FnvHash.Constants.BROADCAST) || lexer.identifierEquals(FnvHash.Constants.SINGLE))) ||
            lexer.identifierEquals(FnvHash.Constants.TEMPORARY) || isExternal || lexer.identifierEquals("SHADOW")) {
            lexer.reset(mark);
            MySqlCreateTableParser parser = new MySqlCreateTableParser(this.exprParser);
            MySqlCreateTableStatement stmt = parser.parseCreateTable(true);
            stmt.setHints(hints);

            if (comments != null) {
                stmt.addBeforeComment(comments);
            }

            return stmt;
        }

        switch (lexer.token()) {
        case DATABASE:
        case SCHEMA:
            if (replace) {
                lexer.reset(mark);
            }
            SQLCreateDatabaseStatement stmt = (SQLCreateDatabaseStatement) parseCreateDatabase();
            if (physical) {
                stmt.setPhysical(true);
            }
            return stmt;
        case USER:
            if (replace) {
                lexer.reset(mark);
            }
            return parseCreateUser();
        case ROLE:
            if (replace) {
                lexer.reset(mark);
            }
            return parseCreateRole();
        case TRIGGER:
            lexer.reset(mark);
            return parseCreateTrigger();
        case PROCEDURE:
            if (replace) {
                lexer.reset(mark);
            }
            return parseCreateProcedure();
        case FUNCTION:
            if (replace) {
                lexer.reset(mark);
            }
            return parseCreateFunction();
        case SEQUENCE:
            lexer.reset(mark);
            return parseCreateSequence(true);
        case FULLTEXT:
            lexer.reset(mark);
            return parseCreateFullTextStatement();
        default:
            break;
        }

        if (lexer.token() == Token.UNIQUE
            || lexer.token() == Token.INDEX
            || lexer.token() == Token.FULLTEXT
            || lexer.identifierEquals(FnvHash.Constants.SPATIAL)
            || lexer.identifierEquals(FnvHash.Constants.ANN)
            || lexer.identifierEquals(FnvHash.Constants.GLOBAL)
            || lexer.identifierEquals(FnvHash.Constants.LOCAL)
            || lexer.identifierEquals(FnvHash.Constants.CLUSTERED)) {
            if (replace) {
                lexer.reset(mark);
            }
            return parseCreateIndex(false);
        }

        if (lexer.token() == Token.VIEW
            || lexer.identifierEquals(FnvHash.Constants.ALGORITHM)) {
            if (replace) {
                lexer.reset(mark);
            }

            return parseCreateView();
        }

        if (lexer.identifierEquals(FnvHash.Constants.EVENT)) {
            lexer.reset(mark);
            return parseCreateEvent();
        }

        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
//            Lexer.SavePoint savePoint = lexer.mark();
            lexer.nextToken();
            accept(Token.EQ);
            SQLName name = this.getExprParser().userName();
            validateName(name);
        }

        if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
            lexer.nextToken();
            acceptIdentifier("SECURITY");
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            lexer.nextToken();
        }
        if (lexer.identifierEquals(FnvHash.Constants.EVENT)) {
            lexer.reset(mark);
            return parseCreateEvent();
        } else if (lexer.token() == Token.TRIGGER) {
            lexer.reset(mark);
            return parseCreateTrigger();
        } else if (lexer.token() == Token.VIEW) {
            lexer.reset(mark);
            return parseCreateView();
        } else if (lexer.token() == Token.FUNCTION) {
            lexer.reset(mark);
            return parseCreateFunction();
        } else if (lexer.token() == PROCEDURE) {
            lexer.reset(mark);
            return parseCreateProcedure();
        }

        if (lexer.identifierEquals(FnvHash.Constants.LOGFILE)) {
            return parseCreateLogFileGroup();
        }

        if (lexer.identifierEquals(FnvHash.Constants.SERVER)) {
            return parseCreateServer();
        }

        if (lexer.token() == Token.TABLESPACE) {
            return parseCreateTableSpace();
        }

        if (lexer.identifierEquals(FnvHash.Constants.DIMENSION)) {
            lexer.reset(mark);
            return parseCreateTable();
        }

        if (lexer.identifierEquals(FnvHash.Constants.TABLEGROUP)) {
            lexer.reset(mark);
            return parseCreateTableGroup();
        }

        if (lexer.identifierEquals(FnvHash.Constants.JOINGROUP)) {
            lexer.reset(mark);
            return parseCreateJoinGroup();
        }

        if (lexer.identifierEquals(FnvHash.Constants.OUTLINE)) {
            lexer.reset(mark);
            return parseCreateOutline();
        }

        if (lexer.identifierEquals(FnvHash.Constants.RESOURCE)) {
            lexer.reset(mark);
            return parseCreateResourceGroup();
        }

        if (lexer.identifierEquals(FnvHash.Constants.MATERIALIZED)) {
            lexer.reset(mark);
            return parseCreateMaterializedView();
        }

        if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals(FnvHash.Constants.CCL_RULE)) {
            lexer.reset(mark);
            return parseCreateCclRule();
        }

        if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals(FnvHash.Constants.CCL_TRIGGER)) {
            lexer.reset(mark);
            return parseCreateCclTrigger();
        }

        if (lexer.identifierEquals("SCHEDULE")) {
            lexer.reset(mark);
            return parseCreateSchedule();
        }

        if (lexer.identifierEquals("FileStorage")) {
            lexer.reset(mark);
            return parseCreateFileStorage();
        }

        throw new ParserException("TODO " + lexer.info());
    }

    private void validateName(SQLName name) {
        if (!(name instanceof MySqlUserName)) {
            return;
        }
        validateUserLength(((MySqlUserName) name).getUserName());
        validateHostLength(((MySqlUserName) name).getHost());
    }

    private void validateUserLength(String userName) {
        if (userName != null && userName.length() > MySqlUserName.MAX_USER_NAME_IN_PROCEDURE) {
            throw new RuntimeException(
                String.format("user name should no more than %s", MySqlUserName.MAX_USER_NAME_IN_PROCEDURE));
        }
    }

    private void validateHostLength(String hostName) {
        if (hostName != null && hostName.length() > MySqlUserName.MAX_HOST_NAME_IN_PROCEDURE) {
            throw new RuntimeException(
                String.format("host name should no more than %s", MySqlUserName.MAX_HOST_NAME_IN_PROCEDURE));
        }
    }

    public SQLStatement parseCreateFullTextStatement() {
        Lexer.SavePoint mark = lexer.mark();

        accept(Token.CREATE);
        accept(Token.FULLTEXT);

        if (lexer.identifierEquals(FnvHash.Constants.CHARFILTER)) {
            lexer.nextToken();
            return parseFullTextCharFilter();
        } else if (lexer.identifierEquals(FnvHash.Constants.TOKENIZER)) {
            lexer.nextToken();
            return parseFullTextTokenizer();
        } else if (lexer.identifierEquals(FnvHash.Constants.TOKENFILTER)) {
            lexer.nextToken();
            return parseFullTextTokenFilter();
        } else if (lexer.identifierEquals(FnvHash.Constants.ANALYZER)) {
            lexer.nextToken();
            return parseFullTextAnalyzer();
        } else if (lexer.token() == Token.INDEX) {
            lexer.reset(mark);
            return parseCreateIndex(true);
        } else if (lexer.identifierEquals(FnvHash.Constants.DICTIONARY)) {
            lexer.nextToken();
            MysqlCreateFullTextDictionaryStatement stmt = new MysqlCreateFullTextDictionaryStatement();
            SQLName name = this.exprParser.name();
            stmt.setName(name);

            accept(Token.LPAREN);

            SQLColumnDefinition col = new SQLColumnDefinition();

            col.setName(this.exprParser.name());

            acceptIdentifier("varchar");

            col.setDataType(new SQLDataTypeImpl("varchar"));

            if (lexer.token() == Token.COMMENT) {
                accept(Token.COMMENT);
                col.setComment(this.exprParser.name());
            }

            stmt.setColumn(col);
            accept(Token.RPAREN);

            if (lexer.token() == Token.COMMENT) {
                accept(Token.COMMENT);
                stmt.setComment(this.exprParser.name().getSimpleName());
            }
            return stmt;
        }
        throw new ParserException("TODO " + lexer.info());
    }

    private SQLStatement parseFullTextAnalyzer() {
        MysqlCreateFullTextAnalyzerStatement stmt = new MysqlCreateFullTextAnalyzerStatement();

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        accept(Token.LPAREN);
        for (; ; ) {

            String key = "";
            if (lexer.token() == Token.LITERAL_ALIAS || lexer.token() == Token.LITERAL_CHARS) {
                key = StringUtils.removeNameQuotes(lexer.stringVal());

                if (key.equalsIgnoreCase("charfilter")) {

                    lexer.nextToken();
                    accept(Token.EQ);

                    accept(Token.LBRACKET);
                    for (; ; ) {
                        String c = SQLUtils.normalize(this.exprParser.name().getSimpleName());
                        stmt.getCharfilters().add(c);
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.RBRACKET);
                } else if (key.equalsIgnoreCase("tokenfilter")) {
                    lexer.nextToken();
                    accept(Token.EQ);

                    accept(Token.LBRACKET);
                    for (; ; ) {
                        String c = SQLUtils.normalize(this.exprParser.name().getSimpleName());
                        stmt.getTokenizers().add(c);
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.RBRACKET);
                } else if (key.equalsIgnoreCase("tokenizer")) {
                    lexer.nextToken();
                    accept(Token.EQ);

                    stmt.setTokenizer(SQLUtils.normalize(this.exprParser.name().getSimpleName()));
                }
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        accept(Token.RPAREN);

        String tokenizer = stmt.getTokenizer();
        if (tokenizer == null || StringUtils.isEmpty(tokenizer)) {
            throw new ParserException("tokenizer is require.");
        }

        return stmt;
    }

    private SQLStatement parseFullTextTokenizer() {
        MysqlCreateFullTextTokenizerStatement stmt = new MysqlCreateFullTextTokenizerStatement();

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        accept(Token.LPAREN);
        for (; ; ) {
            SQLAssignItem assignItem = this.exprParser.parseAssignItem();
            assignItem.setParent(stmt);

            SQLExpr target = assignItem.getTarget();
            if ("type".equalsIgnoreCase(((SQLTextLiteralExpr) target).getText())) {
                stmt.setTypeName((SQLTextLiteralExpr) assignItem.getValue());
            } else if ("user_defined_dict".equalsIgnoreCase(((SQLTextLiteralExpr) target).getText())) {
                stmt.setUserDefinedDict((SQLTextLiteralExpr) assignItem.getValue());
            } else {
                stmt.getOptions().add(assignItem);
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }

        accept(Token.RPAREN);

        SQLTextLiteralExpr typeName = stmt.getTypeName();
        if (typeName == null || StringUtils.isEmpty(typeName.getText())) {
            throw new ParserException("type is require.");
        }

        return stmt;
    }

    private SQLStatement parseFullTextCharFilter() {
        MysqlCreateFullTextCharFilterStatement stmt = new MysqlCreateFullTextCharFilterStatement();

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        accept(Token.LPAREN);
        for (; ; ) {
            SQLAssignItem assignItem = this.exprParser.parseAssignItem();
            assignItem.setParent(stmt);

            if ("type".equalsIgnoreCase(((SQLTextLiteralExpr) assignItem.getTarget()).getText())) {
                stmt.setTypeName((SQLTextLiteralExpr) assignItem.getValue());
            } else {
                stmt.getOptions().add(assignItem);
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }

        accept(Token.RPAREN);

        SQLTextLiteralExpr typeName = stmt.getTypeName();
        if (typeName == null || StringUtils.isEmpty(typeName.getText())) {
            throw new ParserException("type is require.");
        }

        return stmt;
    }

    private SQLStatement parseFullTextTokenFilter() {
        MysqlCreateFullTextTokenFilterStatement stmt = new MysqlCreateFullTextTokenFilterStatement();

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        accept(Token.LPAREN);
        for (; ; ) {
            SQLAssignItem assignItem = this.exprParser.parseAssignItem();
            assignItem.setParent(stmt);

            if ("type".equalsIgnoreCase(((SQLTextLiteralExpr) assignItem.getTarget()).getText())) {
                stmt.setTypeName((SQLTextLiteralExpr) assignItem.getValue());
            } else {
                stmt.getOptions().add(assignItem);
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }

        accept(Token.RPAREN);

        SQLTextLiteralExpr typeName = stmt.getTypeName();
        if (typeName == null || StringUtils.isEmpty(typeName.getText())) {
            throw new ParserException("type is require.");
        }

        return stmt;
    }

    public SQLStatement parseCreateOutline() {
        accept(Token.CREATE);
        acceptIdentifier("OUTLINE");

        SQLCreateOutlineStatement stmt = new SQLCreateOutlineStatement();
        stmt.setDbType(dbType);

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        if (lexer.token() == Token.WHERE) {
            lexer.nextToken();
            stmt.setWhere(this.exprParser.expr());
        }

        accept(ON);

        SQLStatement on = this.parseStatement();
        stmt.setOn(on);

        accept(Token.TO);

        SQLStatement to = this.parseStatement();
        stmt.setTo(to);

        return stmt;
    }

    public SQLStatement parseCreateTableSpace() {
        if (lexer.token() == Token.CREATE) {
            accept(Token.CREATE);
        }

        MySqlCreateTableSpaceStatement stmt = new MySqlCreateTableSpaceStatement();

        accept(Token.TABLESPACE);
        stmt.setName(this.exprParser.name());

        if (lexer.identifierEquals(FnvHash.Constants.ADD)) {
            lexer.nextToken();
            acceptIdentifier("DATAFILE");
            SQLExpr file = this.exprParser.primary();
            stmt.setAddDataFile(file);
        }

        for (; ; ) {
            if (lexer.identifierEquals(FnvHash.Constants.INITIAL_SIZE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr initialSize = this.exprParser.expr();
                stmt.setInitialSize(initialSize);
            } else if (lexer.identifierEquals(FnvHash.Constants.FILE_BLOCK_SIZE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr fileBlockSize = this.exprParser.expr();
                stmt.setFileBlockSize(fileBlockSize);
            } else if (lexer.identifierEquals(FnvHash.Constants.EXTENT_SIZE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr extentSize = this.exprParser.expr();
                stmt.setExtentSize(extentSize);
            } else if (lexer.identifierEquals(FnvHash.Constants.AUTOEXTEND_SIZE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr extentSize = this.exprParser.expr();
                stmt.setAutoExtentSize(extentSize);
            } else if (lexer.identifierEquals(FnvHash.Constants.MAX_SIZE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr size = this.exprParser.expr();
                stmt.setMaxSize(size);
            } else if (lexer.identifierEquals(FnvHash.Constants.NODEGROUP)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr size = this.exprParser.expr();
                stmt.setNodeGroup(size);
            } else if (lexer.identifierEquals(FnvHash.Constants.WAIT)) {
                lexer.nextToken();
                stmt.setWait(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.ENGINE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr engine = this.exprParser.expr();
                stmt.setEngine(engine);
            } else if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                SQLExpr comment = this.exprParser.expr();
                stmt.setComment(comment);
            } else if (lexer.token() == Token.USE) {
                lexer.nextToken();
                acceptIdentifier("LOGFILE");
                accept(Token.GROUP);

                SQLExpr logFileGroup = this.exprParser.expr();
                stmt.setFileBlockSize(logFileGroup);
            } else {
                break;
            }
        }
        return stmt;
    }

    public SQLStatement parseCreateServer() {
        if (lexer.token() == Token.CREATE) {
            accept(Token.CREATE);
        }

        MySqlCreateServerStatement stmt = new MySqlCreateServerStatement();

        acceptIdentifier("SERVER");
        stmt.setName(this.exprParser.name());

        accept(Token.FOREIGN);
        acceptIdentifier("DATA");
        acceptIdentifier("WRAPPER");
        stmt.setForeignDataWrapper(this.exprParser.name());

        acceptIdentifier("OPTIONS");
        accept(Token.LPAREN);
        for (; ; ) {
            if (lexer.identifierEquals(FnvHash.Constants.HOST)) {
                lexer.nextToken();
                SQLExpr host = this.exprParser.expr();
                stmt.setHost(host);
            } else if (lexer.token() == Token.USER) {
                lexer.nextToken();
                SQLExpr user = this.exprParser.expr();
                stmt.setUser(user);
            } else if (lexer.token() == Token.DATABASE) {
                lexer.nextToken();
                SQLExpr db = this.exprParser.expr();
                stmt.setDatabase(db);
            } else if (lexer.identifierEquals(FnvHash.Constants.PASSWORD)) {
                lexer.nextToken();
                SQLExpr pwd = this.exprParser.expr();
                stmt.setPassword(pwd);
            } else if (lexer.identifierEquals(FnvHash.Constants.SOCKET)) {
                lexer.nextToken();
                SQLExpr sock = this.exprParser.expr();
                stmt.setSocket(sock);
            } else if (lexer.identifierEquals(FnvHash.Constants.OWNER)) {
                lexer.nextToken();
                SQLExpr owner = this.exprParser.expr();
                stmt.setOwner(owner);
            } else if (lexer.identifierEquals(FnvHash.Constants.PORT)) {
                lexer.nextToken();
                SQLExpr port = this.exprParser.expr();
                stmt.setPort(port);
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            } else {
                break;
            }
        }
        accept(Token.RPAREN);
        return stmt;
    }

    public SQLCreateIndexStatement parseCreateIndex(boolean acceptCreate) {
        if (acceptCreate) {
            accept(Token.CREATE);
        }

        SQLCreateIndexStatement stmt = new SQLCreateIndexStatement();

        SQLIndexDefinition sqlIndexDefinition = stmt.getIndexDefinition();

        this.exprParser.parseIndex(sqlIndexDefinition, true);
        return stmt;
    }

    private void parseCreateIndexUsing(SQLCreateIndexStatement stmt) {
        if (lexer.identifierEquals(FnvHash.Constants.USING)) {
            lexer.nextToken();

            if (lexer.identifierEquals(FnvHash.Constants.BTREE)) {
                stmt.setUsing("BTREE");
                lexer.nextToken();
            } else if (lexer.identifierEquals(FnvHash.Constants.HASH)) {
                stmt.setUsing("HASH");
                lexer.nextToken();
            } else {
                throw new ParserException("TODO " + lexer.info());
            }
        }
    }

    public SQLStatement parseCreateUser() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        accept(Token.USER);

        MySqlCreateUserStatement stmt = new MySqlCreateUserStatement();

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            stmt.setIfNotExists(true);
        }

        for (; ; ) {
            MySqlCreateUserStatement.UserSpecification userSpec = new MySqlCreateUserStatement.UserSpecification();

            if (lexer.token() == Token.IF) {
                lexer.nextToken();
                accept(Token.NOT);
                accept(Token.EXISTS);
                stmt.setIfNotExists(true);
            }

            SQLExpr expr = exprParser.primary();
            MySqlUserName mySqlUserName;
            if (expr instanceof MySqlUserName) {
                mySqlUserName = (MySqlUserName) expr;
            } else {
                if (expr instanceof SQLCharExpr) {
                    expr = new SQLIdentifierExpr(((SQLCharExpr) expr).getText());
                }

                if (expr instanceof SQLIdentifierExpr) {

                    mySqlUserName = new MySqlUserName();
                    mySqlUserName.setUserName(((SQLIdentifierExpr) expr).getName());
                    if (lexer.token() == Token.VARIANT && lexer.stringVal().charAt(0) == '@') {
                        String str = lexer.stringVal();
                        mySqlUserName.setHost(str.substring(1));
                        lexer.nextToken();
                    } else if (lexer.token() == IDENTIFIER) {
                        // create user without specified host
                        mySqlUserName.setHost("%");
                    }
                } else {
                    throw new ParserException("syntax error. invalid " + expr + " expression.");
                }
            }

            userSpec.setUser(mySqlUserName);

            if (lexer.identifierEquals(FnvHash.Constants.IDENTIFIED)) {
                lexer.nextToken();
                if (lexer.token() == Token.BY) {
                    lexer.nextToken();

                    if (lexer.identifierEquals("PASSWORD")) {
                        lexer.nextToken();
                        userSpec.setPasswordHash(true);
                    }

                    SQLExpr password = this.exprParser.expr();
                    if (password instanceof SQLIdentifierExpr) {
                        userSpec.setPassword(new SQLCharExpr(((SQLIdentifierExpr) password).getName()));
                    } else if (password instanceof SQLCharExpr) {
                        userSpec.setPassword(password);
                    } else {
                        throw new ParserException("syntax error. invalid " + password + " expression.");
                    }

                } else if (lexer.token() == Token.WITH) {
                    lexer.nextToken();
                    userSpec.setAuthPlugin(this.exprParser.expr());

                    // IDENTIFIED WITH auth_plugin BY 'auth_string'
                    // IDENTIFIED WITH auth_plugin AS 'auth_string'
                    if (lexer.token() == Token.BY ||
                        lexer.token() == Token.AS) {
                        userSpec.setPluginAs(lexer.token() == Token.AS);
                        lexer.nextToken();

                        if (userSpec.isPluginAs()) {
                            // Remove ' because lexer don't remove it when token after as.
                            String psw = lexer.stringVal();
                            if (psw.length() >= 2 && '\'' == psw.charAt(0) && '\'' == psw.charAt(psw.length() - 1)) {
                                userSpec.setPassword(new SQLCharExpr(psw.substring(1, psw.length() - 1)));
                            } else {
                                userSpec.setPassword(new SQLCharExpr(psw));
                            }
                            lexer.nextToken();
                        } else {
                            userSpec.setPassword(this.exprParser.charExpr());
                        }
                    }
                }
            }

            stmt.addUser(userSpec);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        return stmt;
    }

    /**
     * Parsing create role statement.
     *
     * @return {@link MySqlCreateRoleStatement}
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/create-role.html">Create role</a>
     */
    @Override
    public SQLStatement parseCreateRole() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        accept(ROLE);

        MySqlCreateRoleStatement stmt = new MySqlCreateRoleStatement();

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            stmt.setIfNotExists(true);
        }

        for (; ; ) {
            MySqlCreateRoleStatement.RoleSpec roleSpec = new MySqlCreateRoleStatement.RoleSpec();

            MySqlUserName expr = MySqlUserName.fromExpr(((MySqlExprParser) exprParser).userName());

            roleSpec.setUsername(expr);
            stmt.addRoleSpec(roleSpec);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        return stmt;
    }

    /**
     * Parsing drop role satement.
     *
     * @return {@link MySqlDropRoleStatement}
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/drop-role.html">Drop Role</a>
     */
    @Override
    public SQLStatement parseDropRole() {
        accept(DROP);
        accept(ROLE);

        MySqlDropRoleStatement stmt = new MySqlDropRoleStatement();

        for (; ; ) {
            MySqlCreateRoleStatement.RoleSpec roleSpec = new MySqlCreateRoleStatement.RoleSpec();

            MySqlUserName expr = MySqlUserName.fromExpr(((MySqlExprParser) exprParser).userName());

            roleSpec.setUsername(expr);
            stmt.addRoleSpec(roleSpec);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        return stmt;
    }

    /**
     * Parsing grant statement in mysql.
     *
     * @return {@link SQLGrantStatement} or {@link MySqlGrantRoleStatement}
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/grant.html">Grant</a>
     */
    @Override
    public SQLStatement parseGrant() {
        Lexer.SavePoint mark = lexer.mark();

        SQLStatement stmt = tryParseGrantRole();
        if (stmt != null) {
            return stmt;
        }

        // Parse grant role failed, then it should be grant privileges statement
        lexer.reset(mark);
        return tryParseGrantPrivileges();
    }

    private SQLStatement tryParseGrantPrivileges() {
        return super.parseGrant();
    }

    private SQLStatement tryParseGrantRole() {
        try {
            return doParseGrantRole();
        } catch (Exception e) {
            return null;
        }
    }

    private SQLStatement doParseGrantRole() {
        accept(GRANT);

        MySqlGrantRoleStatement stmt = new MySqlGrantRoleStatement();

        MySqlExprParser mysqlExprParser = (MySqlExprParser) exprParser;

        MySqlUserName currentUsername;

        while (true) {
            currentUsername = MySqlUserName.fromExpr(mysqlExprParser.userName());
            stmt.addSourceAccount(currentUsername);

            if (COMMA == lexer.token()) {
                lexer.nextToken();
            } else {
                break;
            }
        }

        accept(TO);

        while (true) {
            currentUsername = MySqlUserName.fromExpr(mysqlExprParser.userName());
            stmt.addDestAccount(currentUsername);

            if (COMMA == lexer.token()) {
                lexer.nextToken();
            } else {
                break;
            }
        }

        if (lexer.token() == WITH) {
            accept(WITH);
            acceptIdentifier("ADMIN");
            acceptIdentifier("OPTION");

            stmt.setWithAdminOption(true);
        }

        return stmt;
    }

    @Override
    public SQLStatement parseRevoke() {
        Lexer.SavePoint mark = lexer.mark();

        SQLStatement stmt = tryParseRevokeRole();
        if (stmt != null) {
            return stmt;
        }

        // Parse grant role failed, then it should be grant privileges statement
        lexer.reset(mark);
        return tryParseRevokePrivileges();
    }

    private SQLStatement tryParseRevokePrivileges() {
        return super.parseRevoke();
    }

    private SQLStatement tryParseRevokeRole() {
        try {
            return doParseRevokeRole();
        } catch (Exception e) {
            return null;
        }
    }

    private SQLStatement doParseRevokeRole() {
        accept(REVOKE);

        MySqlRevokeRoleStatement stmt = new MySqlRevokeRoleStatement();

        MySqlExprParser mysqlExprParser = (MySqlExprParser) exprParser;

        MySqlUserName currentUsername;

        while (true) {
            currentUsername = MySqlUserName.fromExpr(mysqlExprParser.userName());
            stmt.addRevokedAccount(currentUsername);

            if (COMMA == lexer.token()) {
                lexer.nextToken();
            } else {
                break;
            }
        }

        accept(FROM);

        while (true) {
            currentUsername = MySqlUserName.fromExpr(mysqlExprParser.userName());
            stmt.addFromAccount(currentUsername);

            if (COMMA == lexer.token()) {
                lexer.nextToken();
            } else {
                break;
            }
        }

        return stmt;
    }

    public SQLStatement parseKill() {
        accept(Token.KILL);

        MySqlKillStatement stmt = new MySqlKillStatement();

        if (lexer.identifierEquals("CONNECTION")) {
            stmt.setType(MySqlKillStatement.Type.CONNECTION);
            lexer.nextToken();
        } else if (lexer.identifierEquals(FnvHash.Constants.QUERY)
            || lexer.identifierEquals(FnvHash.Constants.PROCESS)) {
            stmt.setType(MySqlKillStatement.Type.QUERY);
            lexer.nextToken();
        } else if (lexer.token() == Token.LITERAL_INT
            || lexer.token() == Token.LITERAL_CHARS
            || lexer.token() == Token.LITERAL_ALIAS
        ) {
            // skip
        } else if (lexer.token() == Token.ALL) {
            SQLIdentifierExpr all = new SQLIdentifierExpr(lexer.stringVal());
            all.setParent(stmt);
            stmt.getThreadIds().add(all);
            lexer.nextToken();
        } else {
            throw new ParserException("not support kill type " + lexer.token() + ". " + lexer.info());
        }

        this.exprParser.exprList(stmt.getThreadIds(), stmt);

        if (lexer.token() == Token.SEMI) {
            lexer.nextToken();
            stmt.setAfterSemi(true);
        }
        return stmt;
    }

    public SQLStatement parseBinlog() {
        acceptIdentifier("binlog");

        MySqlBinlogStatement stmt = new MySqlBinlogStatement();

        SQLExpr expr = this.exprParser.expr();
        stmt.setExpr(expr);

        return stmt;
    }

    public MySqlAnalyzeStatement parseAnalyze() {
        accept(Token.ANALYZE);
        MySqlAnalyzeStatement stmt = new MySqlAnalyzeStatement();

        if (lexer.token() == TABLE) {
            accept(Token.TABLE);
            List<SQLName> names = new ArrayList<SQLName>();
            this.exprParser.names(names, stmt);

            for (SQLName name : names) {
                stmt.addTableSource(new SQLExprTableSource(name));
            }
            if (lexer.token() == WHERE) {
                accept(WHERE);
                SQLExpr where = this.exprParser.expr();
                stmt.setAdbWhere(where);
            }
        } else if (lexer.token() == DATABASE) {
            accept(DATABASE);
            SQLName name = this.exprParser.name();
            stmt.setAdbSchema((SQLIdentifierExpr) name);
        } else if (lexer.token() == COLUMN) {
            accept(COLUMN);
            SQLName table = this.exprParser.name();
            stmt.setTable(table);

            accept(LPAREN);

            for (; ; ) {
                SQLName name = this.exprParser.name();
                stmt.getAdbColumns().add((SQLIdentifierExpr) name);
                if (lexer.token() == COMMA) {
                    accept(COMMA);
                    continue;
                }
                break;
            }
            accept(RPAREN);
            if (lexer.token() == WHERE) {
                accept(WHERE);
                SQLExpr where = this.exprParser.expr();
                stmt.setAdbWhere(where);
            }
        } else if (lexer.identifierEquals("columns")) {
            lexer.nextToken();
            accept(GROUP);

            SQLName table = this.exprParser.name();
            stmt.setTable(table);

            accept(LPAREN);

            for (; ; ) {
                SQLName name = this.exprParser.name();
                stmt.getAdbColumnsGroup().add((SQLIdentifierExpr) name);
                if (lexer.token() == COMMA) {
                    accept(COMMA);
                    continue;
                }
                break;
            }
            accept(RPAREN);
            if (lexer.token() == WHERE) {
                accept(WHERE);
                SQLExpr where = this.exprParser.expr();
                stmt.setAdbWhere(where);
            }
        }

        if (lexer.token() == Token.PARTITION) {
            stmt.setPartition(
                parsePartitionRef()
            );
        }

        if (lexer.token() == Token.COMPUTE) {
            lexer.nextToken();
            acceptIdentifier("STATISTICS");
            stmt.setComputeStatistics(true);
        }

        if (lexer.token() == FOR) {
            lexer.nextToken();
            acceptIdentifier("COLUMNS");
            stmt.setForColums(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.CACHE)) {
            lexer.nextToken();
            acceptIdentifier("METADATA");
            stmt.setCacheMetadata(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.NOSCAN)) {
            lexer.nextToken();
            stmt.setNoscan(true);
        }

        return stmt;
    }

    public MySqlOptimizeStatement parseOptimize() {
        accept(Token.OPTIMIZE);
        accept(Token.TABLE);

        MySqlOptimizeStatement stmt = new MySqlOptimizeStatement();
        List<SQLName> names = new ArrayList<SQLName>();
        this.exprParser.names(names, stmt);

        for (SQLName name : names) {
            stmt.addTableSource(new SQLExprTableSource(name));
        }
        return stmt;
    }

    public SQLStatement parseReset() {
        acceptIdentifier(RESET);
        if (lexer.identifierEquals("SLAVE") || lexer.identifierEquals("REPLICA")) {
            MySqlResetSlaveStatement statement = new MySqlResetSlaveStatement();
            lexer.nextToken();
            if (lexer.token() == Token.ALL) {
                lexer.nextToken();
                statement.setAll(true);
            }
            if (lexer.token() == FOR) {
                lexer.nextToken();
                acceptIdentifier("CHANNEL");
                SQLCharExpr channel = this.exprParser.charExpr();
                statement.setChannel(channel);
            }
            return statement;
        }

        MySqlResetStatement stmt = new MySqlResetStatement();

        for (; ; ) {
            if (lexer.token() == Token.IDENTIFIER) {
                if (lexer.identifierEquals("QUERY")) {
                    lexer.nextToken();
                    accept(Token.CACHE);
                    stmt.getOptions().add("QUERY CACHE");
                } else {
                    stmt.getOptions().add(lexer.stringVal());
                    lexer.nextToken();
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
            }
            break;
        }

        return stmt;
    }

    public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        if (lexer.identifierEquals("PREPARE")) {
            MySqlPrepareStatement stmt = parsePrepare();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("EXECUTE")) {
            acceptIdentifier("EXECUTE");

            if (lexer.identifierEquals("RESTART") || lexer.identifierEquals("UPDATE")) {
                MySqlExecuteForAdsStatement stmt = parseExecuteForAds();
                statementList.add(stmt);
            } else {
                MySqlExecuteStatement stmt = parseExecute();
                statementList.add(stmt);
            }
            return true;
        }

        if (lexer.identifierEquals("DEALLOCATE")) {
            MysqlDeallocatePrepareStatement stmt = parseDeallocatePrepare();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("LOAD")) {
            SQLStatement stmt = parseLoad();
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.REPLACE) {
            SQLReplaceStatement stmt = parseReplace();
            statementList.add(stmt);
            return true;
        }
        if (lexer.identifierEquals("START")) {
            acceptIdentifier("START");
            if (lexer.identifierEquals("TRANSACTION")) {
                SQLStartTransactionStatement stmt = parseStart();
                statementList.add(stmt);
            } else if (lexer.identifierEquals("SLAVE") || lexer.identifierEquals("REPLICA")) {
                MySqlStartSlaveStatement stmt = parseStartSlave();
                statementList.add(stmt);
            }
            return true;
        }

        if (lexer.identifierEquals("STOP")) {
            acceptIdentifier("STOP");
            if (lexer.identifierEquals("SLAVE") || lexer.identifierEquals("REPLICA")) {
                MySqlStopSlaveStatement stmt = parseStoptSlave();
                statementList.add(stmt);
                return true;
            }
        }

        if (lexer.token() == Token.SHOW) {
            SQLStatement stmt = parseShow();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("CLEAR")) {
            lexer.nextToken();

            if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals("DDL")) {
                // CLEAR DDL CACHE { ALL | <job_id> [ , <job_id> ] ... }
                lexer.nextToken();
                accept(Token.CACHE);
                return parseGenericDdlCommand(new DrdsClearDDLJobCache(), statementList);
            } else if (isEnabled(SQLParserFeature.DrdsMisc) && lexer.token() == Token.SEQUENCE) {
                // CLEAR SEQUENCE CACHE FOR [ALL | <sequence_name>]
                lexer.nextToken();
                accept(Token.CACHE);
                accept(FOR);

                DrdsClearSeqCacheStatement stmt = new DrdsClearSeqCacheStatement();
                if (lexer.token() == Token.ALL) {
                    lexer.nextToken();
                    stmt.setAll(true);
                } else {
                    SQLName name = exprParser.name();
                    stmt.setName(name);
                }
                statementList.add(stmt);
                return true;
            } else if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals("CCL_RULES")) {
                lexer.nextToken();
                DrdsClearCclRulesStatement stmt = new DrdsClearCclRulesStatement();
                statementList.add(stmt);
                return true;
            } else if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals("CCl_TRIGGERS")) {
                lexer.nextToken();
                DrdsClearCclTriggersStatement stmt = new DrdsClearCclTriggersStatement();
                statementList.add(stmt);
                return true;
            } else if (lexer.identifierEquals("HEATMAP_CACHE")) {
                lexer.nextToken();
                MySqlClearPartitionsHeatmapCacheStatement stmt = new MySqlClearPartitionsHeatmapCacheStatement();
                statementList.add(stmt);
                return true;
            }

            acceptIdentifier("PLANCACHE");

            statementList.add(new MySqlClearPlanCacheStatement());
            return true;
        }

        if (lexer.identifierEquals("DISABLED")) {
            lexer.nextToken();
            acceptIdentifier("PLANCACHE");

            statementList.add(new MySqlDisabledPlanCacheStatement());
            return true;
        }

        if (lexer.token() == Token.EXPLAIN) {
            SQLStatement stmt = this.parseExplain();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(BINLOG)) {
            SQLStatement stmt = parseBinlog();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(RESET)) {
            SQLStatement stmt = parseReset();
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.ANALYZE) {
            SQLStatement stmt = parseAnalyze();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.ARCHIVE)) {
            SQLStatement stmt = parseArchive();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.BACKUP)) {
            SQLStatement stmt = parseBackup();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.RESTORE)) {
            SQLStatement stmt = parseRestore();
            statementList.add(stmt);
            return true;
        }

        // iterate statement
        if (lexer.token() == Token.ITERATE) {
            statementList.add(this.parseIterate());
            return true;
        }

        if (lexer.identifierEquals("BUILD")) {
            SQLStatement stmt = parseBuildTable();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("CANCEL")) {
            SQLStatement stmt = parseCancelJob();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("UNARCHIVE")) {
            SQLStatement stmt = parseUnArchiveJob();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("PUSHDOWN")) {
            SQLStatement statement = parsePushDownUdf();
            statementList.add(statement);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.EXPORT)) {
            lexer.nextToken();
            if (lexer.token() == Token.TABLE) {
                SQLStatement stmt = parseExportTable();
                statementList.add(stmt);
            } else if (lexer.token() == Token.DATABASE) {
                SQLStatement stmt = parseExportDB();
                statementList.add(stmt);
            }
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.IMPORT)) {
            lexer.nextToken();
            if (lexer.token() == Token.TABLE) {
                SQLStatement stmt = parseImportTable();
                statementList.add(stmt);
            } else if (lexer.token() == Token.DATABASE) {
                SQLStatement stmt = parseImportDB();
                statementList.add(stmt);
            }
            return true;
        }

        if (lexer.identifierEquals("SUBMIT")) {
            lexer.nextToken();
            acceptIdentifier("JOB");

            SQLStatement stmt = parseSubmitJob();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.MIGRATE)) {
            SQLStatement stmt = parseMigrate();
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.OPTIMIZE) {
            SQLStatement stmt = parseOptimize();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("HELP")) {
            lexer.nextToken();
            MySqlHelpStatement stmt = new MySqlHelpStatement();
            stmt.setContent(this.exprParser.primary());
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("FLUSH")) {
            SQLStatement stmt = parseFlush();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.SYNC)) {
            SQLStatement stmt = parseSync();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.INIT)) {
            statementList.add(
                new SQLExprStatement(
                    this.exprParser.expr()));
            return true;
        }

        if (lexer.identifierEquals("FIRE")) {
            lexer.nextToken();
            DrdsFireScheduleStatement stmt = parseFireSchedule();
            statementList.add(stmt);
            return true;
        }

        if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals(FnvHash.Constants.RECOVER)) {
            // RECOVER DDL { ALL | <job_id> [, <job_id>] ... }
            lexer.nextToken();
            acceptIdentifier("DDL");
            return parseGenericDdlCommand(new DrdsRecoverDDLJob(), statementList);
        }

        if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.token() == CONTINUE) {
            // CONTINUE DDL { ALL | <job_id> [, <job_id>] ... }
            lexer.nextToken();
            if (identifierEquals("DDL")) {
                acceptIdentifier("DDL");
                return parseGenericDdlCommand(new DrdsContinueDDLJob(), statementList);
            } else if (identifierEquals("SCHEDULE")) {
                DrdsContinueScheduleStatement stmt = parseContinueSchedule();
                statementList.add(stmt);
                return true;
            }
        }

        if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals(FnvHash.Constants.PAUSE)) {
            // PAUSE DDL { ALL | <job_id> [, <job_id>] ... }
            lexer.nextToken();
            if (identifierEquals("DDL")) {
                acceptIdentifier("DDL");
                return parseGenericDdlCommand(new DrdsPauseDDLJob(), statementList);
            } else if (identifierEquals("SCHEDULE")) {
                DrdsPauseScheduleStatement stmt = parsePauseSchedule();
                statementList.add(stmt);
                return true;
            }
        }

        if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals(FnvHash.Constants.REMOVE)) {
            // REMOVE DDL { ALL { COMPLETED | PENDING } | <job_id> [, <job_id>] ...}
            lexer.nextToken();
            acceptIdentifier("DDL");
            DrdsRemoveDDLJob stmt = new DrdsRemoveDDLJob();
            if (Token.ALL == lexer.token()) {
                lexer.nextToken();
                if (lexer.identifierEquals("COMPLETED")) {
                    lexer.nextToken();
                    stmt.setAllCompleted(true);
                } else if (lexer.identifierEquals("PENDING")) {
                    lexer.nextToken();
                    stmt.setAllPending(true);
                } else {
                    throw new ParserException(
                        "syntax error, expect COMPLETED or PENDING, actual " + lexer.token() + ", " + lexer.info());
                }
            } else {
                while (true) {
                    stmt.addJobId(lexer.integerValue().longValue());
                    accept(Token.LITERAL_INT);
                    if (Token.COMMA == lexer.token()) {
                        lexer.nextToken();
                    } else if (lexer.token() == Token.EOF || lexer.token() == Token.SEMI) {
                        break;
                    } else {
                        throw new ParserException(
                            "syntax error, expect job id, actual " + lexer.token() + ", " + lexer.info());
                    }
                }
            }
            statementList.add(stmt);
            return true;
        }

        if (isEnabled(SQLParserFeature.DrdsMisc) && lexer.identifierEquals("MOVE")) {
            DrdsMoveDataBase stmt = new DrdsMoveDataBase();
            // MOVE DATABASE db1,..dbn TO storage_instance_id
            lexer.nextToken();
            accept(Token.DATABASE);
            if (lexer.token() == Token.HINT) {
                stmt.setHeadHints(this.exprParser.parseHints());
            }
            if (!lexer.identifierEquals("CLEAN")) {
                List<String> groups;
                if (lexer.token() == Token.LPAREN) {
                    groups = new ArrayList<String>();
                    accept(Token.LPAREN);
                    while (true) {
                        groups.add(lexer.stringVal());
                        lexer.nextToken();
                        if (Token.COMMA == lexer.token()) {
                            lexer.nextToken();
                        } else if (lexer.token() == Token.RPAREN) {
                            accept(Token.RPAREN);
                            accept(Token.TO);
                            if (!stmt.getStorageGroups().containsKey(lexer.stringVal())) {
                                stmt.put(lexer.stringVal(), groups);
                            } else {
                                stmt.getStorageGroups().get(lexer.stringVal()).addAll(groups);
                            }
                            lexer.nextToken();
                            if (Token.COMMA == lexer.token()) {
                                accept(Token.COMMA);
                                accept(Token.LPAREN);
                                groups = new ArrayList<String>();
                            } else {
                                break;
                            }
                        } else {
                            throw new ParserException(
                                "syntax error, expect ',' or ')', actual " + lexer.token() + ", " + lexer.info());
                        }
                    }
                } else {
                    groups = new ArrayList<String>();
                    while (true) {
                        groups.add(lexer.stringVal());
                        lexer.nextToken();
                        if (Token.COMMA == lexer.token()) {
                            lexer.nextToken();
                        } else if (lexer.token() == Token.TO) {
                            accept(Token.TO);
                            stmt.put(lexer.stringVal(), groups);
                            lexer.nextToken();
                            break;
                        } else {
                            throw new ParserException(
                                "syntax error, expect ',' or 'to', actual " + lexer.token() + ", " + lexer.info());
                        }
                    }
                }

            } else {
                stmt.setCleanUpCommand(true);
                lexer.nextToken();
                List<String> groups = new ArrayList<String>();
                while (true) {
                    groups.add(lexer.stringVal());
                    lexer.nextToken();
                    if (Token.COMMA != lexer.token()) {
                        stmt.put(groups);
                        break;
                    }
                    lexer.nextToken();
                }
            }
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("INSPECT")) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals("DDL")) {
                // INSPECT DDL CACHE
                lexer.nextToken();
                accept(Token.CACHE);
                statementList.add(new DrdsInspectDDLJobCache());
                return true;
            } else if (isEnabled(SQLParserFeature.DrdsMisc) && lexer.identifierEquals("RULE")) {
                // INSPECT RULE VERSION [IGNORE MANAGER]
                lexer.nextToken();
                acceptIdentifier("VERSION");

                boolean ignoreManager = false;
                if (lexer.identifierEquals("IGNORE")) {
                    lexer.nextToken();
                    acceptIdentifier("MANAGER");
                    ignoreManager = true;
                }

                DrdsInspectRuleVersionStatement stmt = new DrdsInspectRuleVersionStatement();
                stmt.setIgnoreManager(ignoreManager);
                statementList.add(stmt);
                return true;
            } else if (isEnabled(SQLParserFeature.DrdsMisc) && lexer.token() == Token.GROUP) {
                // INSPECT GROUP SEQUENCE RANGE FOR <seq_name>
                lexer.nextToken();
                accept(Token.SEQUENCE);
                acceptIdentifier("RANGE");
                accept(FOR);

                SQLName name = exprParser.name();
                lexer.nextToken();

                DrdsInspectGroupSeqRangeStatement stmt = new DrdsInspectGroupSeqRangeStatement();
                stmt.setName(name);
                statementList.add(stmt);
                return true;
            }
            lexer.reset(mark);
        }

        if (lexer.identifierEquals(FnvHash.Constants.CONVERT)) {
            lexer.nextToken();
            accept(ALL);
            acceptIdentifier("SEQUENCES");

            accept(FROM);
            SQLName fromType = exprParser.name();

            accept(TO);
            SQLName toType = exprParser.name();

            SQLName schemaName = null;
            boolean allSchemata = true;
            if (lexer.token() == FOR) {
                lexer.nextToken();
                schemaName = exprParser.name();
                allSchemata = false;
            }

            DrdsConvertAllSequencesStatement stmt = new DrdsConvertAllSequencesStatement();
            stmt.setFromType(fromType);
            stmt.setToType(toType);
            stmt.setSchemaName(schemaName);
            stmt.setAllSchemata(allSchemata);

            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.CHANGE)) {
            SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.identifierEquals("MASTER")) {
                MySqlChangeMasterStatement stmt = parseChangeMaster();
                statementList.add(stmt);
                return true;
            }
            if (lexer.identifierEquals("REPLICATION")) {
                MySqlChangeReplicationFilterStatement stmt = parseChangeReplication();
                statementList.add(stmt);
                return true;
            }
            lexer.reset(mark);
        }

        if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals(FnvHash.Constants.CHANGE)) {
            // CHANGE DDL <job_id> { SKIP | ADD } <group_and_table_name> [ , <group_and_table_name> ] ...
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.identifierEquals("DDL")) {
                lexer.nextToken();

                DrdsChangeDDLJob stmt = new DrdsChangeDDLJob();

                stmt.setJobId(lexer.integerValue().longValue());
                accept(Token.LITERAL_INT);

                if (lexer.identifierEquals("SKIP")) {
                    lexer.nextToken();
                    stmt.setSkip(true);
                } else if (lexer.identifierEquals("ADD")) {
                    lexer.nextToken();
                    stmt.setAdd(true);
                } else {
                    throw new ParserException(
                        "syntax error, expect SKIP or ADD, actual " + lexer.token() + ", " + lexer.info());
                }

                StringBuilder builder = new StringBuilder();
                while (true) {
                    if (Token.COMMA == lexer.token()) {
                        lexer.nextToken();
                        stmt.addGroupAndTableNameList(builder.toString());
                        builder = new StringBuilder();
                    } else if (lexer.token() == Token.EOF || lexer.token() == Token.SEMI) {
                        stmt.addGroupAndTableNameList(builder.toString());
                        break;
                    } else if (lexer.token() == Token.COLON) {
                        builder.append(':');
                        lexer.nextToken();
                    } else if (lexer.token() == Token.DOT) {
                        builder.append('.');
                        lexer.nextToken();
                    } else {
                        builder.append(lexer.stringVal());
                        lexer.nextToken();
                    }
                }

                statementList.add(stmt);
                return true;
            }
            lexer.reset(mark);
        }

        if (isEnabled(SQLParserFeature.DRDSBaseline) && lexer.identifierEquals("BASELINE")) {
            lexer.nextToken();

            DrdsBaselineStatement stmt = new DrdsBaselineStatement();

            if (Token.EOF == lexer.token() || Token.SEMI == lexer.token() ||
                lexer.stringVal().isEmpty() || lexer.stringVal().equalsIgnoreCase("BASELINE")) {
                throw new ParserException(
                    "syntax error, expect baseline operation, actual " + lexer.token() + ", " + lexer.info());
            }

            stmt.setOperation(lexer.stringVal());

            lexer.setToken(Token.COMMA); // Hack here: Set previous comma to deal with negative number.
            lexer.nextToken();

            if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
                // Parse select.
                lexer.nextToken();

                if (lexer.token() == Token.HINT) {
                    stmt.setHeadHints(this.exprParser.parseHints());
                }

                MySqlSelectParser selectParser = createSQLSelectParser();
                stmt.setSelect(selectParser.select());
            } else {
                // Parse id list.
                while (lexer.token() != Token.EOF && lexer.token() != Token.SEMI) {
                    stmt.addBaselineId(lexer.integerValue().longValue());
                    accept(Token.LITERAL_INT);
                    if (Token.COMMA == lexer.token()) {
                        lexer.nextToken();
                    }
                }
            }

            statementList.add(stmt);
            return true;
        }

        if (isEnabled(SQLParserFeature.DrdsMisc)) {
            if (lexer.identifierEquals("UPGRADE")) {
                // UPGRADE VERSION TO <version>
                lexer.nextToken();
                SQLStatement stmt = parseChangeRuleVersion(true);
                statementList.add(stmt);
                return true;
            } else if (lexer.identifierEquals("DOWNGRADE")) {
                // DOWNGRADE VERSION TO <version>
                lexer.nextToken();
                SQLStatement stmt = parseChangeRuleVersion(false);
                statementList.add(stmt);
                return true;
            } else if (lexer.identifierEquals("RESYNC") || lexer.identifierEquals("REFRESH")) {
                // REFRESH LOCAL RULES
                lexer.nextToken();
                acceptIdentifier("LOCAL");
                acceptIdentifier("RULES");
                SQLStatement stmt = new DrdsRefreshLocalRulesStatement();
                statementList.add(stmt);
                return true;
            }
        }

        if (lexer.token() == Token.DESC || lexer.identifierEquals(DESCRIBE)) {
            SQLStatement stmt = parseDescribe();
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.LOCK) {
            lexer.nextToken();
            String val = lexer.stringVal();
            boolean isLockTables = TABLES.equalsIgnoreCase(val) && lexer.token() == Token.IDENTIFIER;
            boolean isLockTable = "TABLE".equalsIgnoreCase(val) && lexer.token() == Token.TABLE;
            if (isLockTables || isLockTable) {
                lexer.nextToken();
            } else {
                setErrorEndPos(lexer.pos());
                throw new ParserException(
                    "syntax error, expect TABLES or TABLE, actual " + lexer.token() + ", " + lexer.info());
            }

            MySqlLockTableStatement stmt = new MySqlLockTableStatement();

            for (; ; ) {
                MySqlLockTableStatement.Item item = new MySqlLockTableStatement.Item();

                SQLExprTableSource tableSource = null;
                SQLName tableName = this.exprParser.name();

                if (lexer.token() == Token.AS) {
                    lexer.nextToken();
                    String as = lexer.stringVal();
                    tableSource = new SQLExprTableSource(tableName, as);
                    lexer.nextToken();
                } else {
                    tableSource = new SQLExprTableSource(tableName);
                }
                item.setTableSource(tableSource);
                stmt.getItems().add(item);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                if (lexer.identifierEquals(READ)) {
                    lexer.nextToken();
                    if (lexer.identifierEquals(LOCAL)) {
                        lexer.nextToken();
                        item.setLockType(LockType.READ_LOCAL);
                    } else {
                        item.setLockType(LockType.READ);
                    }
                } else if (lexer.identifierEquals(WRITE)) {
                    lexer.nextToken();
                    item.setLockType(LockType.WRITE);
                } else if (lexer.identifierEquals(FnvHash.Constants.LOW_PRIORITY)) {
                    lexer.nextToken();
                    acceptIdentifier(WRITE);
                    lexer.nextToken();
                    item.setLockType(LockType.LOW_PRIORITY_WRITE);
                } else {
                    throw new ParserException(
                        "syntax error, expect READ or WRITE OR AS, actual " + lexer.token() + ", " + lexer.info());
                }

                if (lexer.token() == Token.HINT) {
                    item.setHints(this.exprParser.parseHints());
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("UNLOCK")) {
            lexer.nextToken();
            String val = lexer.stringVal();
            boolean isUnLockTables = TABLES.equalsIgnoreCase(val) && lexer.token() == Token.IDENTIFIER;
            boolean isUnLockTable = "TABLE".equalsIgnoreCase(val) && lexer.token() == Token.TABLE;
            statementList.add(new MySqlUnlockTablesStatement());
            if (isUnLockTables || isUnLockTable) {
                lexer.nextToken();
            } else {
                setErrorEndPos(lexer.pos());
                throw new ParserException(
                    "syntax error, expect TABLES or TABLE, actual " + lexer.token() + ", " + lexer.info());
            }
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.CHECKSUM)) {
            statementList.add(this.parseChecksum());
            return true;
        }

        if (lexer.identifierEquals("REBALANCE")) {
            lexer.nextToken();
            statementList.add(parseRebalance(false));
            return true;
        }

        if (lexer.identifierEquals("SCHEDULE")) {
            lexer.nextToken();
            if (lexer.identifierEquals("REBALANCE")) {
                lexer.nextToken();
                statementList.add(parseRebalance(true));
                return true;
            } else if (lexer.token() == Token.ALTER) {
                statementList.add(parseAlter());
                if (statementList.get(0) instanceof SQLAlterTableGroupStatement) {
                    SQLAlterTableGroupStatement statement = (SQLAlterTableGroupStatement) statementList.get(0);
                    if (statement.getItem() instanceof DrdsAlterTableGroupSetLocality) {
                        DrdsAlterTableGroupSetLocality item = (DrdsAlterTableGroupSetLocality) statement.getItem();
                        item.setLogicalDDL(true);
                    } else if (statement.getItem() instanceof DrdsAlterTableGroupSetPartitionsLocality) {
                        DrdsAlterTableGroupSetPartitionsLocality item =
                            (DrdsAlterTableGroupSetPartitionsLocality) statement.getItem();
                        item.setLogicalDDL(true);
                    }
                }
                return true;
            }
        }

        if (lexer.token() == Token.HINT) {
            List<SQLCommentHint> hints = this.exprParser.parseHints();

            boolean tddlHints = false;
            boolean accept = false;

            boolean acceptHint = false;
            switch (lexer.token()) {
            case SELECT:
            case WITH:
            case DELETE:
            case UPDATE:
            case INSERT:
            case SHOW:
            case REPLACE:
            case TRUNCATE:
            case DROP:
            case ALTER:
            case CREATE:
            case CHECK:
            case SET:
            case DESC:
            case OPTIMIZE:
            case ANALYZE:
            case KILL:
            case EXPLAIN:
            case LPAREN:
            case CONTINUE:
                acceptHint = true;
                break;
            case IDENTIFIER:
                acceptHint = lexer.hash_lower() == FnvHash.Constants.DUMP
                    || lexer.hash_lower() == FnvHash.Constants.RENAME
                    || lexer.hash_lower() == FnvHash.Constants.DESCRIBE
                    || lexer.identifierEquals(FnvHash.Constants.RECOVER)
                    || lexer.identifierEquals(FnvHash.Constants.PAUSE)
                    || lexer.identifierEquals(FnvHash.Constants.REMOVE)
                    || lexer.identifierEquals("ROLLBACK")
                    || lexer.identifierEquals("CANCEL")
                    || lexer.identifierEquals("CALL")
                    || lexer.identifierEquals("REFRESH")
                    || lexer.identifierEquals("REBALANCE")
                    || lexer.identifierEquals("UNARCHIVE");
                break;
            default:
                break;
            }
            if (hints.size() >= 1
                && statementList.size() == 0
                && acceptHint) {
                SQLCommentHint hint = hints.get(0);
                String hintText = hint.getText().toUpperCase();
                if (hintText.startsWith("+TDDL")
                    || hintText.startsWith("+ TDDL")
                    || hintText.startsWith("TDDL")
                    || hintText.startsWith("!TDDL")) {
                    tddlHints = true;
                } else if (hintText.startsWith("+")) {
                    accept = true;
                }
            }

            if (tddlHints) {
                SQLStatementImpl stmt = (SQLStatementImpl) this.parseStatementList().get(0);
                stmt.setHeadHints(hints);
                statementList.add(stmt);
                return true;
            } else if (accept) {
                SQLStatementImpl stmt = (SQLStatementImpl) this.parseStatementList().get(0);
                stmt.setHeadHints(hints);
                statementList.add(stmt);
                return true;
            }

            MySqlHintStatement stmt = new MySqlHintStatement();
            stmt.setHints(hints);

            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.BEGIN) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.token() == Token.EOF) {
                statementList.add(new SQLBeginStatement());
            } else {
                lexer.reset(mark);
                statementList.add(this.parseBlock());
            }
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.ADD)) {
            statementList.add(parseAddManageInstanceGroup());
            return true;
        }

        // Label should not in statement.
        if (!inStatement && lexer.token() == Token.IDENTIFIER) {
            String label = lexer.stringVal();
            char ch = lexer.current();
            int bp = lexer.bp();
            lexer.setLexSingleColon(true);
            lexer.nextToken();
            lexer.setLexSingleColon(false);
            if (lexer.token() == Token.COLON) {
                lexer.nextToken();
                if (lexer.token() == Token.LOOP) {
                    // parse loop statement
                    statementList.add(this.parseLoop(label));
                } else if (lexer.token() == Token.WHILE) {
                    // parse while statement with label
                    statementList.add(this.parseWhile(label));
                } else if (lexer.token() == Token.BEGIN) {
                    // parse begin-end statement with label
                    SQLBlockStatement block = this.parseBlock(label);
                    statementList.add(block);
                } else if (lexer.token() == Token.REPEAT) {
                    // parse repeat statement with label
                    statementList.add(this.parseRepeat(label));
                }
                return true;
            } else {
                lexer.reset(bp, ch, Token.IDENTIFIER);
            }

        }

        if (lexer.token() == Token.CHECK) {
            final Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();

            Boolean collectTableName = true;
            if (lexer.token() == Token.ALL) {
                lexer.nextToken();
                collectTableName = false;
            }
            if (lexer.token() == Token.TABLE) {
                lexer.nextToken();

                MySqlCheckTableStatement stmt = new MySqlCheckTableStatement();
                if (lexer.token() != Token.EOF) {
                    if (collectTableName) {
                        for (; ; ) {
                            SQLName table = this.exprParser.name();
                            stmt.addTable(new SQLExprTableSource(table));

                            if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }

                            break;
                        }
                    }
                    if (lexer.token() == WITH) {
                        lexer.nextToken();
                        acceptIdentifier("LOCAL");
                        accept(Token.PARTITION);
                        stmt.setWithLocalPartitions(true);
                    }
                    if (lexer.token() == Token.IDENTIFIER && lexer.stringVal().equalsIgnoreCase("ERROR")) {
                        lexer.nextToken();
                        stmt.setDisplayMode("error");
                    }
                }
                statementList.add(stmt);
            } else if (isEnabled(SQLParserFeature.DrdsGSI) && lexer.identifierEquals(FnvHash.Constants.GLOBAL)) {
                lexer.nextToken();
                accept(Token.INDEX);

                DrdsCheckGlobalIndex checkGlobalIndex = new DrdsCheckGlobalIndex();
                checkGlobalIndex.setIndexName(exprParser.name());
                if (ON == lexer.token()) {
                    lexer.nextToken();
                    checkGlobalIndex.setTableName(exprParser.name());
                }

                if (lexer.token() != Token.EOF && lexer.token() != Token.SEMI && !lexer.stringVal().isEmpty()) {
                    // Extra command.
                    checkGlobalIndex.setExtraCmd(lexer.stringVal());
                    lexer.nextToken();
                }

                statementList.add(checkGlobalIndex);
            }
            return true;
        }

        return false;
    }

    /**
     * REBALANCE TABLE table_name option_list;
     * REBALANCE CLUSTER option_list;
     */
    private SQLStatement parseRebalance(boolean logicalDdl) {
        SQLRebalanceStatement stmt = new SQLRebalanceStatement();
        stmt.setLogicalDdl(logicalDdl);
        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
            stmt.setRebalanceTable();

            SQLName tableName = this.exprParser.name();
            stmt.setTableSource(new SQLExprTableSource(tableName));
        } else if (lexer.identifierEquals("TABLEGROUP")) {
            lexer.nextToken();
            stmt.setRebalanceTableGroup();
            SQLName tableGroupName = this.exprParser.name();
            stmt.setTableSource(new SQLExprTableSource(tableGroupName));
        } else if (lexer.token() == Token.DATABASE) {
            lexer.nextToken();

            stmt.setRebalanceDatabase();
        } else if (lexer.identifierEquals(FnvHash.Constants.CLUSTER)) {
            lexer.nextToken();
            stmt.setRebalanceCluster();
        } else {
            throw new ParserException("only REBALANCE DATABASE/TABLE/CLUSTER/TABLEGROUP supported");
        }

        stmt.setOptions(parseRebalanceOptions());

        return stmt;
    }

    private List<SQLAssignItem> parseRebalanceOptions() {
        List<SQLAssignItem> result = new ArrayList<SQLAssignItem>();
        while (lexer.token() != Token.EOF) {
            SQLIdentifierExpr name = this.exprParser.identifier();
            accept(Token.EQ);
            SQLExpr value = this.exprParser.primary();

            result.add(new SQLAssignItem(name, value));
        }
        return result;
    }

    private SQLStatement parseUnArchiveJob() {
        lexer.nextToken();
        DrdsUnArchiveStatement stmt = new DrdsUnArchiveStatement();
        if (lexer.token() == Token.TABLE) {
            accept(Token.TABLE);
            SQLName name = this.exprParser.name();
            stmt.setTable(new SQLExprTableSource(name));
        } else if (lexer.token() == Token.DATABASE) {
            accept(DATABASE);
            SQLName name = this.exprParser.name();
            stmt.setDatabase(name);
        } else if (lexer.identifierEquals(FnvHash.Constants.TABLEGROUP)) {
            lexer.nextToken();
            SQLName name = this.exprParser.name();
            stmt.setTableGroup(name);
        } else {
            throw new ParserException("only UNARCHIVE DATABASE/TABLEGROUP/TABLE supported");
        }
        return stmt;
    }

    public SQLStatement parsePushDownUdf() {
        lexer.nextToken();
        DrdsPushDownUdfStatement stmt = new DrdsPushDownUdfStatement();
        if (lexer.identifierEquals("UDF")) {
            lexer.nextToken();
            return stmt;
        } else {
            throw new ParserException("only UNARCHIVE DATABASE/TABLEGROUP/TABLE supported");
        }
    }

    private SQLStatement parseArchive() {
        lexer.nextToken();
        accept(Token.TABLE);
        SQLArchiveTableStatement stmt = new SQLArchiveTableStatement();

        SQLName tableName = this.exprParser.name();
        stmt.setTable(tableName);
        stmt.setType(new SQLIdentifierExpr("UPLOAD"));

        if (lexer.token() == Token.LITERAL_INT) {
            for (; ; ) {
                stmt.getSpIdList().add(this.exprParser.integerExpr());
                String pidStr = lexer.stringVal();
                accept(Token.VARIANT);
                String s = pidStr.replaceAll(":", "");
                if (StringUtils.isEmpty(s)) {
                    stmt.getpIdList().add(exprParser.integerExpr());
                } else {
                    stmt.getpIdList().add(new SQLIntegerExpr(Integer.valueOf(s)));
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }
        }
        return stmt;
    }

    private SQLStatement parseBackup() {
        lexer.nextToken();
        SQLBackupStatement stmt = new SQLBackupStatement();

        String type = "BACKUP_DATA";
        String action = "BACKUP";

        if (lexer.identifierEquals(FnvHash.Constants.DATA)) {
            lexer.nextToken();
            accept(Token.INTO);
            type = "BACKUP_DATA";

            for (; ; ) {
                stmt.getProperties().add(new SQLCharExpr(lexer.stringVal()));
                accept(Token.LITERAL_CHARS);
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }
        } else if (lexer.identifierEquals(FnvHash.Constants.LOG)) {
            type = "BACKUP_LOG";
            lexer.nextToken();

            if (lexer.identifierEquals("LIST_LOGS")) {
                lexer.nextToken();
                action = "LIST_LOG";

            } else if (lexer.identifierEquals(FnvHash.Constants.STATUS)) {
                lexer.nextToken();
                action = "STATUS";
            } else if (lexer.token() == Token.INTO) {
                lexer.nextToken();
                for (; ; ) {
                    stmt.getProperties().add(new SQLCharExpr(lexer.stringVal()));
                    accept(Token.LITERAL_CHARS);
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
            }

        } else if (lexer.identifierEquals("CANCEL")) {
            lexer.nextToken();
            type = "BACKUP_DATA";
            action = "BACKUP_CANCEL";
            stmt.getProperties().add(new SQLCharExpr(lexer.stringVal()));
            accept(Token.LITERAL_CHARS);
        }

        stmt.setType(new SQLIdentifierExpr(type));
        stmt.setAction(new SQLIdentifierExpr(action));

        return stmt;
    }

    private SQLStatement parseRestore() {
        lexer.nextToken();

        String type = "DATA";

        SQLRestoreStatement stmt = new SQLRestoreStatement();

        if (lexer.identifierEquals(FnvHash.Constants.DATA)) {
            lexer.nextToken();
            type = "DATA";
        } else if (lexer.identifierEquals(FnvHash.Constants.LOG)) {
            lexer.nextToken();
            type = "LOG";
        }

        stmt.setType(new SQLIdentifierExpr(type));

        accept(Token.FROM);
        for (; ; ) {
            stmt.getProperties().add(new SQLCharExpr(lexer.stringVal()));
            accept(Token.LITERAL_CHARS);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        return stmt;
    }

    private SQLStatement parseBuildTable() {
        lexer.nextToken();

        SQLBuildTableStatement stmt = new SQLBuildTableStatement();

        accept(Token.TABLE);

        stmt.setTable(this.exprParser.name());

        if (lexer.identifierEquals(FnvHash.Constants.VERSION)) {
            lexer.nextToken();
            accept(Token.EQ);
            stmt.setVersion(this.exprParser.integerExpr());
        }

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            acceptIdentifier("SPLIT");

            stmt.setWithSplit(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
            lexer.nextToken();

            accept(EQ);

            if (lexer.token() == TRUE) {
                lexer.nextToken();
                stmt.setForce(true);
            } else if (lexer.token() == FALSE) {
                lexer.nextToken();
                stmt.setForce(false);
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
            lexer.nextToken();
            accept(EQ);

            SQLCharExpr partitions = this.exprParser.charExpr();
            stmt.setPartitions(partitions);
        }

        return stmt;
    }

    private SQLStatement parseCancelJob() {
        lexer.nextToken();

        // DRDS async DDL.
        if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals("DDL")) {
            // CANCEL DDL { ALL | <job_id> [, <job_id>] ... }
            lexer.nextToken();
            DrdsCancelDDLJob cancelDDLJob = new DrdsCancelDDLJob();
            parseGenericDdlCommand(cancelDDLJob);
            return cancelDDLJob;
        }

        SQLCancelJobStatement stmt = new SQLCancelJobStatement();

        if (lexer.identifierEquals("JOB")) {
            lexer.nextToken();
        } else if (lexer.identifierEquals("LOAD_JOB")) {
            lexer.nextToken();
        } else if (lexer.identifierEquals("SYNC_JOB")) {
            lexer.nextToken();
            stmt.setImport(true);
        }

        stmt.setJobName(this.exprParser.name());

        return stmt;

    }

    protected SQLStatement parseExportTable() {
        accept(Token.TABLE);
        SQLExportTableStatement stmt = new SQLExportTableStatement();
        stmt.setTable(
            new SQLExprTableSource(
                this.exprParser.name()));
        return stmt;
    }

    protected SQLStatement parseExportDB() {
        accept(Token.DATABASE);
        SQLExportDatabaseStatement stmt = new SQLExportDatabaseStatement();
        stmt.setDb(this.exprParser.name());

        if (lexer.identifierEquals("REALTIME")) {
            lexer.nextToken();
            accept(Token.EQ);
            if ("y".equalsIgnoreCase(lexer.stringVal())) {
                lexer.nextToken();
                stmt.setRealtime(true);
            } else if ("n".equalsIgnoreCase(lexer.stringVal())) {
                lexer.nextToken();
                stmt.setRealtime(false);
            } else {
                throw new ParserException("Invalid 'realtime' option, should be 'Y' or 'N'. ");
            }
        }

        return stmt;
    }

    protected SQLStatement parseRaftLeaderTransfer() {
        acceptIdentifier("RAFT_LEADER_TRANSFER");
        MySqlRaftLeaderTransferStatement stmt = new MySqlRaftLeaderTransferStatement();

        acceptIdentifier("SHARD");
        accept(Token.EQ);

        stmt.setShard(exprParser.charExpr());

        accept(Token.FROM);
        accept(Token.EQ);
        stmt.setFrom(exprParser.charExpr());

        accept(Token.TO);
        accept(Token.EQ);
        stmt.setTo(exprParser.charExpr());

        acceptIdentifier("TIMEOUT");
        accept(Token.EQ);
        stmt.setTimeout(exprParser.integerExpr());

        return stmt;
    }

    protected SQLStatement parseRaftMemeberChange() {
        acceptIdentifier("RAFT_MEMBER_CHANGE");
        MySqlRaftMemberChangeStatement stmt = new MySqlRaftMemberChangeStatement();

        if (lexer.identifierEquals("NOLEADER")) {
            lexer.nextToken();
            stmt.setNoLeader(true);
        }

        acceptIdentifier("SHARD");
        accept(Token.EQ);

        stmt.setShard(exprParser.charExpr());

        acceptIdentifier("HOST");
        accept(Token.EQ);
        stmt.setHost(exprParser.charExpr());

        acceptIdentifier("STATUS");
        accept(Token.EQ);
        stmt.setStatus(exprParser.charExpr());

        if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
            lexer.nextToken();
            stmt.setForce(true);
        }

        return stmt;
    }

    protected SQLStatement parseMigrate() {
        MySqlMigrateStatement stmt = new MySqlMigrateStatement();
        acceptIdentifier("MIGRATE");
        accept(Token.DATABASE);

        stmt.setSchema(exprParser.name());

        acceptIdentifier("SHARDS");
        accept(Token.EQ);
        stmt.setShardNames(exprParser.charExpr());

        if (lexer.token() == Token.GROUP) {
            lexer.nextToken();
            stmt.setMigrateType(new SQLIntegerExpr(0));
        } else if (lexer.identifierEquals(FnvHash.Constants.HOST)) {
            lexer.nextToken();
            stmt.setMigrateType(new SQLIntegerExpr(1));
        }

        accept(Token.FROM);
        stmt.setFromInsId(exprParser.charExpr());

        if (lexer.token() == Token.VARIANT) {
            lexer.nextToken();
            stmt.setFromInsIp(exprParser.charExpr());

            String variant = lexer.stringVal();
            Integer number = Integer.valueOf(variant.substring(1, variant.length()));
            stmt.setFromInsPort(new SQLIntegerExpr(number));
            accept(Token.VARIANT);

            accept(Token.VARIANT);
            stmt.setFromInsStatus(exprParser.charExpr());
        }

        accept(Token.TO);
        stmt.setToInsId(exprParser.charExpr());

        if (lexer.token() == Token.VARIANT) {
            lexer.nextToken();
            stmt.setToInsIp(exprParser.charExpr());

            String variant = lexer.stringVal();
            Integer number = Integer.valueOf(variant.substring(1, variant.length()));
            stmt.setToInsPort(new SQLIntegerExpr(number));
            accept(Token.VARIANT);

            accept(Token.VARIANT);
            stmt.setToInsStatus(exprParser.charExpr());
        }

        return stmt;
    }

    protected SQLStatement parseImportDB() {

        accept(Token.DATABASE);
        SQLImportDatabaseStatement stmt = new SQLImportDatabaseStatement();

        stmt.setDb(this.exprParser.name());

        if (lexer.identifierEquals(FnvHash.Constants.STATUS)) {
            lexer.nextToken();
            accept(Token.EQ);
            stmt.setStatus(this.exprParser.name());
        }

        return stmt;
    }

    protected SQLStatement parseImportTable() {
        SQLImportTableStatement stmt = new SQLImportTableStatement();
        if (lexer.identifierEquals(FnvHash.Constants.EXTERNAL)) {
            lexer.nextToken();
            stmt.setExtenal(true);
        }

        accept(Token.TABLE);
        stmt.setTable(
            new SQLExprTableSource(
                this.exprParser.name()));

        acceptIdentifier("VERSION");
        accept(Token.EQ);
        stmt.setVersion(this.exprParser.integerExpr());

        if (lexer.identifierEquals("BUILD")) {
            lexer.nextToken();
            accept(Token.EQ);
            if ("Y".equalsIgnoreCase(lexer.stringVal())) {
                lexer.nextToken();
                stmt.setUsingBuild(true);
            } else if ("N".equalsIgnoreCase(lexer.stringVal())) {
                lexer.nextToken();
                stmt.setUsingBuild(false);
            } else {
                throw new ParserException("Invalid 'build' option, should be 'Y' or 'N'. ");
            }
        }

        return stmt;
    }

    protected SQLStatement parseSubmitJob() {
        SQLSubmitJobStatement stmt = new SQLSubmitJobStatement();
        if (lexer.identifierEquals("AWAIT")) {
            lexer.nextToken();
            stmt.setAwait(true);
        }

        stmt.setStatment(this.parseStatement());

        return stmt;
    }

    public SQLStatement parseSync() {
        lexer.nextToken();

        if (lexer.identifierEquals("RAFT_LEADER_TRANSFER")) {
            return parseRaftLeaderTransfer();
        } else if (lexer.identifierEquals("RAFT_MEMBER_CHANGE")) {
            return parseRaftMemeberChange();
        } else {
            acceptIdentifier("META");
            acceptIdentifier("TABLES");

            SQLSyncMetaStatement stmt = new SQLSyncMetaStatement();

            if (lexer.token() == Token.FROM) {
                lexer.nextToken();
                stmt.setFrom(
                    this.exprParser.name()
                );
            }

            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                stmt.setLike(
                    this.exprParser.expr()
                );
            }
            return stmt;
        }

    }

    public SQLStatement parseFlush() {
        acceptIdentifier("FLUSH");
        MySqlFlushStatement stmt = new MySqlFlushStatement();

        if (lexer.identifierEquals("NO_WRITE_TO_BINLOG")) {
            lexer.nextToken();
            stmt.setNoWriteToBinlog(true);
        }

        if (lexer.identifierEquals("LOCAL")) {
            lexer.nextToken();
            stmt.setLocal(true);
        }

        for (; ; ) {
            if (lexer.token() == Token.BINARY || lexer.identifierEquals("BINARY")) {
                lexer.nextToken();
                acceptIdentifier("LOGS");
                stmt.setBinaryLogs(true);
            } else if (lexer.identifierEquals("DES_KEY_FILE")) {
                lexer.nextToken();
                stmt.setDesKeyFile(true);
            } else if (lexer.identifierEquals("ENGINE")) {
                lexer.nextToken();
                acceptIdentifier("LOGS");
                stmt.setEngineLogs(true);
            } else if (lexer.identifierEquals("ERROR")) {
                lexer.nextToken();
                acceptIdentifier("LOGS");
                stmt.setErrorLogs(true);
            } else if (lexer.identifierEquals("GENERAL")) {
                lexer.nextToken();
                acceptIdentifier("LOGS");
                stmt.setGeneralLogs(true);
            } else if (lexer.identifierEquals("HOSTS")) {
                lexer.nextToken();
                stmt.setHots(true);
            } else if (lexer.identifierEquals("LOGS")) {
                lexer.nextToken();
                stmt.setLogs(true);
            } else if (lexer.identifierEquals("PRIVILEGES")) {
                lexer.nextToken();
                stmt.setPrivileges(true);
            } else if (lexer.identifierEquals("OPTIMIZER_COSTS")) {
                lexer.nextToken();
                stmt.setOptimizerCosts(true);
            } else if (lexer.identifierEquals("QUERY")) {
                lexer.nextToken();
                accept(Token.CACHE);
                stmt.setQueryCache(true);
            } else if (lexer.identifierEquals("RELAY")) {
                lexer.nextToken();
                acceptIdentifier("LOGS");
                stmt.setRelayLogs(true);
                if (lexer.token() == FOR) {
                    lexer.nextToken();
                    acceptIdentifier("CHANNEL");
                    stmt.setRelayLogsForChannel(this.exprParser.primary());
                }
            } else if (lexer.identifierEquals("SLOW")) {
                lexer.nextToken();
                acceptIdentifier("LOGS");
                stmt.setSlowLogs(true);
            } else if (lexer.identifierEquals(STATUS)) {
                lexer.nextToken();
                stmt.setStatus(true);
            } else if (lexer.identifierEquals("USER_RESOURCES")) {
                lexer.nextToken();
                stmt.setUserResources(true);
            } else if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            } else {
                break;
            }
        }

        if (lexer.identifierEquals("TABLES") || lexer.token() == Token.TABLE) {
            lexer.nextToken();

            stmt.setTableOption(true);

            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                acceptIdentifier("READ");
                accept(Token.LOCK);
                stmt.setWithReadLock(true);
            }
            for (; ; ) {
                if (lexer.token() == Token.IDENTIFIER) {
                    for (; ; ) {
                        SQLName name = this.exprParser.name();
                        stmt.addTable(name);

                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    break;
                }
                break;
            }

            if (stmt.getTables().size() != 0) {
                if (lexer.token() == FOR) {
                    lexer.nextToken();
                    acceptIdentifier("EXPORT");
                    stmt.setForExport(true);
                } else if (lexer.token() == Token.WITH) {
                    lexer.nextToken();
                    acceptIdentifier("READ");
                    accept(Token.LOCK);
                    stmt.setWithReadLock(true);
                } else if (lexer.identifierEquals(FnvHash.Constants.VERSION)) {
                    lexer.nextToken();
                    accept(Token.EQ);
                    stmt.setVersion(this.exprParser.integerExpr());
                }
            }

        }

        return stmt;
    }

    /**
     * always add an outer begin-end block to handle include all statements
     */
    public SQLBlockStatement parseImplicitBlock() {
        SQLBlockStatement block = new SQLBlockStatement();
        block.setDbType(dbType);
        block.setImplicit(true);
        List<SQLStatement> statementList = block.getStatementList();
        this.parseStatementList(statementList, -1, block);
        checkDuplicateDeclareItems(block);
        return block;
    }

    public SQLBlockStatement parseBlock() {
        SQLBlockStatement block = new SQLBlockStatement();
        block.setDbType(dbType);

        accept(Token.BEGIN);
        if (lexer.token() == Token.SEMI) {
            throw new RuntimeException("unexpect semi(';') after begin");
        }
        List<SQLStatement> statementList = block.getStatementList();
        this.parseStatementList(statementList, -1, block);

        if (lexer.token() != Token.END
            && statementList.size() > 0
            && (statementList.get(statementList.size() - 1) instanceof SQLCommitStatement
            || statementList.get(statementList.size() - 1) instanceof SQLRollbackStatement)) {
            block.setEndOfCommit(true);
            return block;
        }
        accept(Token.END);

        checkDuplicateDeclareItems(block);

        block.setAfterSemi(true);
        return block;
    }

    private void checkDuplicateDeclareItems(SQLBlockStatement blockStmt) {
        checkDuplicateCursorName(blockStmt);
        checkDuplicateParamName(blockStmt);
    }

    private void checkDuplicateCursorName(SQLBlockStatement blockStmt) {
        List<String> names = blockStmt.getStatementList().stream().filter(t -> t instanceof MySqlCursorDeclareStatement)
            .map(t -> ((MySqlCursorDeclareStatement) t).getCursorName().getSimpleName().toLowerCase()).collect(
                Collectors.toList());
        if (names.size() != new HashSet<>(names).size()) {
            throw new RuntimeException("found duplicate cursor");
        }
    }

    private void checkDuplicateParamName(SQLBlockStatement blockStmt) {
        List<List<String>> items = blockStmt.getStatementList().stream().filter(t -> t instanceof MySqlDeclareStatement)
            .map(t -> ((MySqlDeclareStatement) t).getVarList().stream()
                .map(var -> var.getName().getSimpleName().toLowerCase()).collect(
                    Collectors.toList())).collect(
                Collectors.toList());
        int totalCount = items.stream().mapToInt(List::size).sum();
        HashSet<String> uniqueNames = new HashSet<>();
        for (List<String> names : items) {
            uniqueNames.addAll(names);
        }
        if (totalCount != uniqueNames.size()) {
            throw new RuntimeException("found duplicate parameter");
        }
    }

    public MySqlExplainStatement parseDescribe() {
        // see https://dev.mysql.com/doc/refman/5.7/en/explain.html
        MySqlExplainStatement describe = new MySqlExplainStatement();

        // {DESCRIBE | DESC}
        if (lexer.token() == Token.DESC || lexer.identifierEquals(DESCRIBE)) {
            lexer.nextToken();
            describe.setDescribe(true);
        } else {
            throw new ParserException(
                "expect one of {DESCRIBE | DESC} , actual " + lexer.token() + ", " + lexer.info());
        }

        return parseExplain(describe);
    }

    public MySqlExplainStatement parseExplain() {
        // see https://dev.mysql.com/doc/refman/5.7/en/explain.html
        MySqlExplainStatement explain = new MySqlExplainStatement();
        explain.setSourceLine(lexer.getPosLine());
        explain.setSourceLine(lexer.getPosColumn());

        // {EXPLAIN}
        if (lexer.token() == Token.EXPLAIN) {
            lexer.nextToken();
        } else {
            throw new ParserException("expect EXPLAIN , actual " + lexer.token() + ", " + lexer.info());
        }

        return parseExplain(explain);
    }

    private MySqlExplainStatement parseExplain(MySqlExplainStatement explain) {

        if (lexer.identifierEquals(FnvHash.Constants.PLAN)) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.token() == FOR) {
                lexer.nextToken();
            } else {
                lexer.reset(mark);
            }
        }

        if (lexer.token() == Token.ANALYZE) {
            lexer.nextToken();
            explain.setType("ANALYZE");
        } else if (lexer.identifierEquals("LOGICALVIEW")) {
            lexer.nextToken();
            explain.setType("LOGICALVIEW");
        } else if (lexer.identifierEquals("LOGIC")) {
            lexer.nextToken();
            explain.setType("LOGIC");
        } else if (lexer.identifierEquals("SIMPLE")) {
            lexer.nextToken();
            explain.setType("SIMPLE");
        } else if (lexer.identifierEquals("DETAIL")) {
            lexer.nextToken();
            explain.setType("DETAIL");
        } else if (lexer.identifierEquals("EXECUTE")) {
            lexer.nextToken();
            explain.setType("EXECUTE");
        } else if (lexer.identifierEquals("PHYSICAL")) {
            lexer.nextToken();
            explain.setType("PHYSICAL");
        } else if (lexer.identifierEquals("OPTIMIZER")) {
            lexer.nextToken();
            explain.setType("OPTIMIZER");
        } else if (lexer.identifierEquals("SHARDING")) {
            lexer.nextToken();
            explain.setType("SHARDING");
        } else if (lexer.identifierEquals("COST")) {
            lexer.nextToken();
            explain.setType("COST");
        } else if (lexer.identifierEquals("VEC")) {
            lexer.nextToken();
            explain.setType("VEC");
        } else if (lexer.identifierEquals("BASELINE")) {
            lexer.nextToken();
            explain.setType("BASELINE");
        } else if (lexer.identifierEquals("JSON_PLAN")) {
            lexer.nextToken();
            explain.setType("JSON_PLAN");
        } else if (lexer.identifierEquals("ADVISOR")) {
            lexer.nextToken();
            explain.setType("ADVISOR");
        } else if (lexer.identifierEquals("STATISTICS")) {
            lexer.nextToken();
            explain.setType("STATISTICS");
        }

        if (lexer.token() == Token.HINT) {
            List<SQLCommentHint> hints = this.exprParser.parseHints();
            explain.setHints(hints);
        }
        // see https://dev.mysql.com/doc/refman/5.7/en/explain.html

        switch (dbType) {
            case mysql:
                Lexer.SavePoint mark = lexer.mark();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                if (lexer.token() == Token.SELECT) {
                    lexer.reset(mark);
                    break;
                }

                for (; ; ) {
                    if (lexer.identifierEquals("FORMAT")) {
                        lexer.nextToken();
                        String format = lexer.stringVal();
                        explain.setFormat(format);
                        lexer.nextToken();
                    } else if (lexer.identifierEquals("TYPE")) {
                        lexer.nextToken();
                        String type = lexer.stringVal();
                        explain.setType(type);
                        lexer.nextToken();
                    } else {
                        break;
                    }

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    } else {
                        break;
                    }
                }
                accept(Token.RPAREN);
                explain.setParenthesis(true);
            }
            break;
        default:
            break;
        }

        boolean table = false;
        if (lexer.token() == Token.IDENTIFIER) {
            final long hash = lexer.hash_lower();
            String stringVal = lexer.stringVal();
            if (hash == FnvHash.Constants.CALL) {
                throw new RuntimeException("explain call statement is not support!");
            } else if (hash == FnvHash.Constants.EXTENDED) {
                explain.setExtended(true);
                lexer.nextToken();
            } else if (hash == FnvHash.Constants.PARTITIONS && !explain.isDescribe()) {
                explain.setType(stringVal);
                lexer.nextToken();
            } else if (hash == FnvHash.Constants.OPTIMIZER) {
                explain.setOptimizer(true);
                lexer.nextToken();
            } else if (hash == FnvHash.Constants.FORMAT) {
                lexer.nextToken();
                accept(Token.EQ);

                String format = lexer.stringVal();
                explain.setFormat(format);
                accept(Token.IDENTIFIER);
            } else {
                explain.setTableName(exprParser.name());
                if (lexer.token() == Token.IDENTIFIER) {
                    explain.setColumnName(exprParser.name());
                } else if (lexer.token() == Token.LITERAL_CHARS) {
                    explain.setWild(exprParser.expr());
                }
                table = true;
            }
        }

        if (lexer.token() == Token.DISTRIBUTE) {
            lexer.nextToken();
            acceptIdentifier("INFO");
            explain.setDistributeInfo(true);
        }

        if (lexer.token() == FOR) {
            lexer.nextToken();
            acceptIdentifier(CONNECTION);
            explain.setConnectionId(exprParser.expr());
        } else if (!table) {
            explain.setStatement(this.parseStatement());
        }

        return explain;
    }

    public SQLStatement parseShow() {
        accept(Token.SHOW);

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
        }

        boolean isPhysical = false;
        boolean full = false;

        if (lexer.identifierEquals(FnvHash.Constants.PHYSICAL)) {
            lexer.nextToken();
            isPhysical = true;
        }

        if (lexer.identifierEquals("REBALANCE")) {
            lexer.nextToken();
            DrdsShowRebalanceBackFill showRebalanceBackFill = new DrdsShowRebalanceBackFill();
            return showRebalanceBackFill;
        }

        if (lexer.token() == Token.FULL) {
            lexer.nextToken();
            full = true;
        } else if (lexer.token() == Token.HINT) {
            String hints = lexer.stringVal().toLowerCase();
            if (hints.endsWith(" full")
                && hints.length() <= 11
                && hints.charAt(0) == '!'
                && hints.charAt(1) == '5') {
                lexer.nextToken();
                full = true;
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.STATS)) {
            lexer.nextToken();
            SQLShowStatisticStmt showStats = new SQLShowStatisticStmt();
            showStats.setDbType(DbType.mysql);
            if (full) {
                showStats.setFull(true);
            }
            return showStats;
        }

        if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals(FnvHash.Constants.CCL_RULES)) {
            lexer.nextToken();
            DrdsShowCclRuleStatement showCclRules = new DrdsShowCclRuleStatement();
            showCclRules.setAllRules(true);
            return showCclRules;
        }

        if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals(FnvHash.Constants.CCL_RULE)) {
            lexer.nextToken();
            DrdsShowCclRuleStatement showCclRule = new DrdsShowCclRuleStatement();
            SQLName sqlName = exprParser.name();
            List<SQLName> ruleNames = new ArrayList<SQLName>();
            ruleNames.add(sqlName);
            while (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                ruleNames.add(exprParser.name());
            }
            showCclRule.setRuleNames(ruleNames);
            return showCclRule;
        }

        if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals(FnvHash.Constants.CCL_TRIGGERS)) {
            lexer.nextToken();
            DrdsShowCclTriggerStatement showCclTriggers = new DrdsShowCclTriggerStatement();
            showCclTriggers.setAll(true);
            return showCclTriggers;
        }

        if (isEnabled(SQLParserFeature.DrdsCCL) && lexer.identifierEquals(FnvHash.Constants.CCL_TRIGGER)) {
            lexer.nextToken();
            DrdsShowCclTriggerStatement showCclTrigger = new DrdsShowCclTriggerStatement();
            SQLName sqlName = exprParser.name();
            List<SQLName> triggerNames = Lists.newArrayList();
            triggerNames.add(sqlName);
            while (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                triggerNames.add(exprParser.name());
            }
            showCclTrigger.setTriggerNames(triggerNames);
            return showCclTrigger;
        }

        if (lexer.identifierEquals(FnvHash.Constants.PROCESSLIST)) {
            lexer.nextToken();
            MySqlShowProcessListStatement stmt;
            if (isPhysical) {
                stmt = new MySqlShowPhysicalProcesslistStatement();
            } else {
                stmt = new MySqlShowProcessListStatement();
            }
            stmt.setFull(full);

            if (!full) {
                if (lexer.identifierEquals(FnvHash.Constants.MPP)) {
                    lexer.nextToken();
                    stmt.setMpp(true);
                }
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(
                    this.exprParser.expr());
            }

            if (lexer.token() == Token.ORDER) {
                stmt.setOrderBy(
                    this.exprParser.parseOrderBy());
            }

            if (lexer.token() == Token.LIMIT) {
                stmt.setLimit(
                    this.exprParser.parseLimit());
            }

            return stmt;
        }

        if (lexer.identifierEquals("COLUMNS") || lexer.identifierEquals("FIELDS")) {
            lexer.nextToken();

            SQLShowColumnsStatement stmt = parseShowColumns();
            stmt.setFull(full);

            return stmt;
        }

        if (lexer.identifierEquals("COLUMNS")) {
            lexer.nextToken();

            SQLShowColumnsStatement stmt = parseShowColumns();

            return stmt;
        }

        if (lexer.identifierEquals(TABLES)) {
            lexer.nextToken();

            SQLShowTablesStatement stmt = parseShowTables();
            stmt.setFull(full);

            return stmt;
        }

        if (lexer.identifierEquals("DATABASES")) {
            lexer.nextToken();

            SQLShowDatabasesStatement stmt = parseShowDatabases(isPhysical);

            if (full) {
                stmt.setFull(true);
            }

            return stmt;
        }

        if (lexer.identifierEquals("WARNINGS")) {
            lexer.nextToken();

            MySqlShowWarningsStatement stmt = parseShowWarnings();

            return stmt;
        }

        if (lexer.identifierEquals("COUNT")) {
            lexer.nextToken();
            accept(Token.LPAREN);
            accept(Token.STAR);
            accept(Token.RPAREN);

            if (lexer.identifierEquals(FnvHash.Constants.ERRORS)) {
                lexer.nextToken();

                MySqlShowErrorsStatement stmt = new MySqlShowErrorsStatement();
                stmt.setCount(true);

                return stmt;
            } else {
                acceptIdentifier("WARNINGS");

                MySqlShowWarningsStatement stmt = new MySqlShowWarningsStatement();
                stmt.setCount(true);

                return stmt;
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.ERRORS)) {
            lexer.nextToken();

            MySqlShowErrorsStatement stmt = new MySqlShowErrorsStatement();
            stmt.setLimit(this.exprParser.parseLimit());

            return stmt;
        }

        if (lexer.identifierEquals(STATUS)) {
            lexer.nextToken();

            MySqlShowStatusStatement stmt = parseShowStatus();

            return stmt;
        }

        if (lexer.identifierEquals(DBLOCK)) {
            lexer.nextToken();

            return new MysqlShowDbLockStatement();
        }

        if (lexer.identifierEquals(VARIABLES)) {
            lexer.nextToken();

            MySqlShowVariantsStatement stmt = parseShowVariants();

            return stmt;
        }

        if (lexer.identifierEquals(GLOBAL)) {
            lexer.nextToken();

            if (lexer.identifierEquals(STATUS)) {
                lexer.nextToken();
                MySqlShowStatusStatement stmt = parseShowStatus();
                stmt.setGlobal(true);
                return stmt;
            }

            if (lexer.identifierEquals(VARIABLES)) {
                lexer.nextToken();
                MySqlShowVariantsStatement stmt = parseShowVariants();
                stmt.setGlobal(true);
                return stmt;
            }

            // DRDS GSI syntax.
            if (isEnabled(SQLParserFeature.DrdsGSI) && (Token.INDEX == lexer.token() || lexer
                .identifierEquals("INDEXES"))) {
                lexer.nextToken();

                DrdsShowGlobalIndex stmt = new DrdsShowGlobalIndex();
                if (Token.FROM == lexer.token()) {
                    lexer.nextToken();
                    stmt.setTableName(this.exprParser.name());
                }
                return stmt;
            }

            // show global deadlocks syntax
            if (lexer.identifierEquals(DEADLOCKS)) {
                lexer.nextToken();
                DrdsShowGlobalDeadlocks stmt = new DrdsShowGlobalDeadlocks();
                return stmt;
            }
        }

        if (lexer.identifierEquals(LOCAL)) {
            lexer.nextToken();
            acceptIdentifier(DEADLOCKS);
            return new DrdsShowLocalDeadlocks();
        }

        if (lexer.identifierEquals(FnvHash.Constants.SESSION)) {
            lexer.nextToken();

            if (lexer.identifierEquals(STATUS)) {
                lexer.nextToken();
                MySqlShowStatusStatement stmt = parseShowStatus();
                stmt.setSession(true);
                return stmt;
            }

            if (lexer.identifierEquals(VARIABLES)) {
                lexer.nextToken();
                MySqlShowVariantsStatement stmt = parseShowVariants();
                stmt.setSession(true);
                return stmt;
            }

            SQLShowSessionStatement stmt = new SQLShowSessionStatement();
            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                SQLExpr like = this.exprParser.expr();
                stmt.setLike(like);
            }
            return stmt;
        }

        if (lexer.identifierEquals("COBAR_STATUS")) {
            lexer.nextToken();
            return new CobarShowStatus();
        }

        if (lexer.identifierEquals("AUTHORS")) {
            lexer.nextToken();
            return new MySqlShowAuthorsStatement();
        }

        if (lexer.token() == Token.BINARY) {
            lexer.nextToken();
            acceptIdentifier("LOGS");
            return new MySqlShowBinaryLogsStatement();
        }

        if (lexer.identifierEquals("MASTER")) {
            lexer.nextToken();
            if (lexer.identifierEquals("LOGS")) {
                lexer.nextToken();
                return new MySqlShowMasterLogsStatement();
            }
            acceptIdentifier(STATUS);
            return new MySqlShowMasterStatusStatement();
        }

        if (lexer.identifierEquals("CLUSTER") || lexer.token() == Token.CLUSTER) {
            lexer.nextToken();
            acceptIdentifier("NAME");
            return new MySqlShowClusterNameStatement();
        }

        if (lexer.identifierEquals("SYNC_JOB")) {
            lexer.nextToken();
            acceptIdentifier(STATUS);
            MySqlShowJobStatusStatement stmt = new MySqlShowJobStatusStatement();
            stmt.setSync(true);
            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }
            return stmt;
        }
        if (lexer.identifierEquals("JOB")) {
            lexer.nextToken();
            acceptIdentifier(STATUS);

            MySqlShowJobStatusStatement stmt = new MySqlShowJobStatusStatement();
            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }
            return stmt;
        }
        if (lexer.identifierEquals("MIGRATE")) {
            lexer.nextToken();

            acceptIdentifier("TASK");
            acceptIdentifier(STATUS);

            MySqlShowMigrateTaskStatusStatement stmt = new MySqlShowMigrateTaskStatusStatement();
            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.CHARSET)) {
            lexer.nextToken();

            MySqlShowCharacterSetStatement stmt = new MySqlShowCharacterSetStatement();

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                stmt.setPattern(this.exprParser.expr());
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
            lexer.nextToken();
            accept(Token.SET);
            MySqlShowCharacterSetStatement stmt = new MySqlShowCharacterSetStatement();

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                stmt.setPattern(this.exprParser.expr());
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }

            return stmt;
        }

        if (lexer.identifierEquals("COLLATION")) {
            lexer.nextToken();
            MySqlShowCollationStatement stmt = new MySqlShowCollationStatement();

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                stmt.setPattern(this.exprParser.expr());
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }

            return stmt;
        }

        if (lexer.identifierEquals(BINLOG)) {
            lexer.nextToken();
            acceptIdentifier(EVENTS);
            MySqlShowBinLogEventsStatement stmt = new MySqlShowBinLogEventsStatement();

            if (lexer.token() == Token.IN) {
                lexer.nextToken();
                stmt.setIn(this.exprParser.expr());
            }

            if (lexer.token() == Token.FROM) {
                lexer.nextToken();
                stmt.setFrom(this.exprParser.expr());
            }

            stmt.setLimit(this.exprParser.parseLimit());

            return stmt;
        }

        if (lexer.identifierEquals("CONTRIBUTORS")) {
            lexer.nextToken();
            return new MySqlShowContributorsStatement();
        }

        if (lexer.token() == Token.ALL) {
            lexer.nextToken();
            accept(Token.CREATE);
            accept(Token.TABLE);

            SQLShowCreateTableStatement stmt = new SQLShowCreateTableStatement();
            stmt.setAll(true);
            stmt.setName(this.exprParser.name());
            return stmt;
        }

        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();

            if (lexer.token() == Token.DATABASE || lexer.token() == Token.SCHEMA) {
                lexer.nextToken();

                MySqlShowCreateDatabaseStatement stmt = new MySqlShowCreateDatabaseStatement();
                if (lexer.token() == Token.IF) {
                    lexer.nextToken();
                    accept(Token.NOT);
                    accept(Token.EXISTS);
                    stmt.setIfNotExists(true);
                }
                stmt.setDatabase(this.exprParser.name());
                return stmt;
            }

            if (lexer.identifierEquals("EVENT")) {
                lexer.nextToken();

                MySqlShowCreateEventStatement stmt = new MySqlShowCreateEventStatement();
                stmt.setEventName(this.exprParser.name());
                return stmt;
            }

            if (lexer.token() == Token.FUNCTION) {
                lexer.nextToken();

                MySqlShowCreateFunctionStatement stmt = new MySqlShowCreateFunctionStatement();
                stmt.setName(this.exprParser.name());
                return stmt;
            }

            if (lexer.token() == Token.PROCEDURE) {
                lexer.nextToken();

                MySqlShowCreateProcedureStatement stmt = new MySqlShowCreateProcedureStatement();
                stmt.setName(this.exprParser.name());
                return stmt;
            }
            if (lexer.token() == Token.TABLE) {
                lexer.nextToken();
                SQLShowCreateTableStatement stmt = new SQLShowCreateTableStatement();

                if (lexer.token() != Token.LIKE) {
                    stmt.setName(this.exprParser.name());
                }

                if (lexer.token() == Token.LIKE) {
                    lexer.nextToken();
                    if (lexer.identifierEquals(FnvHash.Constants.MAPPING)) {
                        lexer.nextToken();
                        accept(Token.LPAREN);
                        SQLName name = this.exprParser.name();
                        stmt.setLikeMapping(name);
                        accept(Token.RPAREN);
                    }
                }
                stmt.setFull(full);
                return stmt;
            }

            if (lexer.token() == Token.VIEW) {
                lexer.nextToken();

                SQLShowCreateViewStatement stmt = new SQLShowCreateViewStatement();
                stmt.setName(this.exprParser.name());
                return stmt;
            }

            if (lexer.token() == Token.TRIGGER) {
                lexer.nextToken();

                MySqlShowCreateTriggerStatement stmt = new MySqlShowCreateTriggerStatement();
                stmt.setName(this.exprParser.name());
                return stmt;
            }

            if (lexer.token() == Token.FULLTEXT) {
                lexer.nextToken();
                MysqlShowCreateFullTextStatement stmt = new MysqlShowCreateFullTextStatement();

                stmt.setType(parseFullTextType());

                stmt.setName(this.exprParser.name());
                return stmt;
            }

            if (lexer.identifierEquals("MATERIALIZED")) {
                lexer.nextToken();
                SQLShowCreateMaterializedViewStatement stmt = new SQLShowCreateMaterializedViewStatement();
                accept(VIEW);

                stmt.setName(this.exprParser.name());
                return stmt;
            }

            throw new ParserException("TODO " + lexer.info());
        }

        if (lexer.identifierEquals(ENGINE)) {
            lexer.nextToken();
            MySqlShowEngineStatement stmt = new MySqlShowEngineStatement();
            stmt.setName(this.exprParser.name());
            stmt.setOption(MySqlShowEngineStatement.Option.valueOf(lexer.stringVal().toUpperCase()));
            lexer.nextToken();
            return stmt;
        }

        if (lexer.token() == Token.DATABASE || lexer.identifierEquals(FnvHash.Constants.DB)) {
            lexer.nextToken();

            MySqlShowDatabaseStatusStatement stmt = new MySqlShowDatabaseStatusStatement();

            if (full) {
                stmt.setFull(true);
            }

            if (lexer.identifierEquals("STATUS")) {

                lexer.nextToken();

                if (lexer.token() == Token.LIKE) {
                    lexer.nextTokenValue();
                    stmt.setName(this.exprParser.name());
                } else {
                    if (lexer.token() == Token.WHERE) {
                        lexer.nextToken();
                        SQLExpr where = exprParser.expr();
                        stmt.setWhere(where);
                    }

                    if (lexer.token() == Token.ORDER) {
                        SQLOrderBy orderBy = exprParser.parseOrderBy();
                        stmt.setOrderBy(orderBy);
                    }

                    if (lexer.token() == Token.LIMIT) {
                        SQLLimit limit = exprParser.parseLimit();
                        stmt.setLimit(limit);
                    }
                }
            }
            return stmt;
        }

        if (lexer.identifierEquals("STORAGE")) {
            lexer.nextToken();
            MySqlShowEnginesStatement stmt = new MySqlShowEnginesStatement();

            if (lexer.identifierEquals("ENGINES")) {
                lexer.nextToken();
            }
            stmt.setStorage(true);
            return stmt;
        }

        if (lexer.identifierEquals(ENGINES)) {
            lexer.nextToken();
            MySqlShowEnginesStatement stmt = new MySqlShowEnginesStatement();
            stmt.setStorage(false);
            return stmt;
        }

        if (lexer.identifierEquals(EVENTS)) {
            lexer.nextToken();
            MySqlShowEventsStatement stmt = new MySqlShowEventsStatement();

            if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
                lexer.nextToken();
                stmt.setSchema(this.exprParser.name());
            }

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                stmt.setLike(this.exprParser.expr());
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }
            return stmt;
        }

        if (lexer.token() == Token.FUNCTION) {
            lexer.nextToken();

            if (lexer.identifierEquals("CODE")) {
                lexer.nextToken();
                MySqlShowFunctionCodeStatement stmt = new MySqlShowFunctionCodeStatement();
                stmt.setName(this.exprParser.name());
                return stmt;
            }

            acceptIdentifier(STATUS);
            MySqlShowFunctionStatusStatement stmt = new MySqlShowFunctionStatusStatement();

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                stmt.setLike(this.exprParser.expr());
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }
            return stmt;
        }

        // MySqlShowFunctionStatusStatement

        if (lexer.identifierEquals(ENGINE)) {
            lexer.nextToken();
            MySqlShowEngineStatement stmt = new MySqlShowEngineStatement();
            stmt.setName(this.exprParser.name());
            stmt.setOption(MySqlShowEngineStatement.Option.valueOf(lexer.stringVal().toUpperCase()));
            lexer.nextToken();
            return stmt;
        }

        if (lexer.identifierEquals("STORAGE")) {
            lexer.nextToken();
            accept(Token.EQ);
            accept(Token.DEFAULT);
            MySqlShowEnginesStatement stmt = new MySqlShowEnginesStatement();
            stmt.setStorage(true);
            return stmt;
        }

//        if (lexer.identifierEquals("KEYS") || lexer.token() == Token.INDEX) {
//            lexer.nextToken();
//            accept(Token.FROM);
//            MySqlShowKeysStatement stmt = new MySqlShowKeysStatement();
//            stmt.setTable(exprParser.name());
//            return stmt;
//        }

        if (lexer.identifierEquals("GRANTS")) {
            lexer.nextToken();
            MySql8ShowGrantsStatement stmt = new MySql8ShowGrantsStatement();
            MySqlExprParser mysqlExprParser = (MySqlExprParser) exprParser;

            if (lexer.token() == FOR) {
                lexer.nextToken();
                SQLExpr expr = mysqlExprParser.expr();
                stmt.setUser(expr);
                if (expr instanceof SQLIdentifierExpr) {
                    if (((SQLIdentifierExpr) expr).nameEquals("CURRENT_USER")) {
                        stmt.setUserSpec(MySql8ShowGrantsStatement.UserSpec.CURRENT_USER);
                    } else {
                        stmt.setUsername(MySqlUserName.fromExpr(expr));
                        stmt.setUserSpec(MySql8ShowGrantsStatement.UserSpec.USERNAME);
                    }
                } else if (expr instanceof SQLMethodInvokeExpr) {
                    if ("CURRENT_USER".equalsIgnoreCase(((SQLMethodInvokeExpr) expr).getMethodName())) {
                        stmt.setUserSpec(MySql8ShowGrantsStatement.UserSpec.CURRENT_USER_FUNC);
                    } else {
                        throw new ParserException("syntax error: " + ((SQLMethodInvokeExpr) expr).getMethodName());
                    }
                } else if (expr instanceof SQLCurrentUserExpr) {
                    stmt.setUserSpec(MySql8ShowGrantsStatement.UserSpec.CURRENT_USER);
                } else {
                    stmt.setUsername(MySqlUserName.fromExpr(expr));
                    stmt.setUserSpec(MySql8ShowGrantsStatement.UserSpec.USERNAME);
                }
            }

            if (lexer.identifierEquals("USING")) {
                lexer.nextToken();
                while (true) {
                    MySqlUserName role = MySqlUserName.fromExpr(mysqlExprParser.userName());
                    stmt.addRoleToUse(role);

                    if (lexer.token() != COMMA) {
                        break;
                    } else {
                        lexer.nextToken();
                    }
                }
            }

            if (lexer.token() == ON) {
                lexer.nextToken();
                SQLExpr on = this.exprParser.expr();
                stmt.setOn(on);
            }

            return stmt;
        }

        if (lexer.token() == Token.INDEX || lexer.identifierEquals("INDEXES") || lexer.identifierEquals("KEYS")) {

            SQLShowIndexesStatement stmt = new SQLShowIndexesStatement();

            stmt.setType(lexer.stringVal());
            lexer.nextToken();

            if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
                lexer.nextToken();
                SQLName table = exprParser.name();
                stmt.setTable(table);

                if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
                    lexer.nextToken();
                    SQLName database = exprParser.name();
                    stmt.setDatabase(database.toString());
                }

                if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    SQLExpr where = exprParser.expr();
                    stmt.setWhere(where);
                }
            } else {
                throw new ParserException(
                    "syntax error, expect FROM or IN, actual " + lexer.token() + ", " + lexer.info());
            }

            if (lexer.token() == Token.HINT) {
                stmt.setHints(this.exprParser.parseHints());
            }

            return stmt;
        }

        if (lexer.token() == Token.OPEN || lexer.identifierEquals("OPEN")) {
            lexer.nextToken();
            acceptIdentifier(TABLES);
            MySqlShowOpenTablesStatement stmt = new MySqlShowOpenTablesStatement();

            if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
                lexer.nextToken();
                stmt.setDatabase(this.exprParser.name());
            }

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                stmt.setLike(this.exprParser.expr());
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }
            return stmt;
        }

        if (lexer.identifierEquals("PLUGINS")) {
            lexer.nextToken();
            MySqlShowPluginsStatement stmt = new MySqlShowPluginsStatement();
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.ROUTE)) {
            lexer.nextToken();
            MysqlShowRouteStatement stmt = new MysqlShowRouteStatement();
            accept(FROM);
            SQLName name = exprParser.name();
            stmt.setName(name);
            return stmt;
        }

        if (lexer.identifierEquals("HTC")) {
            lexer.nextToken();
            MysqlShowHtcStatement stmt = new MysqlShowHtcStatement();
            stmt.setFull(false);
            return stmt;
        }

        if (lexer.identifierEquals("TRANS")) {
            lexer.nextToken();
            DrdsShowTransStatement stmt = new DrdsShowTransStatement();
            return stmt;
        }

        if (lexer.identifierEquals("HMSMETA")) {
            lexer.nextToken();
            SQLName name = this.exprParser.name();

            MySqlShowHMSMetaStatement stmt = new MySqlShowHMSMetaStatement();
            stmt.setName(name);
            return stmt;
        }

        if (lexer.identifierEquals("STC")) {
            lexer.nextToken();
            MysqlShowStcStatement stmt = new MysqlShowStcStatement();
            if (lexer.identifierEquals("HIS")) {
                lexer.nextToken();
                stmt.setHis(true);
            } else {
                stmt.setHis(false);
            }
            return stmt;
        }

        if (lexer.identifierEquals("PRIVILEGES")) {
            lexer.nextToken();
            MySqlShowPrivilegesStatement stmt = new MySqlShowPrivilegesStatement();
            return stmt;
        }

        if (lexer.token() == Token.PROCEDURE) {
            lexer.nextToken();

            if (lexer.identifierEquals("CODE")) {
                lexer.nextToken();
                MySqlShowProcedureCodeStatement stmt = new MySqlShowProcedureCodeStatement();
                stmt.setName(this.exprParser.name());
                return stmt;
            }

            acceptIdentifier(STATUS);
            MySqlShowProcedureStatusStatement stmt = new MySqlShowProcedureStatusStatement();

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                stmt.setLike(this.exprParser.expr());
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(this.exprParser.expr());
            }
            return stmt;
        }

        if (lexer.identifierEquals("PROFILES")) {
            lexer.nextToken();
            MySqlShowProfilesStatement stmt = new MySqlShowProfilesStatement();
            return stmt;
        }

        if (lexer.identifierEquals("PROFILE")) {
            lexer.nextToken();
            MySqlShowProfileStatement stmt = new MySqlShowProfileStatement();

            for (; ; ) {
                if (lexer.token() == Token.ALL) {
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.ALL);
                    lexer.nextToken();
                } else if (lexer.identifierEquals("BLOCK")) {
                    lexer.nextToken();
                    acceptIdentifier("IO");
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.BLOCK_IO);
                } else if (lexer.identifierEquals("CONTEXT")) {
                    lexer.nextToken();
                    acceptIdentifier("SWITCHES");
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.CONTEXT_SWITCHES);
                } else if (lexer.identifierEquals("CPU")) {
                    lexer.nextToken();
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.CPU);
                } else if (lexer.identifierEquals("IPC")) {
                    lexer.nextToken();
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.IPC);
                } else if (lexer.identifierEquals("MEMORY")) {
                    lexer.nextToken();
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.MEMORY);
                } else if (lexer.identifierEquals("PAGE")) {
                    lexer.nextToken();
                    acceptIdentifier("FAULTS");
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.PAGE_FAULTS);
                } else if (lexer.identifierEquals("SOURCE")) {
                    lexer.nextToken();
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.SOURCE);
                } else if (lexer.identifierEquals("SWAPS")) {
                    lexer.nextToken();
                    stmt.getTypes().add(MySqlShowProfileStatement.Type.SWAPS);
                } else {
                    break;
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            if (lexer.token() == FOR) {
                lexer.nextToken();
                acceptIdentifier("QUERY");
                stmt.setForQuery(this.exprParser.primary());
            }

            stmt.setLimit(this.exprParser.parseLimit());

            return stmt;
        }

        if (lexer.identifierEquals("RELAYLOG")) {
            lexer.nextToken();
            acceptIdentifier(EVENTS);
            MySqlShowRelayLogEventsStatement stmt = new MySqlShowRelayLogEventsStatement();

            if (lexer.token() == Token.IN) {
                lexer.nextToken();
                stmt.setLogName(this.exprParser.primary());
            }

            if (lexer.token() == Token.FROM) {
                lexer.nextToken();
                stmt.setFrom(this.exprParser.primary());
            }

            stmt.setLimit(this.exprParser.parseLimit());

            return stmt;
        }

        if (lexer.identifierEquals("RELAYLOG")) {
            lexer.nextToken();
            acceptIdentifier(EVENTS);
            MySqlShowRelayLogEventsStatement stmt = new MySqlShowRelayLogEventsStatement();

            if (lexer.token() == Token.IN) {
                lexer.nextToken();
                stmt.setLogName(this.exprParser.primary());
            }

            if (lexer.token() == Token.FROM) {
                lexer.nextToken();
                stmt.setFrom(this.exprParser.primary());
            }

            stmt.setLimit(this.exprParser.parseLimit());

            return stmt;
        }

        if (lexer.identifierEquals("SLAVE")) {
            lexer.nextToken();
            if (lexer.identifierEquals(STATUS)) {
                lexer.nextToken();
                MySqlShowSlaveStatusStatement stmt = new MySqlShowSlaveStatusStatement();
                if (lexer.token() == FOR) {
                    lexer.nextToken();
                    acceptIdentifier("CHANNEL");
                    SQLCharExpr channel = this.exprParser.charExpr();
                    stmt.setChannel(channel);
                }
                return stmt;
            } else {
                acceptIdentifier("HOSTS");
                MySqlShowSlaveHostsStatement stmt = new MySqlShowSlaveHostsStatement();
                return stmt;
            }
        }

        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
            if (lexer.identifierEquals(STATUS)) {
                acceptIdentifier(STATUS);
                MySqlShowTableStatusStatement stmt = new MySqlShowTableStatusStatement();
                if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
                    lexer.nextToken();

                    SQLName name = this.exprParser.name();
                    if (name instanceof SQLPropertyExpr) {
                        stmt.setDatabase((SQLIdentifierExpr) ((SQLPropertyExpr) name).getOwner());
                        stmt.setTableGroup(new SQLIdentifierExpr(((SQLPropertyExpr) name).getName()));
                    } else {
                        stmt.setDatabase(name);
                    }
                }

                if (lexer.token() == Token.LIKE) {
                    lexer.nextTokenValue();
                    stmt.setLike(this.exprParser.expr());
                }

                if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    stmt.setWhere(this.exprParser.expr());
                }

                return stmt;
            } else {
                acceptIdentifier(INFO);
                MySqlShowTableInfoStatement stmt = new MySqlShowTableInfoStatement();

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();
                }
                stmt.setName(exprParser.name());

                if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    SQLExpr where = exprParser.expr();
                    stmt.setWhere(where);
                }

                if (lexer.token() == Token.ORDER) {
                    SQLOrderBy orderBy = exprParser.parseOrderBy();
                    stmt.setOrderBy(orderBy);
                }

                if (lexer.token() == Token.LIMIT) {
                    SQLLimit limit = exprParser.parseLimit();
                    stmt.setLimit(limit);
                }

                stmt.setFull(full);

                return stmt;
            }
        }

        if (lexer.identifierEquals("TRIGGERS")) {
            lexer.nextToken();
            MySqlShowTriggersStatement stmt = new MySqlShowTriggersStatement();

            if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
                lexer.nextToken();
                SQLName database = exprParser.name();
                stmt.setDatabase(database);
            }

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                SQLExpr like = exprParser.expr();
                stmt.setLike(like);
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.BROADCASTS)) {
            lexer.nextToken();
            MySqlShowBroadcastsStatement stmt = new MySqlShowBroadcastsStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.DATASOURCES)) {
            lexer.nextToken();
            MySqlShowDatasourcesStatement stmt = new MySqlShowDatasourcesStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.NODE)) {
            lexer.nextToken();
            MySqlShowNodeStatement stmt = new MySqlShowNodeStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.HELP)) {
            lexer.nextToken();
            MySqlShowHelpStatement stmt = new MySqlShowHelpStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.token() == Token.SEQUENCE || lexer.identifierEquals(FnvHash.Constants.SEQUENCES)) {
            lexer.nextToken();
            MySqlShowSequencesStatement stmt = new MySqlShowSequencesStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals("PARTITIONS")) {
            lexer.nextToken();
            if (lexer.identifierEquals("HEATMAP")) {
                SQLShowPartitionsHeatmapStatement stmt = new SQLShowPartitionsHeatmapStatement();
                lexer.nextToken();
                SQLName timeRange = exprParser.name();
                stmt.setTimeRange(timeRange);
                SQLExpr type = exprParser.expr();
                stmt.setType(type);
                return stmt;
            }
            SQLShowPartitionsStmt stmt = new SQLShowPartitionsStmt();

            if (lexer.token() == Token.FROM) {
                lexer.nextToken();
            }

            SQLName name = exprParser.name();
            stmt.setTableSource(name);

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                accept(Token.LPAREN);
                parseAssignItems(stmt.getPartition(), stmt, false);
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(
                    this.exprParser.expr()
                );
            }

            return stmt;
        }

        if (lexer.identifierEquals("RULE")) {
            lexer.nextToken();

            boolean version = false;
            if (lexer.identifierEquals(FnvHash.Constants.VERSION)) {
                version = true;
                lexer.nextToken();
            } else if (lexer.identifierEquals(FnvHash.Constants.FULL) || lexer.token() == Token.FULL) {
                full = true;
                lexer.nextToken();
            }

            if (lexer.identifierEquals(FnvHash.Constants.STATUS)) {
                lexer.nextToken();
                MySqlShowRuleStatusStatement stmt = new MySqlShowRuleStatusStatement();

                if (full) {
                    stmt.setFull(full);
                }

                if (version) {
                    stmt.setVersion(true);
                }

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();
                }

                if (full) {
                    stmt.setLite(false);
                }

                if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    SQLExpr where = exprParser.expr();
                    stmt.setWhere(where);
                }

                if (lexer.token() == Token.ORDER) {
                    SQLOrderBy orderBy = exprParser.parseOrderBy();
                    stmt.setOrderBy(orderBy);
                }

                if (lexer.token() == Token.LIMIT) {
                    SQLLimit limit = exprParser.parseLimit();
                    stmt.setLimit(limit);
                }

                return stmt;
            } else {

                MySqlShowRuleStatement stmt = new MySqlShowRuleStatement();

                if (full) {
                    stmt.setFull(full);
                }

                if (version) {
                    stmt.setVersion(true);
                }

                if (lexer.identifierEquals(FnvHash.Constants.VERSION)) {
                    lexer.nextToken();
                    stmt.setVersion(true);
                }

                if (lexer.token() == Token.EOF || lexer.token() == Token.SEMI) {
                    return stmt;
                }

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();

                    stmt.setName(exprParser.name());
                }

                if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    SQLExpr where = exprParser.expr();
                    stmt.setWhere(where);
                }

                if (lexer.token() == Token.ORDER) {
                    SQLOrderBy orderBy = exprParser.parseOrderBy();
                    stmt.setOrderBy(orderBy);
                }

                if (lexer.token() == Token.LIMIT) {
                    SQLLimit limit = exprParser.parseLimit();
                    stmt.setLimit(limit);
                }
                return stmt;
            }

        }

        if (lexer.identifierEquals("DS")) {
            lexer.nextToken();
            MySqlShowDsStatement stmt = new MySqlShowDsStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals("SCHEDULE")) {
            lexer.nextToken();

            if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals(FnvHash.Constants.RESULT)) {
                // SHOW DDL RESULT [ ALL | <job_id> [ , <job_id> ] ... ]
                lexer.nextToken();
                DrdsShowScheduleResultStatement showScheduleResult = new DrdsShowScheduleResultStatement();
                showScheduleResult.setScheduleId(lexer.integerValue().longValue());
                accept(Token.LITERAL_INT);
                return showScheduleResult;
            }
        }

        if (lexer.identifierEquals("DDL")) {
            lexer.nextToken();

            if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals(FnvHash.Constants.RESULT)) {
                // SHOW DDL RESULT [ ALL | <job_id> [ , <job_id> ] ... ]
                lexer.nextToken();
                DrdsShowDDLResults showDDLResult = new DrdsShowDDLResults();
                parseGenericDdlCommand(showDDLResult, true);
                return showDDLResult;
            }

            if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && !lexer.identifierEquals("STATUS")) {
                // SHOW [FULL] DDL [ <job_id> [ , <job_id> ] ... ].
                DrdsShowDDLJobs showDDLJobs = new DrdsShowDDLJobs();
                showDDLJobs.setFull(full);
                parseGenericDdlCommand(showDDLJobs, true);
                return showDDLJobs;
            }

            acceptIdentifier("STATUS");
            MySqlShowDdlStatusStatement stmt = new MySqlShowDdlStatusStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals("TRACE")) {
            lexer.nextToken();
            MySqlShowTraceStatement stmt = new MySqlShowTraceStatement();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                List<SQLExpr> exprs = new ArrayList<SQLExpr>(8);
                exprParser.exprList(exprs, stmt);

                accept(Token.RPAREN);
                stmt.setSelectList(exprs);
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals("TOPOLOGY")) {
            lexer.nextToken();
            MySqlShowTopologyStatement stmt = new MySqlShowTopologyStatement();

            if (lexer.token() == Token.FROM) {
                lexer.nextToken();
            }
            stmt.setName(exprParser.name());

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            stmt.setFull(full);

            return stmt;
        }

        if (lexer.identifierEquals("FILES")) {
            lexer.nextToken();
            MySqlShowFilesStatement stmt = new MySqlShowFilesStatement();
            if (lexer.token() == Token.FROM) {
                lexer.nextToken();
            }
            stmt.setName(exprParser.name());
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.PLANCACHE)) {
            lexer.nextToken();

            if (lexer.identifierEquals(FnvHash.Constants.STATUS)) {
                lexer.nextToken();
                return new MySqlShowPlanCacheStatusStatement();
            } else if (lexer.identifierEquals(FnvHash.Constants.PLAN)) {
                lexer.nextToken();
                SQLSelect select = this.createSQLSelectParser().select();
                return new MySqlShowPlanCacheStatement(select);
            } else {
                throw new ParserException("TODO " + lexer.info());
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.SLOW)) {
            MySqlShowSlowStatement stmt = parserShowSlow();
            stmt.setPhysical(false);
            stmt.setFull(full);
            return stmt;
        }

        if (lexer.identifierEquals("PHYSICAL_SLOW")) {
            MySqlShowSlowStatement stmt = parserShowSlow();
            stmt.setPhysical(true);
            stmt.setFull(full);
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.QUERY_TASK)) {
            lexer.nextToken();
            SQLShowQueryTaskStatement stmt = new SQLShowQueryTaskStatement();
            stmt.setDbType(dbType);

            if (lexer.token() == FOR) {
                lexer.nextToken();
                stmt.setUser(
                    exprParser.expr()
                );
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = this.exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = this.exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = this.exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            if (full) {
                stmt.setFull(true);
            }

            return stmt;
        }
        if (lexer.identifierEquals(FnvHash.Constants.OUTLINES)) {
            lexer.nextToken();
            SQLShowOutlinesStatement stmt = new SQLShowOutlinesStatement();
            stmt.setDbType(dbType);

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = this.exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = this.exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = this.exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.RECYCLEBIN)) {
            lexer.nextToken();

            SQLShowRecyclebinStatement stmt = new SQLShowRecyclebinStatement();
            stmt.setDbType(dbType);
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.TABLEGROUPS)) {
            lexer.nextToken();

            SQLShowTableGroupsStatement stmt = parseShowTableGroups();

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.CATALOGS)) {
            lexer.nextToken();

            SQLShowCatalogsStatement stmt = new SQLShowCatalogsStatement();
            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                SQLExpr like = this.exprParser.expr();
                stmt.setLike(like);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.FUNCTIONS)) {
            lexer.nextToken();

            SQLShowFunctionsStatement stmt = new SQLShowFunctionsStatement();
            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                SQLExpr like = this.exprParser.expr();
                stmt.setLike(like);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.SCHEMAS)) {
            lexer.nextToken();

            SQLShowDatabasesStatement stmt = new SQLShowDatabasesStatement();
            stmt.setPhysical(isPhysical);
            if (lexer.token() == Token.IN || lexer.token() == Token.FROM) {
                lexer.nextToken();
                SQLName db = this.exprParser.name();
                stmt.setDatabase(db);
            }

            if (full) {
                stmt.setFull(true);
            }

            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                SQLExpr like = this.exprParser.expr();
                stmt.setLike(like);
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.CONFIG)) {
            lexer.nextToken();
            SQLName name = this.exprParser.name();
            MySqlShowConfigStatement stmt = new MySqlShowConfigStatement();
            stmt.setName(name);
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.USERS)) {
            lexer.nextToken();
            SQLShowUsersStatement stmt = new SQLShowUsersStatement();
            stmt.setDbType(dbType);
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.PHYSICAL_PROCESSLIST)) {
            lexer.nextToken();

            MySqlShowPhysicalProcesslistStatement stmt = new MySqlShowPhysicalProcesslistStatement();
            if (full) {
                stmt.setFull(full);
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(
                    this.exprParser.expr());
            }

            if (lexer.token() == Token.ORDER) {
                stmt.setOrderBy(
                    this.exprParser.parseOrderBy());
            }

            if (lexer.token() == Token.LIMIT) {
                stmt.setLimit(
                    this.exprParser.parseLimit());
            }

            return stmt;
        }

        if (lexer.identifierEquals("MATERIALIZED")) {
            return parseShowMaterializedView();
        }

        if (lexer.token() == Token.FULLTEXT) {
            lexer.nextToken();

            MysqlShowFullTextStatement stmt = new MysqlShowFullTextStatement();

            if (lexer.identifierEquals(FnvHash.Constants.CHARFILTERS)) {
                stmt.setType(FullTextType.CHARFILTER);
            } else if (lexer.identifierEquals(FnvHash.Constants.TOKENIZERS)) {
                stmt.setType(FullTextType.TOKENIZER);
            } else if (lexer.identifierEquals(FnvHash.Constants.TOKENFILTERS)) {
                stmt.setType(FullTextType.TOKENFILTER);
            } else if (lexer.identifierEquals(FnvHash.Constants.ANALYZERS)) {
                stmt.setType(FullTextType.ANALYZER);
            } else if (lexer.identifierEquals(FnvHash.Constants.DICTIONARIES)) {
                stmt.setType(FullTextType.DICTIONARY);
            } else {
                throw new ParserException(
                    "type of full text must be [CHARFILTERS/TOKENIZERS/TOKENFILTERS/ANALYZERS/DICTIONARYS] .");
            }

            lexer.nextToken();

            return stmt;
        }

        // DRDS GSI management syntax.
        if (isEnabled(SQLParserFeature.DrdsGSI) && lexer.identifierEquals("METADATA")) {
            lexer.nextToken();

            if (Token.LOCK == lexer.token() || lexer.identifierEquals("LOCKS")) {
                lexer.nextToken();

                DrdsShowMetadataLock stmt = new DrdsShowMetadataLock();
                if (Token.SEMI == lexer.token() || Token.EOF == lexer.token()) {
                    return stmt;
                } else {
                    stmt.setSchemaName(this.exprParser.name());
                    return stmt;
                }
            } else {
                throw new ParserException(
                    "syntax error, expect LOCK/LOCKS, actual " + lexer.token() + ", " + lexer.info());
            }
        }

        // for scaleout management purpose
        // show move database where...orderBy..limit
        if (lexer.identifierEquals("MOVE") && isEnabled(SQLParserFeature.DrdsMisc)) {
            lexer.nextToken();
            accept(Token.DATABASE);
            DrdsShowMoveDatabaseStatement stmt = new DrdsShowMoveDatabaseStatement();

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }

            return stmt;
        }

        if (lexer.identifierEquals("TABLEGROUP") && isEnabled(SQLParserFeature.DrdsMisc)) {
            lexer.nextToken();
            DrdsShowTableGroup stmt = new DrdsShowTableGroup();
            stmt.setFull(full);
            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                SQLExpr where = exprParser.expr();
                stmt.setWhere(where);
            }

            if (lexer.token() == Token.ORDER) {
                SQLOrderBy orderBy = exprParser.parseOrderBy();
                stmt.setOrderBy(orderBy);
            }

            if (lexer.token() == Token.LIMIT) {
                SQLLimit limit = exprParser.parseLimit();
                stmt.setLimit(limit);
            }
            return stmt;
        }

        if (lexer.identifierEquals("LOCALITY")) {
            lexer.nextToken();
            DrdsShowLocality stmt = new DrdsShowLocality();
            return stmt;
        }

        if (lexer.identifierEquals("LOCALITY")) {
            lexer.nextToken();
            DrdsShowLocality stmt = new DrdsShowLocality();
            return stmt;
        }

        // MySqlShowSlaveHostsStatement

        throw new ParserException("TODO " + lexer.info());
    }

    private MySqlShowStatusStatement parseShowStatus() {
        MySqlShowStatusStatement stmt = new MySqlShowStatusStatement();

        if (lexer.token() == Token.LIKE) {
            lexer.nextToken();
            SQLExpr like = exprParser.expr();
            stmt.setLike(like);
        }

        if (lexer.token() == Token.WHERE) {
            lexer.nextToken();
            SQLExpr where = exprParser.expr();
            stmt.setWhere(where);
        }

        return stmt;
    }

    public MySqlShowSlowStatement parserShowSlow() {
        lexer.nextToken();
        MySqlShowSlowStatement stmt = new MySqlShowSlowStatement();

        if (lexer.token() == Token.WHERE) {
            lexer.nextToken();
            SQLExpr where = exprParser.expr();
            stmt.setWhere(where);
        }

        if (lexer.token() == Token.ORDER) {
            SQLOrderBy orderBy = exprParser.parseOrderBy();
            stmt.setOrderBy(orderBy);
        }

        if (lexer.token() == Token.LIMIT) {
            SQLLimit limit = exprParser.parseLimit();
            stmt.setLimit(limit);
        }

        return stmt;
    }

    private MySqlShowVariantsStatement parseShowVariants() {
        MySqlShowVariantsStatement stmt = new MySqlShowVariantsStatement();

        if (lexer.token() == Token.LIKE) {
            lexer.nextToken();
            SQLExpr like = exprParser.expr();
            stmt.setLike(like);
        }

        if (lexer.token() == Token.WHERE) {
            lexer.nextToken();
            SQLExpr where = exprParser.expr();
            stmt.setWhere(where);
        }

        return stmt;
    }

    private MySqlShowWarningsStatement parseShowWarnings() {
        MySqlShowWarningsStatement stmt = new MySqlShowWarningsStatement();

        stmt.setLimit(this.exprParser.parseLimit());

        return stmt;
    }

    public MySqlChangeMasterStatement parseChangeMaster() {
        acceptIdentifier("MASTER");
        if (lexer.token() == Token.TO) {
            lexer.nextToken();
            MySqlChangeMasterStatement stmt = new MySqlChangeMasterStatement();
            parseAssignItems(stmt.getOptions(), stmt, true);
            if (lexer.token() == FOR) {
                lexer.nextToken();
                acceptIdentifier("CHANNEL");
                SQLCharExpr channel = this.exprParser.charExpr();
                stmt.setChannel(channel);
            }
            return stmt;
        }
        return null;
    }

    public MySqlChangeReplicationFilterStatement parseChangeReplication() {
        acceptIdentifier("REPLICATION");
        acceptIdentifier("FILTER");
        MySqlChangeReplicationFilterStatement stmt = new MySqlChangeReplicationFilterStatement();
        parseAssignItems(stmt.getFilters(), stmt, true);
        if (lexer.token() == FOR) {
            lexer.nextToken();
            acceptIdentifier("CHANNEL");
            SQLCharExpr channel = this.exprParser.charExpr();
            stmt.setChannel(channel);
        }
        return stmt;
    }

    public MySqlStartSlaveStatement parseStartSlave() {
        lexer.nextToken();
        MySqlStartSlaveStatement stmt = new MySqlStartSlaveStatement();
        if (lexer.token() == FOR) {
            lexer.nextToken();
            acceptIdentifier("CHANNEL");
            SQLCharExpr channel = this.exprParser.charExpr();
            stmt.setChannel(channel);
        } else {
            if (lexer.token() != Token.EOF) {
                setErrorEndPos(lexer.pos());
                throw new ParserException(
                    "syntax error, only support START {SLAVE | REPLICA} [channel_option] , " + lexer
                        .info());
            }
        }
        return stmt;
    }

    public MySqlStopSlaveStatement parseStoptSlave() {
        lexer.nextToken();
        MySqlStopSlaveStatement stmt = new MySqlStopSlaveStatement();
        if (lexer.token() == FOR) {
            lexer.nextToken();
            acceptIdentifier("CHANNEL");
            SQLCharExpr channel = this.exprParser.charExpr();
            stmt.setChannel(channel);
        } else {
            if (lexer.token() != Token.EOF) {
                setErrorEndPos(lexer.pos());
                throw new ParserException(
                    "syntax error, only support STOP {SLAVE | REPLICA} [channel_option] , " + lexer
                        .info());
            }
        }
        return stmt;
    }

    public SQLStartTransactionStatement parseStart() {
        acceptIdentifier("TRANSACTION");

        SQLStartTransactionStatement stmt = new SQLStartTransactionStatement(dbType);

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            acceptIdentifier("CONSISTENT");
            acceptIdentifier("SNAPSHOT");
            stmt.setConsistentSnapshot(true);
        }

        if (lexer.token() == Token.BEGIN) {
            lexer.nextToken();
            stmt.setBegin(true);
            if (lexer.identifierEquals("WORK")) {
                lexer.nextToken();
                stmt.setWork(true);
            }
        }

        if (lexer.token() == Token.HINT) {
            stmt.setHints(this.exprParser.parseHints());
        }

        if (lexer.identifierEquals(FnvHash.Constants.ISOLATION)) {
            lexer.nextToken();
            acceptIdentifier("LEVEL");

            if (lexer.identifierEquals(FnvHash.Constants.READ)) {
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.UNCOMMITTED)) {
                    lexer.nextToken();
                    stmt.setIsolationLevel(SQLStartTransactionStatement.IsolationLevel.READ_UNCOMMITTED);
                } else if (lexer.identifierEquals(FnvHash.Constants.COMMITTED)) {
                    lexer.nextToken();
                    stmt.setIsolationLevel(SQLStartTransactionStatement.IsolationLevel.READ_COMMITTED);
                } else {
                    throw new ParserException(lexer.info());
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.REPEATABLE)) {
                lexer.nextToken();
                acceptIdentifier("READ");
                stmt.setIsolationLevel(SQLStartTransactionStatement.IsolationLevel.REPEATABLE_READ);
            } else if (lexer.identifierEquals(FnvHash.Constants.SERIALIZABLE)) {
                lexer.nextToken();
                stmt.setIsolationLevel(SQLStartTransactionStatement.IsolationLevel.SERIALIZABLE);
            } else {
                throw new ParserException(lexer.info());
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.READ)) {
            lexer.nextToken();
            if (identifierEquals("ONLY")) {
                stmt.setReadOnly(true);
                lexer.nextToken();
            } else {
                acceptIdentifier("WRITE");
            }
        }

        return stmt;
    }

    @Override
    public SQLStatement parseRollback() {
        acceptIdentifier("ROLLBACK");

        if (isEnabled(SQLParserFeature.DRDSAsyncDDL) && lexer.identifierEquals("DDL")) {
            // ROLLBACK DDL { ALL | <job_id> [, <job_id>] ... }
            lexer.nextToken();
            DrdsRollbackDDLJob stmt = new DrdsRollbackDDLJob();
            parseGenericDdlCommand(stmt);
            return stmt;
        }

        SQLRollbackStatement stmt = new SQLRollbackStatement();

        if (lexer.identifierEquals("WORK")) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.AND) {
            lexer.nextToken();
            if (lexer.token() == Token.NOT) {
                lexer.nextToken();
                acceptIdentifier(CHAIN);
                stmt.setChain(Boolean.FALSE);
            } else {
                acceptIdentifier(CHAIN);
                stmt.setChain(Boolean.TRUE);
            }
        }

        if (lexer.token() == Token.TO) {
            lexer.nextToken();

            if (lexer.identifierEquals("SAVEPOINT")) {
                lexer.nextToken();
            }

            stmt.setTo(this.exprParser.name());
        }

        return stmt;
    }

    public SQLStatement parseCommit() {
        acceptIdentifier("COMMIT");

        SQLCommitStatement stmt = new SQLCommitStatement();

        if (lexer.identifierEquals("WORK")) {
            lexer.nextToken();
            stmt.setWork(true);
        }

        if (lexer.token() == Token.AND) {
            lexer.nextToken();
            if (lexer.token() == Token.NOT) {
                lexer.nextToken();
                acceptIdentifier(CHAIN);
                stmt.setChain(Boolean.FALSE);
            } else {
                acceptIdentifier(CHAIN);
                stmt.setChain(Boolean.TRUE);
            }
        }

        return stmt;
    }

    public SQLReplaceStatement parseReplace() {
        SQLReplaceStatement stmt = new SQLReplaceStatement();
        stmt.setDbType(DbType.mysql);

        List<SQLCommentHint> list = new ArrayList<SQLCommentHint>();

        while (lexer.token() == Token.HINT) {
            this.exprParser.parseHints(list);
        }
        stmt.setHeadHints(list);

        accept(Token.REPLACE);
        while (lexer.token() == Token.HINT) {
            this.exprParser.parseHints(stmt.getHints());
        }
        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
        }

        if (lexer.identifierEquals(FnvHash.Constants.LOW_PRIORITY)) {
            stmt.setLowPriority(true);
            lexer.nextToken();
        }

        if (lexer.identifierEquals(FnvHash.Constants.DELAYED)) {
            stmt.setDelayed(true);
            lexer.nextToken();
        }

        if (lexer.token() == Token.INTO) {
            lexer.nextToken();
        }

        SQLName tableName = exprParser.name();
        stmt.setTableName(tableName);

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();
            accept(Token.LPAREN);
            for (; ; ) {
                SQLAssignItem ptExpr = new SQLAssignItem();
                ptExpr.setTarget(this.exprParser.name());
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                    SQLExpr ptValue = this.exprParser.expr();
                    ptExpr.setValue(ptValue);
                }
                stmt.addPartition(ptExpr);
                if (lexer.token() != Token.COMMA) {
                    break;
                } else {
                    lexer.nextToken();
                }
            }
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            if (lexer.token() == Token.SELECT) {
                SQLQueryExpr queryExpr = (SQLQueryExpr) this.exprParser.expr();
                stmt.setQuery(queryExpr);
            } else {
                this.exprParser.exprList(stmt.getColumns(), stmt);
            }
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.VALUES || lexer.identifierEquals("VALUE")) {
            lexer.nextToken();

            parseValueClause(stmt.getValuesList(), null, 0, stmt);
        } else if (lexer.token() == Token.SELECT) {
            SQLQueryExpr queryExpr = (SQLQueryExpr) this.exprParser.expr();
            stmt.setQuery(queryExpr);
        } else if (lexer.token() == Token.SET) {
            lexer.nextToken();

            SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
            values.setParent(stmt);
            stmt.getValuesList().add(values);
            for (; ; ) {
                stmt.addColumn(this.exprParser.name());
                if (lexer.token() == Token.COLONEQ) {
                    lexer.nextToken();
                } else {
                    accept(Token.EQ);
                }
                values.addValue(this.exprParser.expr());

                if (lexer.token() == (Token.COMMA)) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }
        } else if (lexer.token() == Token.LPAREN) {
            SQLSelect select = this.createSQLSelectParser().select();
            SQLQueryExpr queryExpr = new SQLQueryExpr(select);
            stmt.setQuery(queryExpr);
        }

        return stmt;
    }

    protected SQLStatement parseLoad() {
        acceptIdentifier("LOAD");

        if (lexer.identifierEquals("DATA")) {
            SQLStatement stmt = parseLoadDataInFile();
            return stmt;
        }

        if (lexer.identifierEquals("XML")) {
            SQLStatement stmt = parseLoadXml();
            return stmt;
        }

        throw new ParserException("TODO. " + lexer.info());
    }

    protected MySqlLoadXmlStatement parseLoadXml() {
        acceptIdentifier("XML");

        MySqlLoadXmlStatement stmt = new MySqlLoadXmlStatement();

        if (lexer.identifierEquals(FnvHash.Constants.LOW_PRIORITY)) {
            stmt.setLowPriority(true);
            lexer.nextToken();
        }

        if (lexer.identifierEquals("CONCURRENT")) {
            stmt.setConcurrent(true);
            lexer.nextToken();
        }

        if (lexer.identifierEquals(LOCAL)) {
            stmt.setLocal(true);
            lexer.nextToken();
        }

        acceptIdentifier("INFILE");

        SQLLiteralExpr fileName = (SQLLiteralExpr) exprParser.expr();
        stmt.setFileName(fileName);

        if (lexer.token() == Token.REPLACE) {
            stmt.setReplicate(true);
            lexer.nextToken();
        }

        if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
            stmt.setIgnore(true);
            lexer.nextToken();
        }

        accept(Token.INTO);
        accept(Token.TABLE);

        SQLName tableName = exprParser.name();
        stmt.setTableName(tableName);

        if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
            lexer.nextToken();
            accept(Token.SET);

            if (lexer.token() != Token.LITERAL_CHARS) {
                throw new ParserException("syntax error, illegal charset. " + lexer.info());
            }

            String charset = lexer.stringVal();
            lexer.nextToken();
            stmt.setCharset(charset);
        }

        if (lexer.identifierEquals("ROWS")) {
            lexer.nextToken();
            acceptIdentifier("IDENTIFIED");
            accept(Token.BY);
            SQLExpr rowsIdentifiedBy = exprParser.expr();
            stmt.setRowsIdentifiedBy(rowsIdentifiedBy);
        }

        if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.token() == Token.SET) {
            throw new ParserException("TODO. " + lexer.info());
        }

        return stmt;
    }

    protected MySqlLoadDataInFileStatement parseLoadDataInFile() {

        acceptIdentifier("DATA");

        MySqlLoadDataInFileStatement stmt = new MySqlLoadDataInFileStatement();

        if (lexer.identifierEquals(FnvHash.Constants.LOW_PRIORITY)) {
            stmt.setLowPriority(true);
            lexer.nextToken();
        }

        if (lexer.identifierEquals("CONCURRENT")) {
            stmt.setConcurrent(true);
            lexer.nextToken();
        }

        if (lexer.identifierEquals(LOCAL)) {
            stmt.setLocal(true);
            lexer.nextToken();
        }

        acceptIdentifier("INFILE");

        SQLLiteralExpr fileName = (SQLLiteralExpr) exprParser.expr();
        stmt.setFileName(fileName);

        if (lexer.token() == Token.REPLACE) {
            stmt.setReplicate(true);
            lexer.nextToken();
        }

        if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
            stmt.setIgnore(true);
            lexer.nextToken();
        }

        accept(Token.INTO);
        accept(Token.TABLE);

        SQLName tableName = exprParser.name();
        stmt.setTableName(tableName);

        if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
            lexer.nextToken();
            accept(Token.SET);

            if (lexer.token() != Token.LITERAL_CHARS) {
                throw new ParserException("syntax error, illegal charset. " + lexer.info());
            }

            String charset = lexer.stringVal();
            lexer.nextToken();
            stmt.setCharset(charset);
        }

        if (lexer.identifierEquals("FIELDS") || lexer.identifierEquals("COLUMNS")) {
            lexer.nextToken();
            if (lexer.identifierEquals("TERMINATED")) {
                lexer.nextToken();
                accept(Token.BY);

                if (lexer.token() == Token.LITERAL_CHARS) {
                    stmt.setColumnsTerminatedBy(new SQLCharExpr(lexer.stringVal()));
                    lexer.nextToken();
                } else {
                    SQLExpr primary = this.exprParser.primary();
                    if (primary instanceof SQLHexExpr) {
                        stmt.setColumnsTerminatedBy((SQLHexExpr) primary);
                    } else {
                        throw new ParserException("invalid expr for columns terminated : " + primary);
                    }
                }
            }

            if (lexer.identifierEquals("OPTIONALLY")) {
                stmt.setColumnsEnclosedOptionally(true);
                lexer.nextToken();
            }

            if (lexer.identifierEquals("ENCLOSED")) {
                lexer.nextToken();
                accept(Token.BY);
                stmt.setColumnsEnclosedBy(new SQLCharExpr(lexer.stringVal()));
                lexer.nextToken();
            }

            if (lexer.identifierEquals("ESCAPED")) {
                lexer.nextToken();
                accept(Token.BY);
                stmt.setColumnsEscaped(new SQLCharExpr(lexer.stringVal()));
                lexer.nextToken();
            }
        }

        if (lexer.identifierEquals("LINES")) {
            lexer.nextToken();
            if (lexer.identifierEquals("STARTING")) {
                lexer.nextToken();
                accept(Token.BY);

                if (lexer.token() == Token.LITERAL_CHARS) {
                    stmt.setLinesStartingBy(new SQLCharExpr(lexer.stringVal()));
                    lexer.nextToken();
                } else {
                    SQLExpr primary = this.exprParser.primary();
                    if (primary instanceof SQLHexExpr) {
                        stmt.setLinesStartingBy((SQLHexExpr) primary);
                    } else {
                        throw new ParserException("invalid expr for lines starting : " + primary);
                    }
                }
            }

            if (lexer.identifierEquals("TERMINATED")) {
                lexer.nextToken();
                accept(Token.BY);
                if (lexer.token() == Token.LITERAL_CHARS) {
                    stmt.setLinesTerminatedBy(new SQLCharExpr(lexer.stringVal()));
                    lexer.nextToken();
                } else {
                    SQLExpr primary = this.exprParser.primary();
                    if (primary instanceof SQLHexExpr) {
                        stmt.setLinesTerminatedBy((SQLHexExpr) primary);
                    } else {
                        throw new ParserException("invalid expr for lines terminated : " + primary);
                    }
                }
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
            lexer.nextToken();
            stmt.setIgnoreLinesNumber(this.exprParser.expr());
            acceptIdentifier("LINES");
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            this.exprParser.exprList(stmt.getColumns(), stmt);
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.SET) {
            lexer.nextToken();
            this.exprParser.exprList(stmt.getSetList(), stmt);
        }

        return stmt;

    }

    public MySqlPrepareStatement parsePrepare() {
        acceptIdentifier("PREPARE");

        SQLName name = exprParser.name();
        accept(Token.FROM);
        SQLExpr from = exprParser.expr();

        return new MySqlPrepareStatement(name, from);
    }

    public MySqlExecuteStatement parseExecute() {
        MySqlExecuteStatement stmt = new MySqlExecuteStatement();

        SQLName statementName = exprParser.name();
        stmt.setStatementName(statementName);

        if (lexer.identifierEquals("USING")) {
            lexer.nextToken();
            exprParser.exprList(stmt.getParameters(), stmt);
        } else if (lexer.token() == Token.IDENTIFIER) {
            exprParser.exprList(stmt.getParameters(), stmt);
        }
        return stmt;
    }

    public MySqlExecuteForAdsStatement parseExecuteForAds() {
        MySqlExecuteForAdsStatement stmt = new MySqlExecuteForAdsStatement();
        stmt.setAction(exprParser.name());
        stmt.setRole(exprParser.name());

        stmt.setTargetId(exprParser.charExpr());

        if (lexer.token() == Token.IDENTIFIER) {
            stmt.setStatus(exprParser.name());
        }
        return stmt;
    }

    public MysqlDeallocatePrepareStatement parseDeallocatePrepare() {
        acceptIdentifier("DEALLOCATE");
        acceptIdentifier("PREPARE");

        MysqlDeallocatePrepareStatement stmt = new MysqlDeallocatePrepareStatement();
        SQLName statementName = exprParser.name();
        stmt.setStatementName(statementName);

        return stmt;
    }

    public SQLInsertStatement parseInsert() {
        MySqlInsertStatement stmt = new MySqlInsertStatement();

        SQLName tableName = null;
        if (lexer.token() == Token.INSERT) {
            lexer.nextToken();

            if (lexer.token() == Token.HINT) {
                List<SQLCommentHint> hints = this.exprParser.parseHints();
                stmt.setHints(hints);
            }

            for (; ; ) {
                if (lexer.token() == Token.IDENTIFIER) {
                    long hash = lexer.hash_lower();

                    if (hash == FnvHash.Constants.LOW_PRIORITY) {
                        stmt.setLowPriority(true);
                        lexer.nextToken();
                        continue;
                    }

                    if (hash == FnvHash.Constants.DELAYED) {
                        stmt.setDelayed(true);
                        lexer.nextToken();
                        continue;
                    }

                    if (hash == FnvHash.Constants.HIGH_PRIORITY) {
                        stmt.setHighPriority(true);
                        lexer.nextToken();
                        continue;
                    }

                    if (hash == FnvHash.Constants.IGNORE) {
                        stmt.setIgnore(true);
                        lexer.nextToken();
                        continue;
                    }

                    if (hash == FnvHash.Constants.ROLLBACK_ON_FAIL) {
                        stmt.setRollbackOnFail(true);
                        lexer.nextToken();
                        continue;
                    }
                }

                break;
            }

            if (lexer.token() == Token.INTO) {
                lexer.nextToken();
                if (lexer.token() == Token.TABLE) {
                    lexer.nextToken();
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.OVERWRITE)) {
                lexer.nextToken();
                stmt.setOverwrite(true);
                if (lexer.token() == Token.TABLE) {
                    lexer.nextToken();
                } else if (lexer.token() == Token.INTO) {
                    lexer.nextToken();
                }
            }

            if (lexer.token() == Token.LINE_COMMENT) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.FULLTEXT) {
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.DICTIONARY)) {
                    lexer.nextToken();
                    stmt.setFulltextDictionary(true);
                }
            }

            tableName = this.exprParser.name();
            stmt.setTableName(tableName);

            if (lexer.token() == Token.HINT) {
                String comment = "/*" + lexer.stringVal() + "*/";
                lexer.nextToken();
                stmt.getTableSource().addAfterComment(comment);
            }

            if (lexer.token() == Token.IDENTIFIER
                && !lexer.identifierEquals(FnvHash.Constants.VALUE)) {
                stmt.setAlias(lexer.stringVal());
                lexer.nextToken();
            }

            if (lexer.token() == Token.WITH) {
                SQLSelectStatement withStmt = (SQLSelectStatement) parseWith();
                stmt.setQuery(withStmt.getSelect());
            }

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                accept(Token.LPAREN);
                for (; ; ) {
                    SQLAssignItem ptExpr = new SQLAssignItem();
                    ptExpr.setTarget(this.exprParser.name());
                    if (lexer.token() == Token.EQ) {
                        lexer.nextToken();
                        SQLExpr ptValue = this.exprParser.expr();
                        ptExpr.setValue(ptValue);
                    }
                    stmt.addPartition(ptExpr);
                    if (lexer.token() != Token.COMMA) {
                        break;
                    } else {
                        lexer.nextToken();
                    }
                }
                accept(Token.RPAREN);

                if (lexer.token() == Token.IF) {
                    lexer.nextToken();
                    accept(Token.NOT);
                    accept(Token.EXISTS);

                    stmt.setIfNotExists(true);
                }
            }
        }

        int columnSize = 0;
        List<SQLColumnDefinition> columnDefinitionList = null;

        if (lexer.token() == Token.LPAREN) {
            boolean useInsertColumnsCache = lexer.isEnabled(SQLParserFeature.UseInsertColumnsCache);
            InsertColumnsCache insertColumnsCache = null;

            long tableNameHash = 0;
            InsertColumnsCache.Entry cachedColumns = null;
            if (useInsertColumnsCache) {
                insertColumnsCache = this.insertColumnsCache;
                if (insertColumnsCache == null) {
                    insertColumnsCache = InsertColumnsCache.global;
                }

                if (tableName != null) {
                    tableNameHash = tableName.nameHashCode64();
                    cachedColumns = insertColumnsCache.get(tableNameHash);
                }
            }

            SchemaObject tableObject = null;
            int pos = lexer.pos();
            if (cachedColumns != null
                && lexer.text.startsWith(cachedColumns.columnsString, pos)
                && lexer.text.getCharset() == cachedColumns.charset) {
                if (!lexer.isEnabled(SQLParserFeature.OptimizedForParameterized)) {
                    List<SQLExpr> columns = stmt.getColumns();
                    List<SQLExpr> cachedColumns2 = cachedColumns.columns;
                    for (int i = 0, size = cachedColumns2.size(); i < size; i++) {
                        columns.add(cachedColumns2.get(i).clone());
                    }
                }
                stmt.setColumnsString(cachedColumns.columnsFormattedString, cachedColumns.columnsFormattedStringHash);
                int p2 = pos + cachedColumns.length;
                lexer.reset(p2);
                lexer.nextToken();
            } else {
                Lexer.SavePoint mark = lexer.mark();
                lexer.nextToken();
                if (lexer.token() == Token.SELECT) {
                    lexer.reset(mark);
                    SQLSelect select = this.exprParser.createSelectParser().select();
                    select.setParent(stmt);
                    stmt.setQuery(select);
                } else {
                    if (repository != null && lexer.isEnabled(SQLParserFeature.InsertValueCheckType)) {
                        tableObject = repository.findTable(tableName.nameHashCode64());
                    }

                    if (tableObject != null) {
                        columnDefinitionList = new ArrayList<SQLColumnDefinition>();
                    }

                    List<SQLExpr> columns = stmt.getColumns();

                    if (lexer.token() != Token.RPAREN) {
                        for (; ; ) {
                            String identName;
                            long hash;

                            Token token = lexer.token();
                            if (token == Token.IDENTIFIER) {
                                identName = lexer.stringVal();
                                hash = lexer.hash_lower();
                            } else if (token == Token.LITERAL_CHARS) {
                                if (lexer.isEnabled(SQLParserFeature.IgnoreNameQuotes)) {
                                    identName = lexer.stringVal();
                                } else {
                                    identName = '\'' + lexer.stringVal() + '\'';
                                }
                                hash = 0;
                            } else if (token == Token.LITERAL_ALIAS) {
                                identName = lexer.stringVal();
                                if (lexer.isEnabled(SQLParserFeature.IgnoreNameQuotes)) {
                                    identName = SQLUtils.normalize(identName, dbType);
                                }
                                hash = 0;
                            } else {
                                identName = lexer.stringVal();
                                hash = 0;
                            }
                            lexer.nextTokenComma();
                            SQLExpr expr = new SQLIdentifierExpr(identName, hash);
                            while (lexer.token() == Token.DOT) {
                                lexer.nextToken();
                                String propertyName = lexer.stringVal();
                                lexer.nextToken();
                                expr = new SQLPropertyExpr(expr, propertyName);
                            }

                            expr.setParent(stmt);
                            columns.add(expr);
                            columnSize++;

                            if (tableObject != null) {
                                SQLColumnDefinition columnDefinition = tableObject.findColumn(hash);
                                columnDefinitionList.add(columnDefinition);
                            }

                            if (lexer.token() == Token.COMMA) {
                                lexer.nextTokenIdent();
                                continue;
                            }

                            break;
                        }
                        columnSize = stmt.getColumns().size();

                        if (insertColumnsCache != null && tableName != null) {
                            String columnsString = lexer.subString(pos, lexer.pos() - pos);

                            List<SQLExpr> clonedColumns = new ArrayList<SQLExpr>(columnSize);
                            for (int i = 0; i < columns.size(); i++) {
                                clonedColumns.add(columns.get(i).clone());
                            }

                            StringBuilder buf = new StringBuilder();
                            SQLASTOutputVisitor outputVisitor = SQLUtils.createOutputVisitor(buf, dbType);
                            outputVisitor.printInsertColumns(columns);

                            String formattedColumnsString = buf.toString();
                            long columnsFormattedStringHash = FnvHash.fnv1a_64_lower(formattedColumnsString);

                            insertColumnsCache
                                .put(tableName.hashCode64(), columnsString, formattedColumnsString, clonedColumns,
                                    lexer.text.getCharset(), lexer.pos() - pos);
                            stmt.setColumnsString(formattedColumnsString, columnsFormattedStringHash);
                        }
                    }
                    accept(Token.RPAREN);
                }
            }
        }

        List<SQLCommentHint> commentHints = null;
        if (lexer.token() == Token.HINT) {
            commentHints = this.exprParser.parseHints();
        } else if (lexer.token() == Token.LINE_COMMENT) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.VALUES || lexer.identifierEquals(FnvHash.Constants.VALUE)) {
            lexer.nextTokenLParen();
            if (lexer.isEnabled(SQLParserFeature.InsertReader)) {
                return stmt;
            }

            if (lexer.isEnabled(SQLParserFeature.InsertValueNative)) {
                parseValueClauseNative(stmt.getValuesList(), columnDefinitionList, columnSize, stmt);
            } else {
                parseValueClause(stmt.getValuesList(), columnDefinitionList, columnSize, stmt);
            }
        } else if (lexer.token() == Token.SET) {
            lexer.nextToken();

            SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
            stmt.addValueCause(values);

            for (; ; ) {
                SQLName name = this.exprParser.name();
                stmt.addColumn(name);
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                } else {
                    accept(Token.COLONEQ);
                }
                values.addValue(this.exprParser.expr());

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }

        } else if (lexer.token() == (Token.SELECT)) {
            SQLSelect select = this.exprParser.createSelectParser().select();
            if (commentHints != null && !commentHints.isEmpty()) {
                select.setHeadHint(commentHints.get(0));
            }
            select.setParent(stmt);
            stmt.setQuery(select);
        } else if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();
            SQLSelect select = this.exprParser.createSelectParser().select();
            select.setParent(stmt);
            stmt.setQuery(select);
            accept(Token.RPAREN);
        } else if (lexer.token() == WITH) {
            SQLSelect query = this.exprParser.createSelectParser().select();
            stmt.setQuery(query);
        }

        if (lexer.token() == ON) {
            lexer.nextToken();
            acceptIdentifier("DUPLICATE");
            accept(Token.KEY);
            accept(Token.UPDATE);

            List<SQLExpr> duplicateKeyUpdate = stmt.getDuplicateKeyUpdate();
            for (; ; ) {
                SQLName name = this.exprParser.name();
                accept(Token.EQ);
                SQLExpr value;
                try {
                    value = this.exprParser.expr();
                } catch (EOFParserException e) {
                    throw new ParserException("EOF, " + name + "=", e);
                }

                SQLBinaryOpExpr assignment = new SQLBinaryOpExpr(name, SQLBinaryOperator.Equality, value);
                assignment.setParent(stmt);
                duplicateKeyUpdate.add(assignment);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextTokenIdent();
                    continue;
                }
                break;
            }
        }

        return stmt;
    }

    public MySqlSelectParser createSQLSelectParser() {
        return new MySqlSelectParser(this.exprParser, selectListCache);
    }

    public SQLStatement parseSet() {
        accept(Token.SET);

        Lexer.SavePoint mark = lexer.mark();

        SQLStatement sqlStmt = parseSetDefaultRole();
        if (sqlStmt == null) {
            lexer.reset(mark);
        } else {
            return sqlStmt;
        }

        sqlStmt = parseSetRole();
        if (sqlStmt == null) {
            lexer.reset(mark);
        } else {
            return sqlStmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.PASSWORD)) {
            lexer.nextToken();
            SQLSetStatement stmt = new SQLSetStatement();
            stmt.setDbType(dbType);
            stmt.setOption(SQLSetStatement.Option.PASSWORD);

            SQLExpr user = null;
            if (lexer.token() == FOR) {
                lexer.nextToken();
                user = this.exprParser.name();
            }

            accept(Token.EQ);

            SQLExpr password = this.exprParser.expr();

            stmt.set(user, password);

            return stmt;
        }

        Boolean global = null;
        Boolean session = null;
        boolean local = false;
        if (lexer.identifierEquals(GLOBAL)) {
            global = Boolean.TRUE;
            lexer.nextToken();
        } else if (lexer.identifierEquals(FnvHash.Constants.SESSION)) {
            session = Boolean.TRUE;
            lexer.nextToken();
        } else if (lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
            lexer.nextToken();
            local = true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.TRANSACTION)) {
            MySqlSetTransactionStatement stmt = new MySqlSetTransactionStatement();
            stmt.setGlobal(global);
            stmt.setSession(session);

            if (local) {
                stmt.setLocal(true);
            }

            lexer.nextToken();
            if (lexer.identifierEquals("ISOLATION")) {
                lexer.nextToken();
                acceptIdentifier("LEVEL");

                if (lexer.identifierEquals(READ)) {
                    lexer.nextToken();

                    if (lexer.identifierEquals("UNCOMMITTED")) {
                        stmt.setIsolationLevel("READ UNCOMMITTED");
                        lexer.nextToken();
                    } else if (lexer.identifierEquals(WRITE)) {
                        stmt.setIsolationLevel("READ WRITE");
                        lexer.nextToken();
                    } else if (lexer.identifierEquals("ONLY")) {
                        stmt.setIsolationLevel("READ ONLY");
                        lexer.nextToken();
                    } else if (lexer.identifierEquals("COMMITTED")) {
                        stmt.setIsolationLevel("READ COMMITTED");
                        lexer.nextToken();
                    } else {
                        throw new ParserException(
                            "UNKOWN TRANSACTION LEVEL : " + lexer.stringVal() + ", " + lexer.info());
                    }
                } else if (lexer.identifierEquals("SERIALIZABLE")) {
                    stmt.setIsolationLevel("SERIALIZABLE");
                    lexer.nextToken();
                } else if (lexer.identifierEquals("REPEATABLE")) {
                    lexer.nextToken();
                    if (lexer.identifierEquals(READ)) {
                        stmt.setIsolationLevel("REPEATABLE READ");
                        lexer.nextToken();
                    } else {
                        throw new ParserException(
                            "UNKOWN TRANSACTION LEVEL : " + lexer.stringVal() + ", " + lexer.info());
                    }
                } else {
                    throw new ParserException("UNKOWN TRANSACTION LEVEL : " + lexer.stringVal() + ", " + lexer.info());
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.POLICY)) {
                lexer.nextToken();
                SQLExpr policy = this.exprParser.primary();
                stmt.setPolicy(policy);
            } else if (lexer.identifierEquals(READ)) {
                lexer.nextToken();
                if (lexer.identifierEquals("ONLY")) {
                    stmt.setAccessModel("ONLY");
                    lexer.nextToken();
                } else if (lexer.identifierEquals("WRITE")) {
                    stmt.setAccessModel("WRITE");
                    lexer.nextToken();
                } else {
                    throw new ParserException("UNKOWN ACCESS MODEL : " + lexer.stringVal() + ", " + lexer.info());
                }
            }

            return stmt;
        } else {
            SQLSetStatement stmt = new SQLSetStatement(getDbType());

            parseAssignItems(stmt.getItems(), stmt, true);

            if (global != null) {
                SQLVariantRefExpr varRef = (SQLVariantRefExpr) stmt.getItems().get(0).getTarget();
                varRef.setGlobal(true);
            }

            if (session != null) {
                SQLVariantRefExpr varRef = (SQLVariantRefExpr) stmt.getItems().get(0).getTarget();
                varRef.setSession(true);
            }

            if (lexer.token() == Token.HINT) {
                stmt.setHints(this.exprParser.parseHints());
            }

            return stmt;
        }
    }

    private SQLStatement parseSetDefaultRole() {
        MySqlSetDefaultRoleStatement stmt = null;
        if (lexer.token() == DEFAULT) {
            lexer.nextToken();
            if (lexer.token() == ROLE) {
                lexer.nextToken();
                stmt = new MySqlSetDefaultRoleStatement();
                // Set default role statement
                MySqlExprParser mysqlParser = (MySqlExprParser) exprParser;

                MySqlUserName firstRole = MySqlUserName.fromExpr(mysqlParser.userName());
                if ("NONE".equalsIgnoreCase(firstRole.getUserName())) {
                    stmt.setDefaultRoleSpec(MySqlSetDefaultRoleStatement.DefaultRoleSpec.NONE);
                } else if ("ALL".equalsIgnoreCase(firstRole.getUserName())) {
                    stmt.setDefaultRoleSpec(MySqlSetDefaultRoleStatement.DefaultRoleSpec.ALL);
                } else {
                    stmt.setDefaultRoleSpec(MySqlSetDefaultRoleStatement.DefaultRoleSpec.ROLES);
                    stmt.addRole(firstRole);

                    while (lexer.token() == COMMA) {
                        lexer.nextToken();
                        stmt.addRole(MySqlUserName.fromExpr(mysqlParser.userName()));
                    }
                }

                accept(TO);

                stmt.addUser(MySqlUserName.fromExpr(mysqlParser.userName()));
                while (lexer.token() == COMMA) {
                    lexer.nextToken();
                    stmt.addUser(MySqlUserName.fromExpr(mysqlParser.userName()));
                }
            }
        }

        return stmt;
    }

    private SQLStatement parseSetRole() {
        MySqlSetRoleStatement stmt = null;
        MySqlExprParser mysqlExprParser = (MySqlExprParser) exprParser;
        if (lexer.token() == ROLE) {
            stmt = new MySqlSetRoleStatement();
            lexer.nextToken();
            if (lexer.token() == DEFAULT) {
                stmt.setRoleSpec(MySqlSetRoleStatement.RoleSpec.DEFAULT);
                lexer.nextToken();
            } else if (lexer.identifierEquals("NONE")) {
                stmt.setRoleSpec(MySqlSetRoleStatement.RoleSpec.NONE);
                lexer.nextToken();
            } else if (lexer.token() == ALL) {
                lexer.nextToken();

                if (lexer.token() == EXCEPT) {
                    stmt.setRoleSpec(MySqlSetRoleStatement.RoleSpec.ALL_EXCEPT);
                    lexer.nextToken();
                    MySqlUserName username = MySqlUserName.fromExpr(mysqlExprParser.userName());
                    stmt.addRole(username);
                    while (lexer.token() == COMMA) {
                        lexer.nextToken();
                        username = MySqlUserName.fromExpr(mysqlExprParser.userName());
                        stmt.addRole(username);
                    }
                } else {
                    stmt.setRoleSpec(MySqlSetRoleStatement.RoleSpec.ALL);
                    lexer.nextToken();
                }
            } else {
                stmt.setRoleSpec(MySqlSetRoleStatement.RoleSpec.ROLES);
                MySqlUserName username = MySqlUserName.fromExpr(mysqlExprParser.userName());
                stmt.addRole(username);
                while (lexer.token() == COMMA) {
                    lexer.nextToken();
                    username = MySqlUserName.fromExpr(mysqlExprParser.userName());
                    stmt.addRole(username);
                }
            }
        }

        return stmt;
    }

    public SQLStatement parseAlter() {
        Lexer.SavePoint mark = lexer.mark();
        accept(Token.ALTER);

        if (lexer.token() == Token.USER) {
            return parseAlterUser();
        }

        boolean online = false, offline = false;

        if (lexer.identifierEquals("ONLINE")) {
            online = true;
            lexer.nextToken();
        }

        if (lexer.identifierEquals("OFFLINE")) {
            offline = true;
            lexer.nextToken();
        }

        boolean ignore = false;

        if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
            ignore = true;
            lexer.nextToken();
        }

        if (lexer.token() == Token.TABLE) {
            return parseAlterTable(ignore, online, offline);
        }

        if (lexer.token() == Token.DATABASE
            || lexer.token() == Token.SCHEMA) {
            return parseAlterDatabase();
        }

        if (lexer.identifierEquals(FnvHash.Constants.EVENT)) {
            return parseAlterEvent();
        }

        if (lexer.token() == Token.FUNCTION) {
            return parseAlterFunction();
        }

        if (lexer.token() == Token.PROCEDURE) {
            return parseAlterProcedure();
        }

        if (lexer.token() == Token.TABLESPACE) {
            return parseAlterTableSpace();
        }

        if (lexer.token() == Token.VIEW) {
            return parseAlterView();
        }

        if (lexer.token() == Token.SEQUENCE) {
            lexer.reset(mark);
            return parseAlterSequence();
        }

        if (lexer.identifierEquals(FnvHash.Constants.LOGFILE)) {
            return parseAlterLogFileGroup();
        }

        if (lexer.identifierEquals(FnvHash.Constants.SERVER)) {
            return parseAlterServer();
        }

        if (lexer.identifierEquals(FnvHash.Constants.ALGORITHM)) {
            return parseAlterView();
        }

        if (lexer.identifierEquals(FnvHash.Constants.OUTLINE)) {
            lexer.reset(mark);
            return parseAlterOutline();
        }

        if (lexer.token() == Token.FULLTEXT) {
            lexer.reset(mark);
            return parseAlterFullTextCharFilter();
        }

        if (lexer.token() == Token.INDEX) {
            lexer.reset(mark);
            accept(Token.ALTER);
            accept(Token.INDEX);

            SQLAlterIndexStatement stmt = new SQLAlterIndexStatement();
            stmt.setName(this.exprParser.name());
            accept(Token.SET);

            accept(Token.FULLTEXT);
        }

        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
            Lexer.SavePoint savePoint = lexer.mark();
            lexer.nextToken();
            accept(Token.EQ);
            this.getExprParser().userName();
            if (lexer.identifierEquals(FnvHash.Constants.EVENT)) {
                lexer.reset(savePoint);
                return parseAlterEvent();
            } else {
                lexer.reset(savePoint);
                return parseAlterView();
            }
        }
        if (lexer.identifierEquals(FnvHash.Constants.TABLEGROUP)) {
            lexer.reset(mark);
            return parseAlterTableGroup();
        }

        if (lexer.identifierEquals(FnvHash.Constants.JOINGROUP)) {
            lexer.reset(mark);
            return parseAlterJoinGroup();
        }

        if (lexer.identifierEquals("FILESTORAGE")) {
            lexer.reset(mark);
            return parseAlterFileStorage();
        }

        if (lexer.identifierEquals("SYSTEM")) {
            lexer.reset(mark);
            return parseAlterSystem();
        }

        if (lexer.identifierEquals(FnvHash.Constants.RESOURCE)) {
            lexer.reset(mark);
            return parseAlterResourceGroup();
        }
        if (lexer.identifierEquals(FnvHash.Constants.MATERIALIZED)) {
            lexer.reset(mark);
            return parseAlterMaterialized();
        }

        if (lexer.identifierEquals("INSTANCE")) {
            return parseAlterInstance();
        }

        throw new ParserException("TODO " + lexer.info());
    }

    private SQLStatement parseAddManageInstanceGroup() {
        lexer.nextToken();
        MySqlManageInstanceGroupStatement stmt = new MySqlManageInstanceGroupStatement();
        stmt.setOperation(new SQLIdentifierExpr("ADD"));

        acceptIdentifier("INSTANCE_GROUP");
        for (; ; ) {
            stmt.getGroupNames().add(exprParser.expr());
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }

        acceptIdentifier("REPLICATION");
        accept(Token.EQ);
        stmt.setReplication(exprParser.integerExpr());
        return stmt;
    }

    private SQLStatement parseAlterFullTextCharFilter() {
        accept(Token.ALTER);
        accept(Token.FULLTEXT);

        MysqlAlterFullTextStatement stmt = new MysqlAlterFullTextStatement();

        stmt.setType(parseFullTextType());

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        accept(Token.SET);

        SQLAssignItem item = this.exprParser.parseAssignItem();
        stmt.setItem(item);
        return stmt;
    }

    protected SQLStatement parseAlterTableGroup() {
        accept(Token.ALTER);
        acceptIdentifier("TABLEGROUP");

        SQLName name = this.exprParser.name();

        SQLAlterTableGroupStatement stmt = new SQLAlterTableGroupStatement(getDbType());
        stmt.setName(name);

        if (lexer.identifierEquals("SPLIT")) {
            acceptIdentifier("SPLIT");
            if (lexer.token() == Token.INTO) {
                DrdsSplitHotKey item = parseSplitHotKey();
                stmt.setItem(item);
            } else {
                DrdsSplitPartition item = parseSplitPartition();
                stmt.setItem(item);
            }
        } else if (lexer.token() == Token.MERGE) {
            DrdsMergePartition item = parseMergePartition();
            stmt.setItem(item);
        } else if (lexer.identifierEquals("MOVE")) {
            DrdsMovePartition item = parseMovePartition();
            stmt.setItem(item);
        } else if (lexer.identifierEquals("EXTRACT")) {
            DrdsExtractHotKey item = parseExtractHotKey();
            stmt.setItem(item);
        } else if (lexer.identifierEquals("REORGANIZE")) {
            DrdsAlterTableGroupReorgPartition item = new DrdsAlterTableGroupReorgPartition();
            lexer.nextToken();
            accept(Token.PARTITION);
            while (true) {
                SQLName partitionName = this.exprParser.name();
                item.addOldPartition(partitionName);
                if (lexer.token() != Token.COMMA) {
                    break;
                }
                accept(Token.COMMA);
            }
            accept(Token.INTO);
            accept(Token.LPAREN);
            for (; ; ) {
                SQLPartition partition = this.getExprParser().parsePartition();

                item.addNewPartition(partition);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else {
                    break;
                }
            }
            accept(Token.RPAREN);
            stmt.setItem(item);
        } else if (lexer.identifierEquals("ADD")) {
            lexer.nextToken();
            if (lexer.identifierEquals("TABLES")) {
                lexer.nextToken();
                SQLAlterTableGroupAddTable item = new SQLAlterTableGroupAddTable();
                for (; ; ) {
                    SQLName tableName = this.exprParser.name();
                    item.addTable(tableName);
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    } else {
                        break;
                    }
                }
                if (lexer.identifierEquals("FORCE")) {
                    item.setForce(true);
                    lexer.nextToken();
                }
                stmt.setItem(item);
            } else {

                SQLAlterTableAddPartition item = parseAddPartition();
                stmt.setItem(item);
            }
        } else if (lexer.token() == Token.DROP) {
            lexer.nextToken();
            if (lexer.token() == Token.PARTITION) {
                SQLAlterTableDropPartition dropPartition = parseAlterTableDropPartition(false);
                stmt.setItem(dropPartition);
            }
        } else if (lexer.identifierEquals(FnvHash.Constants.MODIFY)) {
            lexer.nextToken();
            // MODIFY PARTITION [(]part_name ADD|DROP VALUES(),part_name ADD|DROP VALUES()[)]
            accept(Token.PARTITION);
            boolean paren = false;
            if (lexer.token() == Token.LPAREN) {
                paren = true;
                lexer.nextToken();
            }
            for (; ; ) {
                SQLAlterTableModifyPartitionValues item = this.exprParser.parseModifyListPartition();
                stmt.setItem(item);

                if (paren && lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            if (paren) {
                accept(Token.RPAREN);
            }
        } else if (lexer.token() == Token.TRUNCATE) {
            lexer.nextToken();
            accept(Token.PARTITION);

            SQLAlterTableTruncatePartition item = new SQLAlterTableTruncatePartition();
            if (lexer.token() == Token.ALL) {
                item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                lexer.nextToken();
            } else {
                this.exprParser.names(item.getPartitions(), item);
            }
            stmt.setItem(item);
        } else if (lexer.identifierEquals("RENAME")) {
            DrdsRenamePartition item = parseRenamePartition();
            stmt.setItem(item);
        } else if (lexer.token() == Token.SET) {
            accept(Token.SET);
            if (lexer.identifierEquals("PARTITIONS")) {
                acceptIdentifier("PARTITIONS");
                SQLName partitionName = this.exprParser.name();
                acceptIdentifier("LOCALITY");
                accept(Token.EQ);
                DrdsAlterTableGroupSetPartitionsLocality item = new DrdsAlterTableGroupSetPartitionsLocality();
                item.setPartitionTargetLocality(partitionName, this.exprParser.expr());
                stmt.setItem(item);
            } else {
                acceptIdentifier("LOCALITY");
                accept(Token.EQ);
                DrdsAlterTableGroupSetLocality item = new DrdsAlterTableGroupSetLocality();
                item.setTargetLocality(this.exprParser.expr());
                stmt.setItem(item);
            }
        } else {
            for (; ; ) {
                SQLName key = this.exprParser.name();
                accept(Token.EQ);
                SQLExpr value = this.exprParser.expr();
                stmt.getOptions().add(new SQLAssignItem(key, value));

                if (lexer.token() == Token.EOF) {
                    break;
                }
            }
        }
        return stmt;
    }

    protected SQLAlterTableAddPartition parseAddPartition() {
        accept(Token.PARTITION);

        SQLAlterTableAddPartition item = new SQLAlterTableAddPartition();

        /**
         * Hash/Key partitions is not support to add partitions
         */
//            if (lexer.identifierEquals("PARTITIONS")) {
//                lexer.nextToken();
//                item.setPartitionCount(this.exprParser.integerExpr());
//            }
        accept(Token.LPAREN);
        for (; ; ) {
            SQLPartition partition = this.getExprParser().parsePartition();
            item.addPartition(partition);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            } else {
                break;
            }
        }
        accept(Token.RPAREN);
        return item;
    }

    protected DrdsMergePartition parseMergePartition() {
        DrdsMergePartition item = new DrdsMergePartition();
        accept(Token.MERGE);
        acceptIdentifier("PARTITIONS");
        while (true) {
            SQLName partitionName = this.exprParser.name();
            item.addPartition(partitionName);
            if (lexer.token() != Token.COMMA) {
                break;
            }
            accept(Token.COMMA);
        }
        accept(Token.TO);
        SQLName partitionName = this.exprParser.name();
        item.setTargetPartitionName(partitionName);
        return item;
    }

    protected DrdsMovePartition parseMovePartition() {
        DrdsMovePartition item = new DrdsMovePartition();
        lexer.nextToken();
        acceptIdentifier("PARTITIONS");
        Map<SQLName, List<SQLName>> instPartitions = new HashMap<>();
        if (lexer.token() == Token.LPAREN) {
            accept(Token.LPAREN);
            while (true) {
                List<SQLName> partNames = new ArrayList<>();
                while (true) {
                    SQLName partitionName = this.exprParser.name();
                    partNames.add(partitionName);
                    if (lexer.token() != Token.COMMA) {
                        break;
                    }
                    accept(Token.COMMA);
                }
                accept(Token.RPAREN);
                accept(Token.TO);
                SQLName storageId = this.exprParser.name();
                instPartitions.put(storageId, partNames);
                if (lexer.token() != Token.COMMA) {
                    break;
                }
                accept(Token.COMMA);
                accept(Token.LPAREN);
            }
        } else {
            List<SQLName> partNames = new ArrayList<>();
            while (true) {
                SQLName partitionName = this.exprParser.name();
                partNames.add(partitionName);
                if (lexer.token() != Token.COMMA) {
                    break;
                }
                accept(Token.COMMA);
            }
            accept(Token.TO);
            SQLName storageId = this.exprParser.name();
            instPartitions.put(storageId, partNames);
        }
        item.setInstPartitions(instPartitions);
        return item;
    }

    protected DrdsExtractHotKey parseExtractHotKey() {
        DrdsExtractHotKey item = new DrdsExtractHotKey();
        lexer.nextToken();
        accept(Token.TO);
        accept(Token.PARTITION);
        if (lexer.token() != Token.BY) {
            SQLName partitionName = this.exprParser.name();
            item.setHotKeyPartitionName(partitionName);
        }
        accept(Token.BY);
        if (identifierEquals("HOT")) {
            acceptIdentifier("HOT");
        }
        acceptIdentifier("VALUE");
        accept(Token.LPAREN);
        List<SQLExpr> hotKeys = new ArrayList<SQLExpr>();
        while (true) {
            SQLExpr val = this.exprParser.expr();
            hotKeys.add(val);
            if (lexer.token() != Token.COMMA) {
                break;
            }
            accept(Token.COMMA);
        }
        item.setHotKeys(hotKeys);
        accept(Token.RPAREN);
        return item;
    }

    protected DrdsRenamePartition parseRenamePartition() {
        DrdsRenamePartition item = new DrdsRenamePartition();
        boolean firstPart = true;
        acceptIdentifier("RENAME");
        accept(Token.PARTITION);
        do {
            if (!firstPart) {
                accept(Token.COMMA);
            }
            SQLName oldPartitionName = this.exprParser.name();
            accept(Token.TO);
            SQLName newPartitionName = this.exprParser.name();
            item.addRenamePartitionPair(new Pair<>(oldPartitionName, newPartitionName));
            firstPart = false;
        } while (lexer.token() == Token.COMMA);
        return item;
    }

    protected DrdsSplitPartition parseSplitPartition() {
        DrdsSplitPartition item = new DrdsSplitPartition();
        accept(Token.PARTITION);
        SQLName partitionName = this.exprParser.name();
        item.setSplitPartitionName(partitionName);
        if (lexer.identifierEquals("AT")) {
            acceptIdentifier("AT");
            accept(Token.LPAREN);
            SQLExpr value = this.exprParser.expr();
            item.setAtValue(value);
            accept(Token.RPAREN);
            accept(Token.INTO);
            accept(Token.LPAREN);
            SQLPartition partition = this.getExprParser().parsePartition();
            item.addPartition(partition);
            accept(Token.COMMA);
            partition = this.getExprParser().parsePartition();
            item.addPartition(partition);
            accept(Token.RPAREN);
        } else if (lexer.token() == Token.INTO) {
            accept(Token.INTO);
            accept(Token.LPAREN);
            for (; ; ) {
                SQLPartition partition = this.getExprParser().parsePartition();

                item.addPartition(partition);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else {
                    break;
                }
            }
            accept(Token.RPAREN);
        }
        return item;
    }

    protected DrdsSplitHotKey parseSplitHotKey() {
        DrdsSplitHotKey item = new DrdsSplitHotKey();
        accept(Token.INTO);
        if (!lexer.identifierEquals("PARTITIONS")) {
            SQLName partitionName = this.exprParser.name();
            item.setHotKeyPartitionName(partitionName);
        }
        acceptIdentifier("PARTITIONS");
        item.setPartitions(this.exprParser.integerExpr());
        accept(Token.BY);
        acceptIdentifier("HOT");
        acceptIdentifier("VALUE");
        accept(Token.LPAREN);
        List<SQLExpr> hotKeys = new ArrayList<SQLExpr>();
        while (true) {
            SQLExpr val = this.exprParser.expr();
            hotKeys.add(val);
            if (lexer.token() != Token.COMMA) {
                break;
            }
            accept(Token.COMMA);
        }
        item.setHotKeys(hotKeys);
        accept(Token.RPAREN);
        return item;
    }

    protected SQLStatement parseAlterJoinGroup() {
        accept(Token.ALTER);
        acceptIdentifier("JOINGROUP");

        SQLName joinGroupName = this.exprParser.name();

        SQLAlterJoinGroupStatement stmt = new SQLAlterJoinGroupStatement(getDbType());
        stmt.setJoinGroupName(joinGroupName);
        if (lexer.identifierEquals("ADD")) {
            stmt.setAdd(true);
            acceptIdentifier("ADD");
        } else if (lexer.identifierEquals("REMOVE")) {
            stmt.setRemove(true);
            acceptIdentifier("REMOVE");
        } else {
            throw new ParserException("TODO " + lexer.info());
        }
        acceptIdentifier("TABLES");
        boolean firstPart = true;
        List<SQLName> tableNames = new ArrayList<SQLName>();
        do {
            if (!firstPart) {
                accept(Token.COMMA);
            }
            SQLName tableName = this.exprParser.name();
            tableNames.add(tableName);
            firstPart = false;
        } while (lexer.token() == Token.COMMA);
        stmt.setTableNames(tableNames);
        return stmt;
    }

    protected SQLStatement parseAlterSystem() {
        accept(Token.ALTER);
        acceptIdentifier("SYSTEM");

        if (lexer.token() == Token.SET) {
            accept(Token.SET);
            acceptIdentifier("CONFIG");

            SQLAlterSystemSetConfigStatement stmt = new SQLAlterSystemSetConfigStatement();

            for (; ; ) {
                SQLName key = this.exprParser.name();
                accept(Token.EQ);
                SQLExpr value = this.exprParser.expr();
                stmt.getOptions().add(new SQLAssignItem(key, value));

                if (lexer.token() == Token.EOF) {
                    break;
                }
            }

            return stmt;

        } else if (lexer.identifierEquals("GET")) {
            acceptIdentifier("GET");
            acceptIdentifier("CONFIG");
            SQLName name = this.exprParser.name();

            SQLAlterSystemGetConfigStatement stmt = new SQLAlterSystemGetConfigStatement();
            stmt.setName(name);

            return stmt;

        } else if (lexer.identifierEquals("CHANGE_ROLE")) {
            // alter system change_role zone/node 'az1' to learner/follower;
            lexer.nextToken();
            SQLExpr targetType = this.exprParser.expr();
            SQLExpr target = this.exprParser.expr();
            accept(Token.TO);
            SQLExpr role = this.exprParser.expr();

            SQLChangeRoleStatement stmt = new SQLChangeRoleStatement();
            stmt.setRole(role);
            stmt.setTargetType(targetType);
            stmt.setTarget(target);
            return stmt;
        }

        throw new ParserException("TODO " + lexer.info());
    }

    protected SQLStatement parseAlterOutline() {
        accept(Token.ALTER);

        if (lexer.identifierEquals(FnvHash.Constants.OUTLINE)) {
            lexer.nextToken();
        } else {
            throw new ParserException("TODO " + lexer.info());
        }

        SQLAlterOutlineStatement stmt = new SQLAlterOutlineStatement();
        stmt.setDbType(dbType);

        stmt.setName(this.exprParser.name());

        if (lexer.identifierEquals("RESYNC")) {
            lexer.nextToken();
            stmt.setResync(true);
        }

        return stmt;
    }

    protected SQLStatement parseAlterView() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }

        SQLAlterViewStatement createView = new SQLAlterViewStatement(getDbType());

        if (lexer.identifierEquals("ALGORITHM")) {
            lexer.nextToken();
            accept(Token.EQ);
            String algorithm = lexer.stringVal();
            createView.setAlgorithm(algorithm);
            lexer.nextToken();
        }

        if (lexer.identifierEquals("DEFINER")) {
            lexer.nextToken();
            accept(Token.EQ);
            SQLName definer = (SQLName) this.exprParser.expr();
            createView.setDefiner(definer);
        }

        if (lexer.identifierEquals("SQL")) {
            lexer.nextToken();
            acceptIdentifier("SECURITY");
            String sqlSecurity = lexer.stringVal();
            createView.setSqlSecurity(sqlSecurity);
            lexer.nextToken();
        }

        if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
            lexer.nextToken();
            createView.setForce(true);
        }

        this.accept(Token.VIEW);

        if (lexer.token() == Token.IF || lexer.identifierEquals("IF")) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            createView.setIfNotExists(true);
        }

        createView.setName(exprParser.name());

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (; ; ) {

                if (lexer.token() == Token.CONSTRAINT) {
                    SQLTableConstraint constraint = (SQLTableConstraint) this.exprParser.parseConstaint();
                    createView.addColumn(constraint);
                } else {
                    SQLColumnDefinition column = new SQLColumnDefinition();
                    column.setDbType(dbType);
                    SQLName expr = this.exprParser.name();
                    column.setName(expr);

                    this.exprParser.parseColumnRest(column);

                    if (lexer.token() == Token.COMMENT) {
                        lexer.nextToken();

                        SQLExpr comment;
                        if (lexer.token() == Token.LITERAL_ALIAS) {
                            String alias = lexer.stringVal();
                            if (alias.length() > 2 && alias.charAt(0) == '"'
                                && alias.charAt(alias.length() - 1) == '"') {
                                alias = alias.substring(1, alias.length() - 1);
                            }
                            comment = new SQLCharExpr(alias);
                            lexer.nextToken();
                        } else {
                            comment = this.exprParser.primary();
                        }
                        column.setComment(comment);
                    }

                    column.setParent(createView);
                    createView.addColumn(column);
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                } else {
                    break;
                }
            }

            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            SQLCharExpr comment = (SQLCharExpr) exprParser.primary();
            createView.setComment(comment);
        }

        this.accept(Token.AS);

        SQLSelectParser selectParser = this.createSQLSelectParser();
        createView.setSubQuery(selectParser.select());

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();

            if (lexer.identifierEquals("CASCADED")) {
                createView.setWithCascaded(true);
                lexer.nextToken();
            } else if (lexer.identifierEquals("LOCAL")) {
                createView.setWithLocal(true);
                lexer.nextToken();
            } else if (lexer.identifierEquals("READ")) {
                lexer.nextToken();
                accept(Token.ONLY);
                createView.setWithReadOnly(true);
            }

            if (lexer.token() == Token.CHECK) {
                lexer.nextToken();
                acceptIdentifier("OPTION");
                createView.setWithCheckOption(true);
            }
        }

        return createView;
    }

    protected SQLStatement parseAlterTableSpace() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }

        accept(Token.TABLESPACE);

        SQLName name = this.exprParser.name();

        MySqlAlterTablespaceStatement stmt = new MySqlAlterTablespaceStatement();
        stmt.setName(name);

        if (lexer.identifierEquals(FnvHash.Constants.ADD)) {
            lexer.nextToken();
            acceptIdentifier("DATAFILE");
            SQLExpr file = this.exprParser.primary();
            stmt.setAddDataFile(file);
        } else if (lexer.token() == Token.DROP) {
            lexer.nextToken();
            acceptIdentifier("DATAFILE");
            SQLExpr file = this.exprParser.primary();
            stmt.setDropDataFile(file);
        }

        if (lexer.identifierEquals(FnvHash.Constants.INITIAL_SIZE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr initialSize = this.exprParser.expr();
            stmt.setInitialSize(initialSize);
        }

        if (lexer.identifierEquals(FnvHash.Constants.WAIT)) {
            lexer.nextToken();
            stmt.setWait(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.ENGINE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr engine = this.exprParser.expr();
            stmt.setEngine(engine);
        }

        return stmt;
    }

    protected SQLStatement parseAlterServer() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }
        acceptIdentifier("SERVER");

        SQLName name = this.exprParser.name();

        MySqlAlterServerStatement stmt = new MySqlAlterServerStatement();
        stmt.setName(name);

        acceptIdentifier("OPTIONS");
        accept(Token.LPAREN);
        if (lexer.token() == Token.USER) {
            lexer.nextToken();
            SQLExpr user = this.exprParser.name();
            stmt.setUser(user);
        }
        accept(Token.RPAREN);

        return stmt;
    }

    protected SQLStatement parseAlterFileStorage() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }
        acceptIdentifier("FileStorage");
        SQLName name = this.exprParser.name();
        DrdsAlterFileStorageStatement stmt = new DrdsAlterFileStorageStatement();
        stmt.setName(name);
        if (lexer.token() == Token.AS) {
            accept(Token.AS);
            accept(Token.OF);
            stmt.setAsOf(true);
            acceptIdentifier("TIMESTAMP");
            SQLExpr timestamp = this.exprParser.expr();
            stmt.setTimestamp(timestamp);
        } else if (lexer.identifierEquals("BACKUP")) {
            acceptIdentifier("BACKUP");
            stmt.setBackup(true);
        } else {
            acceptIdentifier("PURGE");
            acceptIdentifier("BEFORE");
            stmt.setPurgeBefore(true);

            acceptIdentifier("TIMESTAMP");
            SQLExpr timestamp = this.exprParser.expr();
            stmt.setTimestamp(timestamp);
        }
        return stmt;
    }

    protected SQLStatement parseCreateLogFileGroup() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }
        acceptIdentifier("LOGFILE");
        accept(Token.GROUP);

        SQLName name = this.exprParser.name();

        MySqlCreateAddLogFileGroupStatement stmt = new MySqlCreateAddLogFileGroupStatement();
        stmt.setName(name);

        acceptIdentifier("ADD");
        acceptIdentifier("UNDOFILE");

        SQLExpr fileName = this.exprParser.primary();
        stmt.setAddUndoFile(fileName);

        if (lexer.identifierEquals(FnvHash.Constants.INITIAL_SIZE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr initialSize = this.exprParser.expr();
            stmt.setInitialSize(initialSize);
        }

        if (lexer.identifierEquals(FnvHash.Constants.WAIT)) {
            lexer.nextToken();
            stmt.setWait(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.ENGINE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr engine = this.exprParser.expr();
            stmt.setEngine(engine);
        }

        return stmt;
    }

    protected SQLStatement parseAlterLogFileGroup() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }
        acceptIdentifier("LOGFILE");
        accept(Token.GROUP);

        SQLName name = this.exprParser.name();

        MySqlAlterLogFileGroupStatement stmt = new MySqlAlterLogFileGroupStatement();
        stmt.setName(name);

        acceptIdentifier("ADD");
        acceptIdentifier("UNDOFILE");

        SQLExpr fileName = this.exprParser.primary();
        stmt.setAddUndoFile(fileName);

        if (lexer.identifierEquals(FnvHash.Constants.INITIAL_SIZE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr initialSize = this.exprParser.expr();
            stmt.setInitialSize(initialSize);
        }

        if (lexer.identifierEquals(FnvHash.Constants.WAIT)) {
            lexer.nextToken();
            stmt.setWait(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.ENGINE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr engine = this.exprParser.expr();
            stmt.setEngine(engine);
        }

        return stmt;
    }

    protected SQLStatement parseAlterProcedure() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }
        accept(Token.PROCEDURE);

        SQLAlterProcedureStatement stmt = new SQLAlterProcedureStatement();
        stmt.setDbType(dbType);

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        // for mysql
        for (; ; ) {
            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                SQLCharExpr comment = this.exprParser.charExpr();
                stmt.setComment(comment);
            } else if (lexer.identifierEquals(FnvHash.Constants.LANGUAGE)) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setLanguageSql(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
                lexer.nextToken();
                acceptIdentifier("SECURITY");
                if (lexer.identifierEquals("DEFINER")) {
                    stmt.setSqlSecurity(SqlSecurity.DEFINER);
                    lexer.nextToken();
                } else if (lexer.identifierEquals("INVOKER")) {
                    stmt.setSqlSecurity(SqlSecurity.INVOKER);
                    lexer.nextToken();
                } else {
                    throw new RuntimeException("sql security can only be set to definer or invoker");
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.CONTAINS) || lexer.token() == Token.CONTAINS) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setSqlDataAccess(SqlDataAccess.CONTAINS_SQL);
            } else if (lexer.identifierEquals(FnvHash.Constants.NO)) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setSqlDataAccess(SqlDataAccess.NO_SQL);
            } else if (lexer.identifierEquals("READS")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                stmt.setSqlDataAccess(SqlDataAccess.READS_SQL_DATA);
            } else if (lexer.identifierEquals("MODIFIES")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                stmt.setSqlDataAccess(SqlDataAccess.MODIFIES_SQL_DATA);
            } else {
                break;
            }
        }

        return stmt;
    }

    protected SQLStatement parseAlterFunction() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }
        accept(Token.FUNCTION);

        SQLAlterFunctionStatement stmt = new SQLAlterFunctionStatement();
        stmt.setDbType(dbType);

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        // for mysql
        for (; ; ) {
            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                SQLCharExpr comment = this.exprParser.charExpr();
                stmt.setComment(comment);
            } else if (lexer.identifierEquals(FnvHash.Constants.LANGUAGE)) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setLanguageSql(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
                lexer.nextToken();
                acceptIdentifier("SECURITY");
                if (lexer.identifierEquals("DEFINER")) {
                    stmt.setSqlSecurity(SqlSecurity.DEFINER);
                    lexer.nextToken();
                } else if (lexer.identifierEquals("INVOKER")) {
                    stmt.setSqlSecurity(SqlSecurity.INVOKER);
                    lexer.nextToken();
                } else {
                    throw new RuntimeException("sql security can only be set to definer or invoker");
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.CONTAINS) || lexer.token() == Token.CONTAINS) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                throw new RuntimeException("SQL_DATA_ACCESS part can not be alter");
            } else if (lexer.identifierEquals(FnvHash.Constants.NO)) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                throw new RuntimeException("SQL_DATA_ACCESS part can not be alter");
            } else if (lexer.identifierEquals("READS")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                throw new RuntimeException("SQL_DATA_ACCESS part can not be alter");
            } else if (lexer.identifierEquals("MODIFIES")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                throw new RuntimeException("SQL_DATA_ACCESS part can not be alter");
            } else {
                break;
            }
        }

        return stmt;
    }

    protected SQLStatement parseCreateEvent() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        MySqlCreateEventStatement stmt = new MySqlCreateEventStatement();

        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
            lexer.nextToken();
            accept(Token.EQ);
            SQLName definer = this.getExprParser().userName();
            stmt.setDefiner(definer);

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                accept(Token.RPAREN);
            }
        }

        acceptIdentifier("EVENT");

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            stmt.setIfNotExists(true);
        }

        SQLName eventName = this.exprParser.name();
        stmt.setName(eventName);

        while (lexer.token() == ON) {
            lexer.nextToken();
            if (lexer.identifierEquals(FnvHash.Constants.SCHEDULE)) {
                lexer.nextToken();
                MySqlEventSchedule schedule = parseSchedule();
                stmt.setSchedule(schedule);
            } else if (lexer.identifierEquals(FnvHash.Constants.COMPLETION)) {
                lexer.nextToken();

                boolean value;
                if (lexer.token() == Token.NOT) {
                    lexer.nextToken();
                    value = false;
                } else {
                    value = true;
                }
                acceptIdentifier("PRESERVE");
                stmt.setOnCompletionPreserve(value);
            } else {
                throw new ParserException("TODO " + lexer.info());
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.RENAME)) {
            lexer.nextToken();
            accept(Token.TO);
            SQLName renameTo = this.exprParser.name();
            stmt.setRenameTo(renameTo);
        }

        if (lexer.token() == Token.ENABLE) {
            stmt.setEnable(true);
            lexer.nextToken();
        } else if (lexer.token() == Token.DISABLE) {
            lexer.nextToken();
            stmt.setEnable(false);

            if (lexer.token() == ON) {
                lexer.nextToken();
                acceptIdentifier("SLAVE");
                stmt.setDisableOnSlave(true);
            }
        }

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            SQLExpr comment = this.exprParser.primary();
            stmt.setComment(comment);
        }

        if (lexer.token() == Token.DO) {
            lexer.nextToken();
            SQLStatement eventBody = this.parseStatement();
            stmt.setEventBody(eventBody);
        } else if (lexer.token() == Token.IDENTIFIER) {
            SQLExpr expr = this.exprParser.expr();
            SQLExprStatement eventBody = new SQLExprStatement(expr);
            eventBody.setDbType(dbType);
            stmt.setEventBody(eventBody);
        }

        return stmt;
    }

    protected SQLStatement parseAlterEvent() {
        if (lexer.token() == Token.ALTER) {
            lexer.nextToken();
        }

        MySqlAlterEventStatement stmt = new MySqlAlterEventStatement();

        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
            lexer.nextToken();
            accept(Token.EQ);
            SQLName definer = this.getExprParser().userName();
            stmt.setDefiner(definer);
        }

        acceptIdentifier("EVENT");

        SQLName eventName = this.exprParser.name();
        stmt.setName(eventName);

        while (lexer.token() == ON) {
            lexer.nextToken();
            if (lexer.identifierEquals(FnvHash.Constants.SCHEDULE)) {
                lexer.nextToken();
                MySqlEventSchedule schedule = parseSchedule();
                stmt.setSchedule(schedule);
            } else if (lexer.identifierEquals(FnvHash.Constants.COMPLETION)) {
                lexer.nextToken();

                boolean value;
                if (lexer.token() == Token.NOT) {
                    lexer.nextToken();
                    value = false;
                } else {
                    value = true;
                }
                acceptIdentifier("PRESERVE");
                stmt.setOnCompletionPreserve(value);
            } else {
                throw new ParserException("TODO " + lexer.info());
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.RENAME)) {
            lexer.nextToken();
            accept(Token.TO);
            SQLName renameTo = this.exprParser.name();
            stmt.setRenameTo(renameTo);
        }

        if (lexer.token() == Token.ENABLE) {
            stmt.setEnable(true);
            lexer.nextToken();
        } else if (lexer.token() == Token.DISABLE) {
            lexer.nextToken();
            stmt.setEnable(false);

            if (lexer.token() == ON) {
                lexer.nextToken();
                acceptIdentifier("SLAVE");
                stmt.setDisableOnSlave(true);
            }
        }

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            SQLExpr comment = this.exprParser.primary();
            stmt.setComment(comment);
        }

        if (lexer.token() == Token.DO) {
            lexer.nextToken();
            SQLStatement eventBody = this.parseStatement();
            stmt.setEventBody(eventBody);
        } else if (lexer.token() == Token.IDENTIFIER) {
            SQLExpr expr = this.exprParser.expr();
            SQLExprStatement eventBody = new SQLExprStatement(expr);
            eventBody.setDbType(dbType);
            stmt.setEventBody(eventBody);
        }

        return stmt;
    }

    private MySqlEventSchedule parseSchedule() {
        MySqlEventSchedule schedule = new MySqlEventSchedule();

        if (lexer.identifierEquals(FnvHash.Constants.AT)) {
            lexer.nextToken();
            schedule.setAt(this.exprParser.expr());
        } else if (lexer.identifierEquals(FnvHash.Constants.EVERY)) {
            lexer.nextToken();
            SQLExpr value = this.exprParser.expr();
            String unit = lexer.stringVal();
            lexer.nextToken();

            SQLIntervalExpr intervalExpr = new SQLIntervalExpr();
            intervalExpr.setValue(value);
            intervalExpr.setUnit(SQLIntervalUnit.valueOf(unit.toUpperCase()));

            schedule.setEvery(intervalExpr);
        }

        if (lexer.identifierEquals(FnvHash.Constants.STARTS)) {
            lexer.nextToken();
            schedule.setStarts(this.exprParser.expr());

            if (lexer.identifierEquals(FnvHash.Constants.ENDS)) {
                lexer.nextToken();
                schedule.setEnds(this.exprParser.expr());
            }
        } else if (lexer.identifierEquals(FnvHash.Constants.ENDS)) {
            lexer.nextToken();
            schedule.setEnds(this.exprParser.expr());
        }

        return schedule;
    }

    private boolean parseAlterSpecification(SQLAlterTableStatement stmt) {
        // Specification except table options.
        switch (lexer.token()) {
        case IDENTIFIER:
            if (lexer.identifierEquals(FnvHash.Constants.ADD)) {
                lexer.nextToken();

                boolean hasConstraint = false;
                SQLName constraintSymbol = null;

                switch (lexer.token()) {
                // ADD [COLUMN] col_name column_definition [FIRST | AFTER col_name]
                // ADD [COLUMN] (col_name column_definition,...)
                case COLUMN:
                    lexer.nextToken();
                case LPAREN:
                    parseAlterTableAddColumn(stmt);
                    return true;

                // ADD {FULLTEXT|SPATIAL} [INDEX|KEY] [index_name] (key_part,...) [index_option] ...
                case FULLTEXT:
                case IDENTIFIER:
                    if (lexer.token() == Token.FULLTEXT
                        || lexer.identifierEquals(FnvHash.Constants.SPATIAL)
                        || lexer.identifierEquals(FnvHash.Constants.CLUSTERED)
                        || lexer.identifierEquals(FnvHash.Constants.CLUSTERING)
                        || lexer.identifierEquals(FnvHash.Constants.ANN)
                        || lexer.identifierEquals(FnvHash.Constants.GLOBAL)
                        || lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
                        // Index.
                        SQLAlterTableAddIndex item = new SQLAlterTableAddIndex();
                        this.exprParser.parseIndex(item.getIndexDefinition());
                        stmt.addItem(item);
                    } else if (lexer.identifierEquals(FnvHash.Constants.EXTPARTITION)) {
                        // Caution: Not in MySql documents.
                        lexer.nextToken();
                        accept(Token.LPAREN);
                        SQLAlterTableAddExtPartition extPartitionItem = new SQLAlterTableAddExtPartition();
                        MySqlExtPartition partitionDef = parseExtPartition();
                        extPartitionItem.setExPartition(partitionDef);
                        stmt.addItem(extPartitionItem);
                        accept(Token.RPAREN);
                    } else if (lexer.identifierEquals(FnvHash.Constants.ROUTE)) {
                        // Caution: Not in MySql documents.
                        lexer.nextToken();
                        SQLAlterTableAddRoute routeStatement = new SQLAlterTableAddRoute();

                        acceptIdentifier("scattered_id");
                        accept(EQ);
                        routeStatement.setScatteredId(this.exprParser.charExpr());

                        acceptIdentifier("shard_assign_count");
                        accept(EQ);
                        routeStatement.setShardAssignCount(this.exprParser.integerExpr());

                        acceptIdentifier("effective_after_seconds");
                        accept(EQ);
                        routeStatement.setEffectiveAfterSeconds(this.exprParser.integerExpr());

                        stmt.addItem(routeStatement);
                    } else {
                        // Add column.
                        parseAlterTableAddColumn(stmt);
                    }
                    return true;

                // ADD {INDEX|KEY} [index_name] [index_type] (key_part,...) [index_option] ...
                case INDEX:
                case KEY: {
                    SQLAlterTableAddIndex item = new SQLAlterTableAddIndex();
                    this.exprParser.parseIndex(item.getIndexDefinition());
                    stmt.addItem(item);
                    return true;
                }

                // ADD [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (key_part,...) [index_option] ...
                // ADD [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY] [index_name] [index_type] (key_part,...) [index_option] ...
                // ADD [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (col_name,...) reference_definition
                // ADD [CONSTRAINT [symbol]] CHECK (expr) [[NOT] ENFORCED] **new 8.0**
                case CONSTRAINT:
                    hasConstraint = true;
                    lexer.nextToken();
                    if (lexer.token() == Token.IDENTIFIER) {
                        constraintSymbol = this.exprParser.name();
                        if (lexer.token() != Token.PRIMARY && lexer.token() != Token.UNIQUE
                            && lexer.token() != Token.FOREIGN && lexer.token() != Token.CHECK) {
                            throw new ParserException(
                                "syntax error, expect PRIMARY, UNIQUE, FOREIGN or CHECK, actual " + lexer.token() + ", "
                                    + lexer.info());
                        }
                    }
                case PRIMARY:
                case UNIQUE:
                case FOREIGN:
                case CHECK:
                    // Constraint.
                    if (lexer.token() == Token.FOREIGN) {
                        MysqlForeignKey fk = this.getExprParser().parseForeignKey();
                        if (constraintSymbol != null) {
                            fk.setName(constraintSymbol);
                        }
                        fk.setHasConstraint(hasConstraint);
                        SQLAlterTableAddConstraint constraint = new SQLAlterTableAddConstraint(fk);
                        stmt.addItem(constraint);
                    } else if (lexer.token() == Token.PRIMARY) {
                        MySqlPrimaryKey pk = new MySqlPrimaryKey();
                        if (constraintSymbol != null) {
                            pk.setName(constraintSymbol);
                        }
                        pk.getIndexDefinition().setHasConstraint(hasConstraint);
                        pk.getIndexDefinition().setSymbol(constraintSymbol);
                        this.exprParser.parseIndex(pk.getIndexDefinition());
                        SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(pk);
                        stmt.addItem(item);
                    } else if (lexer.token() == Token.UNIQUE) {
                        MySqlUnique uk = new MySqlUnique();
                        uk.getIndexDefinition().setHasConstraint(hasConstraint);
                        uk.getIndexDefinition().setSymbol(constraintSymbol);
                        this.exprParser.parseIndex(uk.getIndexDefinition());
                        SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(uk);
                        stmt.addItem(item);
                    } else if (lexer.token() == Token.CHECK) {
                        lexer.nextToken();
                        accept(Token.LPAREN);
                        SQLCheck check = new SQLCheck();
                        check.setExpr(this.exprParser.expr());
                        if (constraintSymbol != null) {
                            check.setName(constraintSymbol);
                        }
                        accept(Token.RPAREN);
                        if (lexer.token() == Token.NOT) {
                            lexer.nextToken();
                            acceptIdentifier("ENFORCED");
                            check.setWithEnforced(true);
                            check.setEnforced(false);
                        } else if (lexer.token() == Token.IDENTIFIER) {
                            acceptIdentifier("ENFORCED");
                            check.setWithEnforced(true);
                            check.setEnforced(true);
                        }
                        SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(check);
                        stmt.addItem(item);
                    }
                    return true;

                // ADD PARTITION (partition_definition)
                case PARTITION: {
                    SQLAlterTableAddPartition item = parseAddPartition();
                    stmt.addItem(item);
                    return true;
                }

                default:
                    // Add column.
                    parseAlterTableAddColumn(stmt);
                    return true;
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.ALGORITHM)) {
                // ALGORITHM [=] {DEFAULT|INPLACE|COPY}
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addItem(new MySqlAlterTableOption("ALGORITHM", lexer.stringVal()));
                lexer.nextToken();
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.CHANGE)) {
                // CHANGE [COLUMN] old_col_name new_col_name column_definition [FIRST|AFTER col_name]
                lexer.nextToken();
                if (lexer.token() == Token.COLUMN) {
                    lexer.nextToken();
                }
                MySqlAlterTableChangeColumn item = new MySqlAlterTableChangeColumn();
                item.setColumnName(this.exprParser.name());
                item.setNewColumnDefinition(this.exprParser.parseColumn());
                if (lexer.identifierEquals("AFTER")) {
                    lexer.nextToken();
                    item.setAfterColumn(this.exprParser.name());
                } else if (lexer.identifierEquals("FIRST")) {
                    lexer.nextToken();
//                    if (lexer.token() == Token.IDENTIFIER) {
//                        item.setFirstColumn(this.exprParser.name());
//                    } else {
//                        item.setFirst(true);
//                    }
//                    // Why do we have [FIRST column] syntax? It is not supported by MySQL.
                    item.setFirst(true);
                }
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.CONVERT)) {
                // CONVERT TO CHARACTER SET charset_name [COLLATE collation_name]
                lexer.nextToken();
                accept(Token.TO);
                acceptIdentifier("CHARACTER");
                accept(Token.SET);
                SQLAlterTableConvertCharSet item = new SQLAlterTableConvertCharSet();
                SQLExpr charset = this.exprParser.name();
                item.setCharset(charset);
                if (lexer.identifierEquals("COLLATE")) {
                    lexer.nextToken();
                    SQLExpr collate = this.exprParser.primary();
                    item.setCollate(collate);
                }
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.DISCARD)) {
                // DISCARD PARTITION {partition_names | ALL} TABLESPACE
                // DISCARD TABLESPACE
                lexer.nextToken();
                if (lexer.token() == Token.PARTITION) {
                    lexer.nextToken();
                    SQLAlterTableDiscardPartition item = new SQLAlterTableDiscardPartition();

                    if (lexer.token() == Token.ALL) {
                        lexer.nextToken();
                        item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                    } else {
                        this.exprParser.names(item.getPartitions(), item);
                    }

                    if (lexer.token() == Token.TABLESPACE) {
                        lexer.nextToken();
                        item.setTablespace(true);
                    }

                    stmt.addItem(item);
                } else {
                    accept(Token.TABLESPACE);
                    MySqlAlterTableDiscardTablespace item = new MySqlAlterTableDiscardTablespace();
                    stmt.addItem(item);
                }
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.IMPORT)) {
                // IMPORT PARTITION {partition_names | ALL} TABLESPACE
                // IMPORT TABLESPACE
                lexer.nextToken();
                if (lexer.token() == Token.PARTITION) {
                    lexer.nextToken();
                    SQLAlterTableImportPartition item = new SQLAlterTableImportPartition();

                    if (lexer.token() == Token.ALL) {
                        lexer.nextToken();
                        item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                    } else {
                        this.exprParser.names(item.getPartitions(), item);
                    }

                    if (lexer.token() == Token.TABLESPACE) {
                        lexer.nextToken();
                        item.setTablespace(true);
                    }

                    stmt.addItem(item);
                } else {
                    accept(Token.TABLESPACE);
                    MySqlAlterTableImportTablespace item = new MySqlAlterTableImportTablespace();
                    stmt.addItem(item);
                }
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
                // FORCE
                lexer.nextToken();
                MySqlAlterTableForce item = new MySqlAlterTableForce();
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.MODIFY)) {
                lexer.nextToken();
                if (lexer.token() == Token.PARTITION) {
                    // MODIFY PARTITION [(]part_name ADD|DROP VALUES(),part_name ADD|DROP VALUES()[)]
                    lexer.nextToken();
                    boolean paren = false;
                    if (lexer.token() == Token.LPAREN) {
                        paren = true;
                        lexer.nextToken();
                    }
                    for (; ; ) {
                        SQLAlterTableModifyPartitionValues item = this.exprParser.parseModifyListPartition();
                        stmt.addItem(item);

                        if (paren && lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }

                    if (paren) {
                        accept(Token.RPAREN);
                    }
                    return true;
                } else {
                    // MODIFY [COLUMN] col_name column_definition [FIRST | AFTER col_name]
                    if (lexer.token() == Token.COLUMN) {
                        lexer.nextToken();
                    }
                    boolean paren = false;
                    if (lexer.token() == Token.LPAREN) {
                        paren = true;
                        lexer.nextToken();
                    }
                    for (; ; ) {
                        MySqlAlterTableModifyColumn item = new MySqlAlterTableModifyColumn();
                        item.setNewColumnDefinition(this.exprParser.parseColumn());
                        if (lexer.identifierEquals("AFTER")) {
                            lexer.nextToken();
                            item.setAfterColumn(this.exprParser.name());
                        } else if (lexer.identifierEquals("FIRST")) {
                            lexer.nextToken();
//                            if (lexer.token() == Token.IDENTIFIER) {
//                                item.setFirstColumn(this.exprParser.name());
//                            } else {
//                                item.setFirst(true);
//                            }
//                            // Why do we have [FIRST column] syntax? It is not supported by MySQL.
                            item.setFirst(true);
                        }
                        stmt.addItem(item);

                        if (paren && lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }

                    if (paren) {
                        accept(Token.RPAREN);
                    }
                }
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.RENAME)) {
                Lexer.SavePoint mark = lexer.mark();
                // RENAME {INDEX|KEY} old_index_name TO new_index_name
                // RENAME [TO|AS] new_tbl_name
                lexer.nextToken();

                switch (lexer.token()) {
                case INDEX:
                case KEY: {
                    lexer.nextToken();
                    SQLName name = this.exprParser.name();
                    accept(Token.TO);
                    SQLName to = this.exprParser.name();
                    SQLAlterTableRenameIndex item = new SQLAlterTableRenameIndex(name, to);
                    stmt.addItem(item);
                    return true;
                }

                case COLUMN: {
                    lexer.nextToken();
                    SQLName columnName = exprParser.name();
                    accept(Token.TO);
                    SQLName toName = this.exprParser.name();
                    SQLAlterTableRenameColumn renameColumn = new SQLAlterTableRenameColumn();
                    renameColumn.setColumn(columnName);
                    renameColumn.setTo(toName);
                    stmt.addItem(renameColumn);
                    return true;
                }

                case TO:
                case AS:
                    lexer.nextToken();
                case IDENTIFIER:
                    SQLAlterTableRename item = new SQLAlterTableRename();
                    SQLName to = this.exprParser.name();
                    item.setTo(to);
                    stmt.addItem(item);
                    return true;
                case PARTITION:
                    lexer.reset(mark);
                    stmt.addItem(parseRenamePartition());
                    return true;
                default:
                    break;
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.WITHOUT)) {
                // WITHOUT VALIDATION
                lexer.nextToken();
                acceptIdentifier("VALIDATION");
                MySqlAlterTableValidation item = new MySqlAlterTableValidation();
                item.setWithValidation(false);
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals("COALESCE")) {
                lexer.nextToken();
                accept(Token.PARTITION);

                SQLAlterTableCoalescePartition item = new SQLAlterTableCoalescePartition();
                SQLIntegerExpr countExpr = this.exprParser.integerExpr();
                item.setCount(countExpr);
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals("ALLOCATE")) {
                lexer.nextToken();
                acceptIdentifier("LOCAL");
                accept(Token.PARTITION);
                DrdsAlterTableAllocateLocalPartition item = new DrdsAlterTableAllocateLocalPartition();
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals("EXPIRE")) {
                lexer.nextToken();
                acceptIdentifier("LOCAL");
                accept(Token.PARTITION);
                DrdsAlterTableExpireLocalPartition item = new DrdsAlterTableExpireLocalPartition();
                this.exprParser.names(item.getPartitions(), item);
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals("REORGANIZE")) {
                    // REORGANIZE PARTITION partition_names INTO (partition_definitions)
                    lexer.nextToken();
                    accept(Token.PARTITION);

                SQLAlterTableReOrganizePartition item = new SQLAlterTableReOrganizePartition();

                this.exprParser.names(item.getNames(), item);

                accept(Token.INTO);
                accept(Token.LPAREN);
                for (; ; ) {
                    SQLPartition partition = this.getExprParser().parsePartition();

                    item.addPartition(partition);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    } else {
                        break;
                    }
                }
                accept(Token.RPAREN);
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.EXCHANGE)) {
                // EXCHANGE PARTITION partition_name WITH TABLE tbl_name [{WITH|WITHOUT} VALIDATION]
                lexer.nextToken();
                accept(Token.PARTITION);

                SQLAlterTableExchangePartition item = new SQLAlterTableExchangePartition();

                SQLName partition = this.exprParser.name();
                item.addPartition(partition);

                accept(Token.WITH);
                accept(Token.TABLE);
                SQLName table = this.exprParser.name();
                item.setTable(table);

                if (lexer.token() == Token.WITH) {
                    lexer.nextToken();
                    acceptIdentifier("VALIDATION");
                    item.setValidation(true);
                } else if (lexer.identifierEquals(FnvHash.Constants.WITHOUT)) {
                    lexer.nextToken();
                    acceptIdentifier("VALIDATION");
                    item.setValidation(false);
                }
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals("REBUILD")) {
                // REBUILD PARTITION {partition_names | ALL}
                lexer.nextToken();
                accept(Token.PARTITION);

                SQLAlterTableRebuildPartition item = new SQLAlterTableRebuildPartition();

                if (lexer.token() == Token.ALL) {
                    lexer.nextToken();
                    item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                } else {
                    this.exprParser.names(item.getPartitions(), item);
                }
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals("REPAIR")) {
                // REPAIR PARTITION {partition_names | ALL}
                lexer.nextToken();
                accept(Token.PARTITION);

                SQLAlterTableRepairPartition item = new SQLAlterTableRepairPartition();

                if (lexer.token() == Token.ALL) {
                    lexer.nextToken();
                    item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                } else {
                    this.exprParser.names(item.getPartitions(), item);
                }
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.REMOVE)) {
                // REMOVE PARTITIONING
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
                    acceptIdentifier("LOCAL");
                    acceptIdentifier("PARTITIONING");
                    stmt.setRemoveLocalPatiting(true);
                    return true;
                }
                acceptIdentifier("PARTITIONING");
                stmt.setRemovePatiting(true);
            } else if (lexer.identifierEquals("UPGRADE")) {
                // UPGRADE PARTITIONING
                lexer.nextToken();
                acceptIdentifier("PARTITIONING");
                stmt.setUpgradePatiting(true);
            } else if (lexer.identifierEquals("HOT_PARTITION_COUNT")) {
                // UPGRADE PARTITIONING
                lexer.nextToken();
                accept(EQ);
                try {
                    stmt.getTableOptions().add(
                        new SQLAssignItem(new SQLIdentifierExpr("HOT_PARTITION_COUNT"), this.exprParser.integerExpr()));
                } catch (Exception e) {
                    throw new ParserException("only integer number is supported for hot_partition_count");
                }
            }

            //
            // Other not in MySql documents.
            //
            else if (lexer.identifierEquals(FnvHash.Constants.DBPARTITION)) {
                lexer.nextToken();
                accept(Token.BY);

                DrdsAlterTablePartition item = new DrdsAlterTablePartition();
                SQLExpr dbPartitoinBy = this.exprParser.primary();
                item.setDbPartitionBy(dbPartitoinBy);

                if (lexer.identifierEquals(FnvHash.Constants.TBPARTITION)) {
                    lexer.nextToken();
                    accept(Token.BY);
                    SQLExpr expr = this.exprParser.expr();
                    item.setTablePartitionBy(expr);

                    if (lexer.identifierEquals(FnvHash.Constants.TBPARTITIONS)) {
                        lexer.nextToken();
                        SQLExpr tbPartitions = this.exprParser.primary();
                        item.setTablePartitions(tbPartitions);
                    }
                }
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.BROADCAST)) {
                lexer.nextToken();
                DrdsAlterTableBroadcast item = new DrdsAlterTableBroadcast();
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.SINGLE)) {
                lexer.nextToken();
                DrdsAlterTableSingle item = new DrdsAlterTableSingle();
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
                // Caution: Not in MySql documents.
                SQLAlterTablePartitionCount item = new SQLAlterTablePartitionCount();
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                item.setCount((SQLIntegerExpr) exprParser.integerExpr());
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION)) {
                // Caution: Not in MySql documents.
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.LIFECYCLE)) {
                    lexer.nextToken();
                    SQLAlterTableSubpartitionLifecycle item = new SQLAlterTableSubpartitionLifecycle();
                    if (lexer.token() == Token.LITERAL_INT) {
                        for (; ; ) {
                            item.getPartitionIds().add(this.exprParser.integerExpr());
                            String pidStr = lexer.stringVal();
                            accept(Token.VARIANT);
                            String s = pidStr.replaceAll(":", "");
                            if (StringUtils.isEmpty(s)) {
                                item.getSubpartitionLifeCycle().add(exprParser.integerExpr());
                            } else {
                                item.getSubpartitionLifeCycle().add(new SQLIntegerExpr(Integer.valueOf(s)));
                            }

                            if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }

                            break;
                        }
                    }
                    stmt.addItem(item);
                }
                return true;
            } else if (lexer.identifierEquals("BLOCK_SIZE")) {
                // Caution: Not in MySql documents.
                SQLAlterTableBlockSize item = new SQLAlterTableBlockSize();
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    accept(Token.EQ);
                }
                item.setSize((SQLIntegerExpr) exprParser.expr());
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals(INSERT_METHOD)) {
                // Caution: Not in MySql documents.
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions()
                    .add(new SQLAssignItem(new SQLIdentifierExpr(INSERT_METHOD), this.exprParser.primary()));
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.CLUSTERED)) {
                // Caution: Not in MySql documents.
                SQLAlterTableModifyClusteredBy clusteredBy = new SQLAlterTableModifyClusteredBy();

                acceptIdentifier("CLUSTERED");
                accept(Token.BY);
                accept(Token.LPAREN);

                // for ads: ALTER TABLE SCHEMA1.TABLE1 CLUSTERED BY ();
                if (lexer.token() != Token.RPAREN) {
                    for (; ; ) {
                        clusteredBy.addClusterColumn(this.exprParser.name());

                        if (lexer.token() == Token.COMMA) {
                            accept(Token.COMMA);
                            continue;
                        }
                        break;
                    }
                }

                accept(Token.RPAREN);

                stmt.addItem(clusteredBy);
                return true;
            } else if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION_AVAILABLE_PARTITION_NUM)) {
                // Caution: Not in MySql documents.
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLIntegerExpr num = this.exprParser.integerExpr();
                SQLAlterTableSubpartitionAvailablePartitionNum item =
                    new SQLAlterTableSubpartitionAvailablePartitionNum();
                item.setNumber(num);
                stmt.addItem(item);
                return true;
            } else if (lexer.identifierEquals("SPLIT")) {
                acceptIdentifier("SPLIT");
                if (lexer.token() == Token.INTO) {
                    DrdsSplitHotKey item = parseSplitHotKey();
                    stmt.addItem(item);
                } else {
                    DrdsSplitPartition item = parseSplitPartition();
                    stmt.addItem(item);
                }
            } else if (lexer.identifierEquals("EXTRACT")) {
                DrdsExtractHotKey item = parseExtractHotKey();
                stmt.addItem(item);
            } else if (lexer.identifierEquals("MOVE")) {
                DrdsMovePartition item = parseMovePartition();
                stmt.addItem(item);
            } else if (lexer.identifierEquals("PURGE")) {
                lexer.nextToken();
                acceptIdentifier("BEFORE");
                acceptIdentifier("TIMESTAMP");
                SQLExpr timeStamp = this.exprParser.expr();
                stmt.addItem(new DrdsAlterTablePurgeBeforeTimeStamp(timeStamp));
                return true;
            }
            break;

        case ALTER: {
            lexer.nextToken();
            if (lexer.token() == Token.INDEX) {
                // Caution: Not in MySql documents.
                lexer.nextToken();
                MySqlAlterTableAlterFullTextIndex alterIndex = new MySqlAlterTableAlterFullTextIndex();
                SQLName indexName = this.exprParser.name();
                alterIndex.setIndexName(indexName);

                accept(Token.SET);
                accept(Token.FULLTEXT);

                if (lexer.token() == Token.INDEX) {
                    lexer.nextToken();
                    alterIndex.setAnalyzerType(AnalyzerIndexType.INDEX);
                } else if (lexer.identifierEquals(FnvHash.Constants.QUERY)) {
                    lexer.nextToken();
                    alterIndex.setAnalyzerType(AnalyzerIndexType.QUERY);
                }

                acceptIdentifier("ANALYZER");

                accept(Token.EQ);

                alterIndex.setAnalyzerName(this.exprParser.name());
                stmt.addItem(alterIndex);
            } else if (lexer.token() == Token.CHECK) {
                lexer.nextToken();
                MySqlAlterTableCheckConstraint checkAlter = new MySqlAlterTableCheckConstraint();
                SQLName checkName = this.exprParser.name();
                checkAlter.setCheckName(checkName);

                if (lexer.token() == Token.NOT) {
                    lexer.nextToken();
                    checkAlter.setEnforced(false);
                } else {
                    checkAlter.setEnforced(true);
                }

                acceptIdentifier("ENFORCED");

                stmt.addItem(checkAlter);
            } else {
                // ALTER [COLUMN] col_name {SET DEFAULT literal | DROP DEFAULT}
                if (lexer.token() == Token.COLUMN) {
                    lexer.nextToken();
                }
                MySqlAlterTableAlterColumn alterColumn = new MySqlAlterTableAlterColumn();
                alterColumn.setColumn(this.exprParser.name());
                if (lexer.token() == Token.SET) {
                    lexer.nextToken();
                    accept(Token.DEFAULT);
                    alterColumn.setDefaultExpr(this.exprParser.expr());
                } else {
                    accept(Token.DROP);
                    accept(Token.DEFAULT);
                    alterColumn.setDropDefault(true);
                }
                stmt.addItem(alterColumn);
            }
            return true;
        }

        // [DEFAULT] CHARACTER SET [=] charset_name [COLLATE [=] collation_name]
        // parse in table options.

        case DISABLE:
            lexer.nextToken();
            if (lexer.token() == Token.CONSTRAINT) {
                // Caution: Not in MySql documents.
                lexer.nextToken();
                SQLAlterTableDisableConstraint item = new SQLAlterTableDisableConstraint();
                item.setConstraintName(this.exprParser.name());
                stmt.addItem(item);
            } else {
                // DISABLE KEYS
                acceptIdentifier("KEYS");
                SQLAlterTableDisableKeys item = new SQLAlterTableDisableKeys();
                stmt.addItem(item);
            }
            return true;

        case ENABLE:
            lexer.nextToken();
            if (lexer.token() == Token.CONSTRAINT) {
                // Caution: Not in MySql documents.
                lexer.nextToken();
                SQLAlterTableEnableConstraint item = new SQLAlterTableEnableConstraint();
                item.setConstraintName(this.exprParser.name());
                stmt.addItem(item);
            } else {
                // ENABLE KEYS
                acceptIdentifier("KEYS");
                SQLAlterTableEnableKeys item = new SQLAlterTableEnableKeys();
                stmt.addItem(item);
            }
            return true;

        case LOCK: {
            // LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            MySqlAlterTableLock item = new MySqlAlterTableLock();
            item.setLockType(new SQLIdentifierExpr(lexer.stringVal()));
            lexer.nextToken();
            stmt.addItem(item);
            return true;
        }

        case ORDER: {
            lexer.nextToken();
            accept(Token.BY);
            MySqlAlterTableOrderBy item = new MySqlAlterTableOrderBy();
            while (true) {
                if (lexer.token() == Token.IDENTIFIER) {
                    SQLSelectOrderByItem column = this.exprParser.parseSelectOrderByItem();
                    column.setParent(item);
                    item.addColumn(column);
                } else {
                    break;
                }
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                } else {
                    break;
                }
            }
            stmt.addItem(item);
            return true;
        }

        case WITH: {
            // WITH VALIDATION
            lexer.nextToken();
            acceptIdentifier("VALIDATION");
            MySqlAlterTableValidation item = new MySqlAlterTableValidation();
            item.setWithValidation(true);
            stmt.addItem(item);
            return true;
        }

        case DROP:
            // DROP [COLUMN] col_name
            // DROP {INDEX|KEY} index_name
            // DROP PRIMARY KEY
            // DROP FOREIGN KEY fk_symbol
            // DROP PARTITION partition_names
            // TODO: need check.
            parseAlterDrop(stmt);
            return true;

        case TRUNCATE: {
            // TRUNCATE PARTITION {partition_names | ALL}
            lexer.nextToken();
            accept(Token.PARTITION);
            SQLAlterTableTruncatePartition item = new SQLAlterTableTruncatePartition();
            if (lexer.token() == Token.ALL) {
                item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                lexer.nextToken();
            } else {
                this.exprParser.names(item.getPartitions(), item);
            }
            stmt.addItem(item);
            return true;
        }

        case ANALYZE: {
            // ANALYZE PARTITION {partition_names | ALL}
            lexer.nextToken();
            accept(Token.PARTITION);
            SQLAlterTableAnalyzePartition item = new SQLAlterTableAnalyzePartition();
            if (lexer.token() == Token.ALL) {
                lexer.nextToken();
                item.getPartitions().add(new SQLIdentifierExpr("ALL"));
            } else {
                this.exprParser.names(item.getPartitions(), item);
            }
            stmt.addItem(item);
            return true;
        }

        case CHECK: {
            // CHECK PARTITION {partition_names | ALL}
            lexer.nextToken();
            accept(Token.PARTITION);
            SQLAlterTableCheckPartition item = new SQLAlterTableCheckPartition();
            if (lexer.token() == Token.ALL) {
                lexer.nextToken();
                item.getPartitions().add(new SQLIdentifierExpr("ALL"));
            } else {
                this.exprParser.names(item.getPartitions(), item);
            }
            stmt.addItem(item);
            return true;
        }

        case OPTIMIZE: {
            // OPTIMIZE PARTITION {partition_names | ALL}
            lexer.nextToken();
            accept(Token.PARTITION);
            SQLAlterTableOptimizePartition item = new SQLAlterTableOptimizePartition();
            if (lexer.token() == Token.ALL) {
                lexer.nextToken();
                item.getPartitions().add(new SQLIdentifierExpr("ALL"));
            } else {
                this.exprParser.names(item.getPartitions(), item);
            }
            stmt.addItem(item);
            return true;
        }

        //
        // Other not in MySql documents.
        //

        case SET:
            lexer.nextToken();

            if (lexer.identifierEquals(FnvHash.Constants.RULE)) {
                SQLAlterTableSetOption setOption = new SQLAlterTableSetOption();
                SQLAssignItem item = this.exprParser.parseAssignItem();
                setOption.addOption(item);
                stmt.addItem(setOption);
            } else if (lexer.identifierEquals("TABLEGROUP")) {
                SQLAlterTableSetOption setOption = new SQLAlterTableSetOption();
                SQLAssignItem item = this.exprParser.parseAssignItem();
                setOption.addOption(item);
                setOption.setAlterTableGroup(true);
                if (lexer.identifierEquals("FORCE")) {
                    setOption.setForce(true);
                    lexer.nextToken();
                }
                stmt.addItem(setOption);
            } else {
                acceptIdentifier("TBLPROPERTIES");
                SQLAlterTableSetOption setOption = new SQLAlterTableSetOption();
                accept(Token.LPAREN);
                for (; ; ) {
                    SQLAssignItem item = this.exprParser.parseAssignItem();
                    setOption.addOption(item);
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
                stmt.addItem(setOption);

                if (lexer.token() == ON) {
                    lexer.nextToken();
                    SQLName on = this.exprParser.name();
                    setOption.setOn(on);
                }
            }

            return true;
        case AS: {
            lexer.nextToken();
            accept(Token.OF);
            acceptIdentifier("TIMESTAMP");
            SQLExpr timeStamp = this.exprParser.expr();
            stmt.addItem(new DrdsAlterTableAsOfTimeStamp(timeStamp));
            return true;
        }
        case PARTITION: {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.identifierEquals(FnvHash.Constants.LIFECYCLE)) {
                lexer.nextToken();
                SQLAlterTablePartitionLifecycle item = new SQLAlterTablePartitionLifecycle();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                item.setLifecycle((SQLIntegerExpr) exprParser.integerExpr());
                stmt.addItem(item);
                return true;
            } else {
                lexer.reset(mark);
            }
            break;
        }
        case MERGE: {
            DrdsMergePartition item = parseMergePartition();
            stmt.addItem(item);
            break;
        }

        }
        return false;
    }

    protected void parseRotateInnodbMasterKey(MySqlAlterInstanceStatement stmt) {
        lexer.nextToken();
        if (lexer.identifierEquals("MASTER")) {
            lexer.nextToken();
            if (lexer.identifierEquals("KEY") || lexer.token().name().equalsIgnoreCase("KEY")) {
                lexer.nextToken();
                stmt.setItem(new MySQLRotateInnodbMasterKey());
                return;
            }
        }
        throw new ParserException("syntax error," + lexer.info());
    }

    protected SQLStatement parseAlterInstance() {
        MySqlAlterInstanceStatement stmt = new MySqlAlterInstanceStatement();
        lexer.nextToken();
        if (lexer.identifierEquals("ROTATE")) {
            lexer.nextToken();
            if (lexer.identifierEquals("INNODB")) {
                parseRotateInnodbMasterKey(stmt);
            }
        }
        return stmt;
    }

    protected SQLStatement parseAlterTable(boolean ignore, boolean online, boolean offline) {
        lexer.nextToken();

        SQLAlterTableStatement stmt = new SQLAlterTableStatement(getDbType());
        stmt.setIgnore(ignore);
        stmt.setOnline(online);
        stmt.setOffline(offline);
        stmt.setName(this.exprParser.name());

        while (true) {
            boolean parsed = ((MySqlExprParser) this.exprParser).parseTableOptions(stmt.getTableOptions(), stmt);
            if (!parsed) {
                parsed = parseAlterSpecification(stmt);
            }

            if (parsed) {
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                }
            } else {
                break;
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
            SQLPartitionBy localPartitionClause = this.getSQLCreateTableParser().parseLocalPartitionBy();
            stmt.setLocalPartition(localPartitionClause);
        }

        // partition_options
        if (Token.PARTITION == lexer.token()) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (Token.BY == lexer.token()) {
                lexer.reset(mark);
                SQLPartitionBy partitionBy = this.getSQLCreateTableParser().parsePartitionBy();
                stmt.setPartition(partitionBy);
            } else {
                acceptIdentifier("align");
                accept(Token.TO);
                SQLName tableGroupName = this.exprParser.name();
                DrdsAlignToTableGroup alignToTableGroup = new DrdsAlignToTableGroup();
                alignToTableGroup.setTablegroup(tableGroupName);
                lexer.nextToken();
                stmt.setAlignToTableGroup(alignToTableGroup);
            }

        } else {
            // Change to rename table if only one rename to xx.
            if (1 == stmt.getItems().size() && stmt.getItems().get(0) instanceof SQLAlterTableRename) {
                MySqlRenameTableStatement renameStmt = new MySqlRenameTableStatement();
                MySqlRenameTableStatement.Item item = new MySqlRenameTableStatement.Item();
                item.setName((SQLName) stmt.getTableSource().getExpr());
                item.setTo(((SQLAlterTableRename) stmt.getItems().get(0)).getToName());
                renameStmt.addItem(item);
                return renameStmt;
            }
        }

        return stmt;
    }

    /*
    protected SQLStatement parseAlterTableOld(boolean ignore) {
        lexer.nextToken();

        SQLAlterTableStatement stmt = new SQLAlterTableStatement(getDbType());
        stmt.setIgnore(ignore);
        stmt.setName(this.exprParser.name());

        for_:
        for (; ; ) {
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            switch (lexer.token()) {
                case DROP: {
                    parseAlterDrop(stmt);
                    break;
                }
                case TRUNCATE: {
                    lexer.nextToken();
                    accept(Token.PARTITION);

                    SQLAlterTableTruncatePartition item = new SQLAlterTableTruncatePartition();
                    if (lexer.token() == Token.ALL) {
                        item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                        lexer.nextToken();
                    } else {
                        this.exprParser.names(item.getPartitions(), item);
                    }
                    stmt.addItem(item);
                    break;
                }
                case ALTER: {
                    lexer.nextToken();
                    if (lexer.token() == Token.INDEX) {
                        lexer.nextToken();
                        MySqlAlterTableAlterFullTextIndex alterIndex = new MySqlAlterTableAlterFullTextIndex();
                        SQLName indexName = this.exprParser.name();
                        alterIndex.setIndexName(indexName);

                        accept(Token.SET);
                        accept(Token.FULLTEXT);

                        if (lexer.token() == Token.INDEX) {
                            lexer.nextToken();
                            alterIndex.setAnalyzerType(AnalyzerIndexType.INDEX);
                        } else if (lexer.identifierEquals(FnvHash.Constants.QUERY)) {
                            lexer.nextToken();
                            alterIndex.setAnalyzerType(AnalyzerIndexType.QUERY);
                        }

                        acceptIdentifier("ANALYZER");

                        accept(Token.EQ);

                        alterIndex.setAnalyzerName(this.exprParser.name());
                        stmt.addItem(alterIndex);
                    } else {

                        if (lexer.token() == Token.COLUMN) {
                            lexer.nextToken();
                        }

                        MySqlAlterTableAlterColumn alterColumn = new MySqlAlterTableAlterColumn();
                        alterColumn.setColumn(this.exprParser.name());

                        if (lexer.token() == Token.SET) {
                            lexer.nextToken();
                            accept(Token.DEFAULT);

                            alterColumn.setDefaultExpr(this.exprParser.expr());
                        } else {
                            accept(Token.DROP);
                            accept(Token.DEFAULT);
                            alterColumn.setDropDefault(true);
                        }

                        stmt.addItem(alterColumn);
                    }
                    break;
                }
                case DISABLE: {
                    lexer.nextToken();
                    if (lexer.token() == Token.CONSTRAINT) {
                        lexer.nextToken();
                        SQLAlterTableDisableConstraint item = new SQLAlterTableDisableConstraint();
                        item.setConstraintName(this.exprParser.name());
                        stmt.addItem(item);
                    } else {
                        acceptIdentifier("KEYS");
                        SQLAlterTableDisableKeys item = new SQLAlterTableDisableKeys();
                        stmt.addItem(item);
                    }
                    break;
                }
                case ENABLE: {
                    lexer.nextToken();
                    if (lexer.token() == Token.CONSTRAINT) {
                        lexer.nextToken();
                        SQLAlterTableEnableConstraint item = new SQLAlterTableEnableConstraint();
                        item.setConstraintName(this.exprParser.name());
                        stmt.addItem(item);
                    } else {
                        acceptIdentifier("KEYS");
                        SQLAlterTableEnableKeys item = new SQLAlterTableEnableKeys();
                        stmt.addItem(item);
                    }
                    break;
                }
                case DEFAULT: {
                    lexer.nextToken();
                    if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
                        SQLAlterCharacter item = alterTableCharacter();
                        stmt.addItem(item);
                    } else if (lexer.identifierEquals(FnvHash.Constants.COLLATE)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        SQLAlterCharacter item = new SQLAlterCharacter();
                        item.setCollate(this.exprParser.primary());
                        stmt.addItem(item);
                    } else {
                        throw new ParserException("TODO " + lexer.info());
                    }
                    continue for_;
                }
                case CHECK: {
                    lexer.nextToken();
                    accept(Token.PARTITION);

                    SQLAlterTableCheckPartition item = new SQLAlterTableCheckPartition();

                    if (lexer.token() == Token.ALL) {
                        lexer.nextToken();
                        item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                    } else {
                        this.exprParser.names(item.getPartitions(), item);
                    }

                    stmt.addItem(item);
                    break;
                }
                case OPTIMIZE: {
                    lexer.nextToken();

                    accept(Token.PARTITION);

                    SQLAlterTableOptimizePartition item = new SQLAlterTableOptimizePartition();

                    if (lexer.token() == Token.ALL) {
                        lexer.nextToken();
                        item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                    } else {
                        this.exprParser.names(item.getPartitions(), item);
                    }

                    stmt.addItem(item);
                    break;
                }
                case ANALYZE: {
                    lexer.nextToken();
                    accept(Token.PARTITION);

                    SQLAlterTableAnalyzePartition item = new SQLAlterTableAnalyzePartition();

                    if (lexer.token() == Token.ALL) {
                        lexer.nextToken();
                        item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                    } else {
                        this.exprParser.names(item.getPartitions(), item);
                    }
                    stmt.addItem(item);
                    break;
                }
                case COMMENT: {
                    lexer.nextToken();
                    if (lexer.token() == Token.EQ) {
                        accept(Token.EQ);
                    }
                    stmt.getTableOptions().put("COMMENT", this.exprParser.charExpr());
                    continue for_;
                }
                case UNION: {
                    lexer.nextToken();
                    if (lexer.token() == Token.EQ) {
                        lexer.nextToken();
                    }

                    accept(Token.LPAREN);
                    SQLTableSource tableSrc = this.createSQLSelectParser().parseTableSource();
                    stmt.getTableOptions().put("UNION", tableSrc);
                    accept(Token.RPAREN);
                    continue for_;
                }
                case SET: {
                    lexer.nextToken();

                    if (lexer.identifierEquals(FnvHash.Constants.RULE)) {
                        SQLAlterTableSetOption setOption = new SQLAlterTableSetOption();
                        SQLAssignItem item = this.exprParser.parseAssignItem();
                        setOption.addOption(item);
                        stmt.addItem(setOption);
                    } else {
                        acceptIdentifier("TBLPROPERTIES");
                        SQLAlterTableSetOption setOption = new SQLAlterTableSetOption();
                        accept(Token.LPAREN);
                        for (; ; ) {
                            SQLAssignItem item = this.exprParser.parseAssignItem();
                            setOption.addOption(item);
                            if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                            break;
                        }
                        accept(Token.RPAREN);
                        stmt.addItem(setOption);

                        if (lexer.token() == Token.ON) {
                            lexer.nextToken();
                            SQLName on = this.exprParser.name();
                            setOption.setOn(on);
                        }
                    }

                    break;
                }
                default: {
                    if (lexer.identifierEquals(FnvHash.Constants.ADD)) {
                        lexer.nextToken();

                        if (lexer.token() == Token.COLUMN) {
                            lexer.nextToken();
                            parseAlterTableAddColumn(stmt);
                        } else if (lexer.token() == Token.INDEX
                                || lexer.token() == Token.FULLTEXT
                                || lexer.identifierEquals(FnvHash.Constants.SPATIAL)
                                || lexer.identifierEquals(FnvHash.Constants.CLUSTERED)
                                || lexer.identifierEquals(FnvHash.Constants.ANN)
                                || lexer.identifierEquals(FnvHash.Constants.GLOBAL)
                                || lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
                            SQLAlterTableAddIndex item = parseAlterTableAddIndex();
                            item.setParent(stmt);
                            stmt.addItem(item);
                        } else if (lexer.token() == Token.UNIQUE) {
                            SQLAlterTableAddIndex item = parseAlterTableAddIndex();
                            item.setParent(stmt);
                            stmt.addItem(item);
                        } else if (lexer.token() == Token.PRIMARY) {
                            SQLPrimaryKey primaryKey = this.exprParser.parsePrimaryKey();
                            SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(primaryKey);
                            stmt.addItem(item);
                        } else if (lexer.token() == Token.KEY) {
                            // throw new ParserException("TODO " + lexer.token() +
                            // " " + lexer.stringVal());
                            SQLAlterTableAddIndex item = parseAlterTableAddIndex();
                            item.setParent(stmt);
                            stmt.addItem(item);
                        } else if (lexer.identifierEquals(FnvHash.Constants.CLUSTERING)) {
                            SQLAlterTableAddClusteringKey item = parseAlterTableAddClusteringKey();
                            item.setParent(stmt);
                            stmt.addItem(item);
                        } else if (lexer.token() == Token.FOREIGN) {
                            MysqlForeignKey fk = this.getExprParser().parseForeignKey();
                            SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(fk);

                            stmt.addItem(item);
                        } else if (lexer.token() == Token.CONSTRAINT) {
                            lexer.nextToken();

                            if (lexer.token() == Token.PRIMARY) {
                                SQLPrimaryKey primaryKey = ((MySqlExprParser) this.exprParser).parsePrimaryKey();
                                SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(primaryKey);
                                item.setParent(stmt);

                                stmt.addItem(item);
                            } else if (lexer.token() == Token.FOREIGN) {
                                MysqlForeignKey fk = this.getExprParser().parseForeignKey();
                                fk.setHasConstraint(true);

                                SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(fk);

                                stmt.addItem(item);
                            } else if (lexer.token() == Token.UNIQUE) {

                                SQLUnique unique = this.exprParser.parseUnique();
                                SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(unique);
                                stmt.addItem(item);
                            } else {
                                SQLName constraintName = this.exprParser.name();

                                if (lexer.token() == Token.PRIMARY) {
                                    SQLPrimaryKey primaryKey = ((MySqlExprParser) this.exprParser).parsePrimaryKey();

                                    primaryKey.setName(constraintName);

                                    SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(primaryKey);
                                    item.setParent(stmt);

                                    stmt.addItem(item);
                                } else if (lexer.token() == Token.FOREIGN) {
                                    MysqlForeignKey fk = this.getExprParser().parseForeignKey();
                                    fk.setName(constraintName);
                                    fk.setHasConstraint(true);

                                    SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(fk);

                                    stmt.addItem(item);
                                } else if (lexer.token() == Token.UNIQUE) {
                                    SQLUnique unique = this.exprParser.parseUnique();
                                    SQLAlterTableAddConstraint item = new SQLAlterTableAddConstraint(unique);
                                    stmt.addItem(item);
                                } else {
                                    throw new ParserException("TODO " + lexer.info());
                                }
                            }
                        } else if (lexer.token() == Token.PARTITION) {
                            lexer.nextToken();

                            SQLAlterTableAddPartition item = new SQLAlterTableAddPartition();

                            if (lexer.identifierEquals("PARTITIONS")) {
                                lexer.nextToken();
                                item.setPartitionCount(this.exprParser.integerExpr());
                            }

                            if (lexer.token() == Token.LPAREN) {
                                lexer.nextToken();
                                SQLPartition partition = this.getExprParser().parsePartition();
                                accept(Token.RPAREN);
                                item.addPartition(partition);
                            }

                            stmt.addItem(item);
                        } else if (lexer.identifierEquals(FnvHash.Constants.EXTPARTITION)) {
                            lexer.nextToken();
                            accept(Token.LPAREN);
                            SQLAlterTableAddExtPartition extPartitionItem = new SQLAlterTableAddExtPartition();
                            MySqlExtPartition partitionDef = parseExtPartition();
                            extPartitionItem.setExPartition(partitionDef);
                            stmt.addItem(extPartitionItem);
                            accept(Token.RPAREN);
                        } else {
                            parseAlterTableAddColumn(stmt);
                        }
                    } else if (lexer.identifierEquals(FnvHash.Constants.CHANGE)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.COLUMN) {
                            lexer.nextToken();
                        }
                        MySqlAlterTableChangeColumn item = new MySqlAlterTableChangeColumn();
                        item.setColumnName(this.exprParser.name());
                        item.setNewColumnDefinition(this.exprParser.parseColumn());
                        if (lexer.identifierEquals("AFTER")) {
                            lexer.nextToken();
                            item.setAfterColumn(this.exprParser.name());
                        } else if (lexer.identifierEquals("FIRST")) {
                            lexer.nextToken();
                            if (lexer.token() == Token.IDENTIFIER) {
                                item.setFirstColumn(this.exprParser.name());
                            } else {
                                item.setFirst(true);
                            }
                        }
                        stmt.addItem(item);
                    } else if (lexer.identifierEquals(FnvHash.Constants.MODIFY)) {
                        lexer.nextToken();

                        if (lexer.token() == Token.COLUMN) {
                            lexer.nextToken();
                        }

                        boolean paren = false;
                        if (lexer.token() == Token.LPAREN) {
                            paren = true;
                            lexer.nextToken();
                        }

                        for (; ; ) {
                            MySqlAlterTableModifyColumn item = new MySqlAlterTableModifyColumn();
                            item.setNewColumnDefinition(this.exprParser.parseColumn());
                            if (lexer.identifierEquals("AFTER")) {
                                lexer.nextToken();
                                item.setAfterColumn(this.exprParser.name());
                            } else if (lexer.identifierEquals("FIRST")) {
                                lexer.nextToken();
                                if (lexer.token() == Token.IDENTIFIER) {
                                    item.setFirstColumn(this.exprParser.name());
                                } else {
                                    item.setFirst(true);
                                }
                            }
                            stmt.addItem(item);

                            if (paren && lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                            break;
                        }

                        if (paren) {
                            accept(Token.RPAREN);
                        }
                    } else if (lexer.identifierEquals(FnvHash.Constants.RENAME)) {
                        lexer.nextToken();

                        if (lexer.token() == Token.INDEX) {
                            lexer.nextToken();
                            SQLName name = this.exprParser.name();
                            accept(Token.TO);
                            SQLName to = this.exprParser.name();
                            SQLAlterTableRenameIndex item = new SQLAlterTableRenameIndex(name, to);
                            stmt.addItem(item);
                            continue for_;
                        }

                        if (lexer.token() == Token.COLUMN) {
                            lexer.nextToken();

                            SQLName columnName = exprParser.name();
                            accept(Token.TO);
                            SQLName toName = this.exprParser.name();
                            SQLAlterTableRenameColumn renameColumn = new SQLAlterTableRenameColumn();

                            renameColumn.setColumn(columnName);
                            renameColumn.setTo(toName);
                            stmt.addItem(renameColumn);
                            continue for_;
                        }

                        if (lexer.token() == Token.TO || lexer.token() == Token.AS) {
                            lexer.nextToken();
                        }

                        if (stmt.getItems().size() > 0) {
                            SQLAlterTableRename item = new SQLAlterTableRename();
                            SQLName to = this.exprParser.name();
                            item.setTo(to);
                            stmt.addItem(item);
                        } else {
                            MySqlRenameTableStatement renameStmt = new MySqlRenameTableStatement();
                            MySqlRenameTableStatement.Item item = new MySqlRenameTableStatement.Item();
                            item.setName((SQLName) stmt.getTableSource().getExpr());
                            item.setTo(this.exprParser.name());
                            renameStmt.addItem(item);

                            if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();

                                SQLAlterTableRename alterItem = new SQLAlterTableRename();
                                alterItem.setTo(item.getTo());
                                stmt.addItem(alterItem);
                                continue;
                            } else {
                                // SQLAlterTableRename
                                return renameStmt;
                            }

                        }
                    } else if (lexer.token() == Token.ORDER) {
                        throw new ParserException("TODO " + lexer.info());
                    } else if (lexer.identifierEquals("CONVERT")) {
                        lexer.nextToken();
                        accept(Token.TO);
                        acceptIdentifier("CHARACTER");
                        accept(Token.SET);

                        SQLAlterTableConvertCharSet item = new SQLAlterTableConvertCharSet();
                        SQLExpr charset = this.exprParser.primary();
                        item.setCharset(charset);

                        if (lexer.identifierEquals("COLLATE")) {
                            lexer.nextToken();
                            SQLExpr collate = this.exprParser.primary();
                            item.setCollate(collate);
                        }

                        stmt.addItem(item);
                    } else if (lexer.identifierEquals(FnvHash.Constants.DISCARD)) {
                        lexer.nextToken();

                        if (lexer.token() == Token.PARTITION) {
                            lexer.nextToken();
                            SQLAlterTableDiscardPartition item = new SQLAlterTableDiscardPartition();

                            if (lexer.token() == Token.ALL) {
                                lexer.nextToken();
                                item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                            } else {
                                this.exprParser.names(item.getPartitions(), item);
                            }

                            if (lexer.token() == Token.TABLESPACE) {
                                lexer.nextToken();
                                item.setTablespace(true);
                            }

                            stmt.addItem(item);
                        } else {
                            accept(Token.TABLESPACE);
                            MySqlAlterTableDiscardTablespace item = new MySqlAlterTableDiscardTablespace();
                            stmt.addItem(item);
                        }


                    } else if (lexer.identifierEquals("IMPORT")) {
                        lexer.nextToken();

                        if (lexer.token() == Token.PARTITION) {
                            lexer.nextToken();
                            SQLAlterTableImportPartition item = new SQLAlterTableImportPartition();

                            if (lexer.token() == Token.ALL) {
                                lexer.nextToken();
                                item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                            } else {
                                this.exprParser.names(item.getPartitions(), item);
                            }

                            stmt.addItem(item);
                        } else {
                            accept(Token.TABLESPACE);
                            MySqlAlterTableImportTablespace item = new MySqlAlterTableImportTablespace();
                            stmt.addItem(item);
                        }
                    } else if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
                        throw new ParserException("TODO " + lexer.info());
                    } else if (lexer.identifierEquals("COALESCE")) {
                        lexer.nextToken();
                        accept(Token.PARTITION);

                        SQLAlterTableCoalescePartition item = new SQLAlterTableCoalescePartition();
                        SQLIntegerExpr countExpr = this.exprParser.integerExpr();
                        item.setCount(countExpr);
                        stmt.addItem(item);
                    } else if (lexer.identifierEquals("REORGANIZE")) {
                        lexer.nextToken();
                        accept(Token.PARTITION);

                        SQLAlterTableReOrganizePartition item = new SQLAlterTableReOrganizePartition();

                        this.exprParser.names(item.getNames(), item);

                        accept(Token.INTO);
                        accept(Token.LPAREN);
                        for (; ; ) {
                            SQLPartition partition = this.getExprParser().parsePartition();

                            item.addPartition(partition);

                            if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            } else {
                                break;
                            }
                        }
                        accept(Token.RPAREN);
                        stmt.addItem(item);
                    } else if (lexer.identifierEquals(FnvHash.Constants.EXCHANGE)) {
                        lexer.nextToken();
                        accept(Token.PARTITION);

                        SQLAlterTableExchangePartition item = new SQLAlterTableExchangePartition();

                        SQLName partition = this.exprParser.name();
                        item.addPartition(partition);

                        accept(Token.WITH);
                        accept(Token.TABLE);
                        SQLName table = this.exprParser.name();
                        item.setTable(table);

                        if (lexer.token() == Token.WITH) {
                            lexer.nextToken();
                            acceptIdentifier("VALIDATION");
                            item.setValidation(true);
                        } else if (lexer.identifierEquals(FnvHash.Constants.WITHOUT)) {
                            lexer.nextToken();
                            acceptIdentifier("VALIDATION");
                            item.setValidation(false);
                        }


                        stmt.addItem(item);
                    } else if (lexer.identifierEquals("REBUILD")) {
                        lexer.nextToken();

                        accept(Token.PARTITION);

                        SQLAlterTableRebuildPartition item = new SQLAlterTableRebuildPartition();

                        if (lexer.token() == Token.ALL) {
                            lexer.nextToken();
                            item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                        } else {
                            this.exprParser.names(item.getPartitions(), item);
                        }

                        stmt.addItem(item);
                    } else if (lexer.identifierEquals("REPAIR")) {
                        lexer.nextToken();

                        accept(Token.PARTITION);

                        SQLAlterTableRepairPartition item = new SQLAlterTableRepairPartition();

                        if (lexer.token() == Token.ALL) {
                            lexer.nextToken();
                            item.getPartitions().add(new SQLIdentifierExpr("ALL"));
                        } else {
                            this.exprParser.names(item.getPartitions(), item);
                        }

                        stmt.addItem(item);
                    } else if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
                        SQLAlterTablePartitionCount item = new SQLAlterTablePartitionCount();
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        item.setCount((SQLIntegerExpr) exprParser.integerExpr());
                        stmt.addItem(item);
                    } else if (lexer.token() == Token.PARTITION) {
                        lexer.nextToken();
                        if(lexer.identifierEquals(FnvHash.Constants.LIFECYCLE)) {
                            lexer.nextToken();
                            SQLAlterTablePartitionLifecycle item = new SQLAlterTablePartitionLifecycle();
                            if (lexer.token() == Token.EQ) {
                                lexer.nextToken();
                            }
                            item.setLifecycle((SQLIntegerExpr) exprParser.integerExpr());
                            stmt.addItem(item);
                        }
                    } else if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION)) {
                        lexer.nextToken();
                        if (lexer.identifierEquals(FnvHash.Constants.LIFECYCLE)) {
                            lexer.nextToken();
                            SQLAlterTableSubpartitionLifecycle item = new SQLAlterTableSubpartitionLifecycle();
                            if (lexer.token() == Token.LITERAL_INT) {
                                for (; ; ) {
                                    item.getPartitionIds().add(this.exprParser.integerExpr());
                                    String pidStr = lexer.stringVal();
                                    accept(Token.VARIANT);
                                    String s = pidStr.replaceAll(":", "");
                                    if (StringUtils.isEmpty(s)) {
                                        item.getSubpartitionLifeCycle().add(exprParser.integerExpr());
                                    } else {
                                        item.getSubpartitionLifeCycle().add(new SQLIntegerExpr(Integer.valueOf(s)));
                                    }

                                    if (lexer.token() == Token.COMMA) {
                                        lexer.nextToken();
                                        continue;
                                    }

                                    break;
                                }
                            }
                            stmt.addItem(item);
                        }
                    } else if (lexer.identifierEquals("BLOCK_SIZE")) {
                        SQLAlterTableBlockSize item = new SQLAlterTableBlockSize();
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            accept(Token.EQ);
                        }
                        item.setSize((SQLIntegerExpr) exprParser.expr());
                        stmt.addItem(item);
                    } else if (lexer.identifierEquals("COMPRESSION")) {
                        SQLAlterTableCompression item = new SQLAlterTableCompression();
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        item.setName(exprParser.charExpr());
                        stmt.addItem(item);
                        continue for_;
                    } else if (lexer.identifierEquals("REMOVE")) {
                        lexer.nextToken();
                        acceptIdentifier("PARTITIONING");
                        stmt.setRemovePatiting(true);
                    } else if (lexer.identifierEquals("UPGRADE")) {
                        lexer.nextToken();
                        acceptIdentifier("PARTITIONING");
                        stmt.setUpgradePatiting(true);
                    } else if (lexer.identifierEquals("ALGORITHM")) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.addItem(new MySqlAlterTableOption("ALGORITHM", lexer.stringVal()));
                        lexer.nextToken();
                        continue for_;
                    } else if (lexer.identifierEquals(ENGINE)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(ENGINE, this.exprParser.primary());
                        continue for_;
                    } else if (lexer.identifierEquals(AUTO_INCREMENT)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(AUTO_INCREMENT, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(AVG_ROW_LENGTH)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(AVG_ROW_LENGTH, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.CHECKSUM)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(CHECKSUM2, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.CONNECTION)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(CONNECTION, this.exprParser.charExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.DATA) ||
                            lexer.token() == Token.INDEX) {
                        boolean isIndex = lexer.token() == Token.INDEX;
                        lexer.nextToken();
                        acceptIdentifier("DIRECTORY");
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(
                                isIndex ? "INDEX DIRECTORY" : "DATA DIRECTORY",
                                this.exprParser.primary());
                        continue for_;
                    } else if (lexer.identifierEquals(DELAY_KEY_WRITE)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(DELAY_KEY_WRITE, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.ENCRYPTION)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(ENCRYPTION2, this.exprParser.primary());
                        continue for_;
                    } else if (lexer.identifierEquals(INSERT_METHOD)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(INSERT_METHOD, this.exprParser.primary());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.KEY_BLOCK_SIZE)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(KEY_BLOCK_SIZE2, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.MAX_ROWS)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(MAX_ROWS2, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.MIN_ROWS)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(MIN_ROWS2, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.PASSWORD)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(PASSWORD2, this.exprParser.primary());
                        continue for_;
                    } else if (lexer.identifierEquals(STATS_AUTO_RECALC)) {
                        lexer.nextToken();
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(STATS_AUTO_RECALC, this.exprParser.primary());
                        continue for_;
                    } else if (lexer.identifierEquals(STATS_PERSISTENT)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(STATS_PERSISTENT, this.exprParser.primary());
                        continue for_;
                    } else if (lexer.identifierEquals(STATS_SAMPLE_PAGES)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        stmt.getTableOptions().put(STATS_SAMPLE_PAGES, this.exprParser.integerExpr());
                        continue for_;
                    } else if (lexer.token() == Token.TABLESPACE) {
                        lexer.nextToken();
                        MySqlCreateTableStatement.TableSpaceOption option = new MySqlCreateTableStatement.TableSpaceOption();
                        option.setName(this.exprParser.name());
                        if (lexer.identifierEquals("STORAGE")) {
                            lexer.nextToken();
                            option.setStorage(this.exprParser.name());
                        }
                        stmt.addItem(new MySqlAlterTableOption(TABLESPACE2, option));
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.COLLATE)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        SQLAlterCharacter item = new SQLAlterCharacter();
                        item.setCollate(this.exprParser.primary());
                        stmt.addItem(item);
                        continue for_;
                    } else if (lexer.identifierEquals("PACK_KEYS")) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        if (lexer.identifierEquals("PACK")) {
                            lexer.nextToken();
                            accept(Token.ALL);
                            stmt.getTableOptions().put("PACK_KEYS", new SQLIdentifierExpr("PACK ALL"));
                        } else {
                            stmt.getTableOptions().put("PACK_KEYS", this.exprParser.primary());
                        }
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
                        SQLAlterCharacter item = alterTableCharacter();
                        stmt.addItem(item);
                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.CLUSTERED)) {
                        SQLAlterTableModifyClusteredBy clusteredBy = new SQLAlterTableModifyClusteredBy();

                        acceptIdentifier("CLUSTERED");
                        accept(Token.BY);
                        accept(Token.LPAREN);

                        // for ads: ALTER TABLE SCHEMA1.TABLE1 CLUSTERED BY ();
                        if (lexer.token() != Token.RPAREN) {
                            for (; ; ) {
                                clusteredBy.addClusterColumn(this.exprParser.name());

                                if (lexer.token() == Token.COMMA) {
                                    accept(Token.COMMA);
                                    continue;
                                }
                                break;
                            }
                        }

                        accept(Token.RPAREN);

                        stmt.addItem(clusteredBy);
                    } else if (lexer.identifierEquals("ROW_FORMAT")) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }

                        if (lexer.token() == Token.DEFAULT || lexer.token() == Token.IDENTIFIER) {
                            SQLIdentifierExpr rowFormat = new SQLIdentifierExpr(lexer.stringVal());
                            lexer.nextToken();
                            stmt.getTableOptions().put("ROW_FORMAT", rowFormat);
                        } else {
                            throw new ParserException("illegal syntax. " + lexer.info());
                        }

                        continue for_;
                    } else if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION_AVAILABLE_PARTITION_NUM)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        SQLIntegerExpr num = this.exprParser.integerExpr();
                        SQLAlterTableSubpartitionAvailablePartitionNum item = new SQLAlterTableSubpartitionAvailablePartitionNum();
                        item.setNumber(num);
                        stmt.addItem(item);
                    } else if (lexer.token() == Token.LOCK) {
                        lexer.nextToken();
                        if (lexer.token() == Token.EQ) {
                            lexer.nextToken();
                        }
                        SQLIdentifierExpr rowFormat = new SQLIdentifierExpr(lexer.stringVal());
                        lexer.nextToken();
                        stmt.getTableOptions().put("LOCK", rowFormat);
                    } else {
                        break for_;
                    }
                    break;
                }
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            } else {
                break;
            }

        }

        return stmt;
    }
    */

    private MySqlExtPartition parseExtPartition() {
        MySqlExtPartition partitionDef = new MySqlExtPartition();
        for (; ; ) {
            MySqlExtPartition.Item item = new MySqlExtPartition.Item();

            if (lexer.identifierEquals(FnvHash.Constants.DBPARTITION)) {
                lexer.nextToken();
                SQLName name = this.exprParser.name();
                item.setDbPartition(name);
                accept(Token.BY);
                SQLExpr value = this.exprParser.primary();
                item.setDbPartitionBy(value);
            }

            if (lexer.identifierEquals(FnvHash.Constants.TBPARTITION)) {
                lexer.nextToken();
                SQLName name = this.exprParser.name();
                item.setTbPartition(name);
                accept(Token.BY);
                SQLExpr value = this.exprParser.primary();
                item.setTbPartitionBy(value);
            }

            item.setParent(partitionDef);
            partitionDef.getItems().add(item);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            } else {
                break;
            }
        }

        return partitionDef;
    }

    private SQLAlterCharacter alterTableCharacter() {
        lexer.nextToken();
        accept(Token.SET);
        if (lexer.token() == Token.EQ) {
            lexer.nextToken();
        }
        SQLAlterCharacter item = new SQLAlterCharacter();
        item.setCharacterSet(this.exprParser.primary());
        if (lexer.token() == Token.COMMA) {
            lexer.nextToken();
            if (lexer.identifierEquals(FnvHash.Constants.COLLATE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                item.setCollate(this.exprParser.primary());
            }
        }
        return item;
    }

    protected void parseAlterTableAddColumn(SQLAlterTableStatement stmt) {
        boolean parenFlag = false;
        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            parenFlag = true;
        }

        SQLAlterTableAddColumn item = new SQLAlterTableAddColumn();
        for (; ; ) {

            SQLColumnDefinition columnDef = this.exprParser.parseColumn();
            item.addColumn(columnDef);
            if (lexer.identifierEquals("AFTER")) {
                lexer.nextToken();
                item.setAfterColumn(this.exprParser.name());
            } else if (lexer.identifierEquals("FIRST")) {
                lexer.nextToken();
                if (lexer.token() == Token.IDENTIFIER) {
                    item.setFirstColumn(this.exprParser.name());
                } else {
                    item.setFirst(true);
                }
            }

            if (parenFlag && lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        if (parenFlag) {
            accept(Token.RPAREN);
        }

        if (lexer.identifierEquals(FnvHash.Constants.RESTRICT)) {
            lexer.nextToken();
            item.setRestrict(true);
        } else if (lexer.token() == Token.CASCADE || lexer.identifierEquals(FnvHash.Constants.CASCADE)) {
            lexer.nextToken();
            item.setCascade(true);
        } else {
            item.setCascade(false);
        }

        stmt.addItem(item);
    }

    public void parseAlterDrop(SQLAlterTableStatement stmt) {
        lexer.nextToken();
        if (lexer.token() == Token.INDEX) {
            lexer.nextToken();
            SQLName indexName = this.exprParser.name();
            SQLAlterTableDropIndex item = new SQLAlterTableDropIndex();
            item.setIndexName(indexName);
            stmt.addItem(item);
        } else if (lexer.token() == Token.CHECK) {
            lexer.nextToken();
            SQLAlterTableDropCheck item = new SQLAlterTableDropCheck();
            item.setCheckName(this.exprParser.name());
            stmt.addItem(item);
        } else if (lexer.token() == Token.FOREIGN) {
            lexer.nextToken();
            accept(Token.KEY);
            SQLName indexName = this.exprParser.name();
            SQLAlterTableDropForeignKey item = new SQLAlterTableDropForeignKey();
            item.setIndexName(indexName);
            stmt.addItem(item);
        } else if (lexer.token() == Token.KEY) {
            lexer.nextToken();
            SQLName keyName = this.exprParser.name();
            SQLAlterTableDropKey item = new SQLAlterTableDropKey();
            item.setKeyName(keyName);
            stmt.addItem(item);
        } else if (lexer.token() == Token.PRIMARY) {
            lexer.nextToken();
            accept(Token.KEY);
            SQLAlterTableDropPrimaryKey item = new SQLAlterTableDropPrimaryKey();
            stmt.addItem(item);
        } else if (lexer.token() == Token.CONSTRAINT) {
            lexer.nextToken();
            SQLAlterTableDropConstraint item = new SQLAlterTableDropConstraint();
            item.setConstraintName(this.exprParser.name());
            stmt.addItem(item);
        } else if (lexer.token() == Token.COLUMN) {
            lexer.nextToken();
            SQLAlterTableDropColumnItem item = new SQLAlterTableDropColumnItem();

            SQLName name = exprParser.name();
            name.setParent(item);
            item.addColumn(name);

            if (dbType != DbType.mysql) {
                while (lexer.token() == Token.COMMA) {
                    char markChar = lexer.current();
                    int markBp = lexer.bp();

                    lexer.nextToken();
                    if (lexer.identifierEquals(FnvHash.Constants.CHANGE)
                        || lexer.identifierEquals(FnvHash.Constants.MODIFY)) {
                        lexer.reset(markBp, markChar, Token.COMMA);
                        break;
                    }

                    if (lexer.token() == Token.IDENTIFIER) {
                        if ("ADD".equalsIgnoreCase(lexer.stringVal())) {
                            lexer.reset(markBp, markChar, Token.COMMA);
                            break;
                        }
                        name = exprParser.name();
                        name.setParent(item);
                        item.addColumn(name);
                    } else {
                        lexer.reset(markBp, markChar, Token.COMMA);
                        break;
                    }
                }
            }

            stmt.addItem(item);
        } else if (lexer.token() == Token.PARTITION) {
            SQLAlterTableDropPartition dropPartition = parseAlterTableDropPartition(false);
            stmt.addItem(dropPartition);
        } else if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION)) {
            SQLAlterTableDropSubpartition dropPartition = parseAlterTableDropSubpartition();
            stmt.addItem(dropPartition);
        } else if (lexer.identifierEquals(FnvHash.Constants.CLUSTERING) || lexer
            .identifierEquals(FnvHash.Constants.CLUSTERED)) {
            lexer.nextToken();
            SQLAlterTableDropClusteringKey dropPartition = new SQLAlterTableDropClusteringKey();
            accept(Token.KEY);
            dropPartition.setKeyName(exprParser.name());
            stmt.addItem(dropPartition);
        } else if (lexer.token() == Token.IDENTIFIER) {
            if (lexer.identifierEquals(FnvHash.Constants.EXTPARTITION)) {
                lexer.nextToken();
                accept(Token.LPAREN);
                SQLAlterTableDropExtPartition extPartitionItem = new SQLAlterTableDropExtPartition();
                MySqlExtPartition partitionDef = parseExtPartition();
                extPartitionItem.setExPartition(partitionDef);
                stmt.addItem(extPartitionItem);
                accept(Token.RPAREN);
            } else if (lexer.identifierEquals(FnvHash.Constants.FILE)) {
                lexer.nextToken();
                SQLAlterTableDropFile item = new SQLAlterTableDropFile();
                for (; ; ) {
                    SQLExpr file = this.exprParser.expr();
                    item.addFile(file);
                    if (lexer.token() == COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                stmt.addItem(item);
            } else {
                SQLAlterTableDropColumnItem item = new SQLAlterTableDropColumnItem();

                SQLName name = this.exprParser.name();
                item.addColumn(name);
                stmt.addItem(item);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                }

                if (lexer.token() == Token.DROP) {
                    parseAlterDrop(stmt);
                }
            }
        } else {
            super.parseAlterDrop(stmt);
        }
    }

    public SQLStatement parseRename() {

        acceptIdentifier("RENAME");

        if (lexer.token() == Token.SEQUENCE) {
            lexer.nextToken();
            MySqlRenameSequenceStatement stmt = new MySqlRenameSequenceStatement();

            SQLName name = this.exprParser.name();
            stmt.setName(name);

            accept(Token.TO);

            SQLName to = this.exprParser.name();
            stmt.setTo(to);

            return stmt;
        }
        if (lexer.token() == Token.USER) {
            lexer.nextToken();
            SQLRenameUserStatement stmt = new SQLRenameUserStatement();

            SQLName name = this.exprParser.name();
            stmt.setName(name);

            accept(Token.TO);

            SQLName to = this.exprParser.name();
            stmt.setTo(to);

            return stmt;
        }

        accept(Token.TABLE);
        MySqlRenameTableStatement stmt = new MySqlRenameTableStatement();

        for (; ; ) {
            MySqlRenameTableStatement.Item item = new MySqlRenameTableStatement.Item();
            item.setName(this.exprParser.name());
            accept(Token.TO);
            item.setTo(this.exprParser.name());

            stmt.addItem(item);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        return stmt;
    }

    /**
     * CREATE CCL_RULE  ON database_name.table_name TO ‘username’@’host’
     * FOR { UPDATE | SELECT | INSERT | DELETE }
     * [filter_options]
     * with_options
     * <p>
     * user:
     * ‘username’ @ ’host’
     * filter_options:
     * [ FILTER  BY KEYWORD(‘KEYWORD1’, ’KEYWORD2’,…) ]
     * [ FILTER  BY TEMPLATE(‘template_id’) ]
     * <p>
     * with_options:
     * WITH MAX_CONCURRENCY = value1 [, WAIT_QUEUE_SIZE = value2 ]
     *
     * @return SQLCreateCclRuleSQLStatement
     */
    public SQLStatement parseCreateCclRule() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }
        acceptIdentifier("CCL_RULE");
        DrdsCreateCclRuleStatement stmt = new DrdsCreateCclRuleStatement();

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            stmt.setIfNotExists(true);
        }

        SQLName name = exprParser.name();
        stmt.setName(name);

        //on
        accept(ON);
        SQLExpr expr = exprParser.expr();
        if (expr instanceof SQLPropertyExpr) {
            stmt.setTable(new SQLExprTableSource(expr));
        } else {
            setErrorEndPos(lexer.pos());
            throw new ParserException(
                "syntax error, no table, " + lexer
                    .info());
        }

        if (lexer.token() == Token.TO) {
            //to
            lexer.nextToken();
            expr = exprParser.primary();
            if (expr instanceof MySqlUserName) {
                stmt.setUser((MySqlUserName) expr);
            } else {
                setErrorEndPos(lexer.pos());
                throw new ParserException("syntax error, no user, example: 'username'@'host', " + lexer.info());
            }
        }

        //for
        accept(FOR);
        switch (lexer.token()) {
        case INSERT:
        case SELECT:
        case UPDATE:
        case DELETE:
            stmt.setFOR(new SQLIdentifierExpr(lexer.token().name));
            break;
        default:
            setErrorEndPos(lexer.pos());
            throw new ParserException(
                "syntax error, expect " + "INSERT|SELECT|UPDATE|DELETE" + ", actual " + lexer.token() + ", " + lexer
                    .info());
        }
        lexer.nextToken();

        //filter options
        while (lexer.identifierEquals("FILTER")) {
            lexer.nextToken();
            accept(Token.BY);
            if (lexer.identifierEquals("KEYWORD")) {
                lexer.nextToken();
                SQLExpr keywords = this.exprParser.primary();
                if (keywords instanceof SQLCharExpr) {
                    keywords = new SQLListExpr(keywords);
                }
                if (!(keywords instanceof SQLListExpr)) {
                    setErrorEndPos(lexer.pos());
                    throw new ParserException(
                        "syntax error, " + "Failed to parse values of  FILTER BY KEYWORD" + ", " + lexer.info());
                }
                stmt.setKeywords((SQLListExpr) keywords);
            } else if (lexer.identifierEquals("TEMPLATE")) {
                lexer.nextToken();
                SQLExpr template = this.exprParser.primary();
                if (template instanceof SQLCharExpr) {
                    template = new SQLListExpr(template);
                }
                if (!(template instanceof SQLListExpr)) {
                    setErrorEndPos(lexer.pos());
                    throw new ParserException(
                        "syntax error, Failed to parse values of FILTER BY TEMPLATE, " + lexer.info());
                }
                stmt.setTemplate((SQLListExpr) template);
            } else if (lexer.identifierEquals("QUERY")) {
                lexer.nextToken();
                SQLExpr query = this.exprParser.primary();
                if (!(query instanceof SQLCharExpr)) {
                    setErrorEndPos(lexer.pos());
                    throw new ParserException(
                        "syntax error, Failed to parse values of FILTER BY QUERY, " + lexer.info());
                }
                stmt.setQuery((SQLCharExpr) query);
            }
        }

        //with options
        accept(Token.WITH);
        List<SQLAssignItem> assignItems = new ArrayList<SQLAssignItem>();
        this.exprParser.parseAssignItemWithoutParen(assignItems, stmt);
        stmt.setWith(assignItems);

        return stmt;
    }

    public SQLStatement parseCreateCclTrigger() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        DrdsCreateCclTriggerStatement stmt = new DrdsCreateCclTriggerStatement();

        acceptIdentifier("CCL_TRIGGER");

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            stmt.setIfNotExists(true);
        }

        SQLName name = exprParser.name();
        stmt.setName(name);

        accept(ON);
        SQLName schema = exprParser.name();
        stmt.setSchema(schema);

        accept(Token.WHEN);
        List<SQLBinaryOpExpr> whens = Lists.newArrayList();
        do {
            SQLExpr expr = exprParser.expr();
            if (!(expr instanceof SQLBinaryOpExpr)) {
                setErrorEndPos(lexer.pos());
                throw new ParserException(
                    "syntax error, Failed to parse binary trigger condition " + lexer.info());
            }
            whens.add((SQLBinaryOpExpr) expr);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            } else {
                break;
            }
        } while (true);
        stmt.setWhens(whens);

        accept(Token.LIMIT);
        List<SQLAssignItem> limitAssignItems = new ArrayList<SQLAssignItem>();
        this.exprParser.parseAssignItemWithoutParen(limitAssignItems, stmt);
        stmt.setLimitAssignItems(limitAssignItems);

        accept(Token.CREATE);
        acceptIdentifier("CCL_RULE");
        accept(Token.WITH);
        List<SQLAssignItem> withAssignItems = new ArrayList<SQLAssignItem>();
        this.exprParser.parseAssignItemWithoutParen(withAssignItems, stmt);

        stmt.setWithAssignItems(withAssignItems);

        return stmt;
    }

    public SQLStatement parseCreateFileStorage() {
        accept(Token.CREATE);
        acceptIdentifier("FileStorage");

        CreateFileStorageStatement stmt = new CreateFileStorageStatement();

        stmt.setEngineName(this.exprParser.name());
        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            accept(Token.LPAREN);

            for (; ; ) {
                SQLName key = this.exprParser.name();
                accept(Token.EQ);
                SQLName value = this.exprParser.name();
                stmt.getWithValue().add(new SQLAssignItem(key, value));
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            accept(Token.RPAREN);
        }
        return stmt;
    }

    public SQLStatement parseCreateSchedule() {
        DrdsCreateScheduleStatement stmt = new DrdsCreateScheduleStatement();
        accept(Token.CREATE);
        acceptIdentifier("SCHEDULE");
        accept(FOR);

        if (identifierEquals("LOCAL_PARTITION")) {
            acceptIdentifier("LOCAL_PARTITION");
            stmt.setForLocalPartition(true);
        } else if (identifierEquals("AUTO_SPLIT_TABLE_GROUP")) {
            acceptIdentifier("AUTO_SPLIT_TABLE_GROUP");
            stmt.setForAutoSplitTableGroup(true);
        } else {
            throw new ParserException("unknown schedule type");
        }

        accept(ON);
        stmt.setIfNotExists(false);
        stmt.setName(this.exprParser.name());

        if (identifierEquals("PARAMS")) {
            acceptIdentifier("PARAMS");
            stmt.setParamsExpr(this.exprParser.expr());
        }

        acceptIdentifier("CRON");
        stmt.setCronExpr(this.exprParser.expr());

        if (identifierEquals("TIMEZONE")) {
            acceptIdentifier("TIMEZONE");
            stmt.setTimeZone(this.exprParser.expr());
        }

        return stmt;
    }

    public SQLStatement parseCreateDatabase() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.SCHEMA) {
            lexer.nextToken();
        } else {
            accept(Token.DATABASE);
        }

        SQLCreateDatabaseStatement stmt = new SQLCreateDatabaseStatement(dbType);

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            stmt.setIfNotExists(true);
        }

        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.DEFAULT) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.HINT) {
            stmt.setHints(this.exprParser.parseHints());
        }

        if (lexer.token() == Token.DEFAULT) {
            lexer.nextToken();
        }

        for (; ; ) {
            if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
                lexer.nextToken();
                accept(Token.SET);
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                String charset = lexer.stringVal();
                if (lexer.token() == Token.IDENTIFIER || lexer.token() == LITERAL_CHARS || lexer.token() == BINARY) {
                    lexer.nextToken();
                } else {
                    setErrorEndPos(lexer.pos());
                    printError(lexer.token());
                }
                stmt.setCharacterSet(charset);
            } else if (lexer.identifierEquals(FnvHash.Constants.CHARSET)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                String charset = lexer.stringVal();
                if (lexer.token() == IDENTIFIER || lexer.token() == LITERAL_CHARS || lexer.token() == BINARY) {
                    lexer.nextToken();
                } else {
                    setErrorEndPos(lexer.pos());
                    printError(lexer.token());
                }
                stmt.setCharacterSet(charset);
            } else if (lexer.token() == Token.DEFAULT) {
                lexer.nextToken();
            } else if (lexer.identifierEquals(FnvHash.Constants.COLLATE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                String collate = lexer.stringVal();
                if (lexer.token() == Token.IDENTIFIER || lexer.token() == LITERAL_CHARS || lexer.token() == BINARY) {
                    lexer.nextToken();
                } else {
                    setErrorEndPos(lexer.pos());
                    printError(lexer.token());
                }
                stmt.setCollate(collate);
            } else if (lexer.identifierEquals(FnvHash.Constants.PASSWORD)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr password = this.exprParser.primary();
                stmt.setPassword(password);
            } else if (lexer.identifierEquals(FnvHash.Constants.LOCALITY)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.setLocality(this.exprParser.expr());
            } else if (lexer.identifierEquals(FnvHash.Constants.PARTITION_MODE) || lexer
                .identifierEquals(FnvHash.Constants.MODE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.setPartitionMode(this.exprParser.expr());
            }

            //ads 特殊支持
            else if (lexer.identifierEquals("SHARDS") || lexer.identifierEquals("SHARD_ID")
                || lexer.identifierEquals("REPLICATION") || lexer.identifierEquals("STORAGE_DEPENDENCY")
                || lexer.identifierEquals("REPLICA_TYPE") || lexer.identifierEquals("DATA_REPLICATION")) {
                String key = lexer.stringVal();
                lexer.nextToken();
                accept(Token.EQ);
                SQLExpr value = this.exprParser.expr();
                stmt.getOptions().put(key, value);
            } else {
                break;
            }
        }

        if (lexer.token() == FOR) {
            lexer.nextToken();
            String user = lexer.stringVal();
            lexer.nextToken();
            stmt.setUser(user);
        }

        if (lexer.identifierEquals(FnvHash.Constants.OPTIONS)) {
            lexer.nextToken();
            accept(Token.LPAREN);
            for (; ; ) {
                if (lexer.token() == Token.RPAREN) {
                    accept(Token.RPAREN);
                    break;
                }
                String key = lexer.stringVal();
                lexer.nextToken();
                accept(Token.EQ);
                SQLExpr value = this.exprParser.expr();
                stmt.getOptions().put(key, value);
            }
        }

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();

            accept(Token.LPAREN);
            for (; ; ) {
                SQLAssignItem assignItem = this.exprParser.parseAssignItem();
                assignItem.setParent(stmt);
                stmt.getDbProperties().add(assignItem);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }
            accept(Token.RPAREN);
        }

        if (lexer.identifierEquals(FnvHash.Constants.STORED)) {
            lexer.nextToken();

            if (lexer.token() == Token.BY) {
                accept(Token.BY);

                for (; ; ) {
                    List<SQLAssignItem> storedByItem = new ArrayList<SQLAssignItem>();
                    accept(Token.LPAREN);
                    for (; ; ) {
                        SQLAssignItem assignItem = this.exprParser.parseAssignItem();
                        assignItem.setParent(stmt);
                        storedByItem.add(assignItem);

                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.RPAREN);
                    stmt.getStoredBy().add(storedByItem);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }

                    break;
                }
            } else if (lexer.token() == Token.IN) {
                lexer.nextToken();

                stmt.setStoredIn(this.exprParser.name());

                accept(ON);
                accept(Token.LPAREN);
                for (; ; ) {
                    SQLAssignItem assignItem = this.exprParser.parseAssignItem();
                    assignItem.setParent(stmt);
                    stmt.getStoredOn().add(assignItem);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            } else if (lexer.token() == Token.AS) {
                lexer.nextToken();
                SQLExpr like = this.exprParser.expr();
                stmt.setStoredAs(like);
            } else {
                throw new ParserException("TODO " + lexer.info());
            }
        }

        return stmt;
    }

    protected void parseUpdateSet(SQLUpdateStatement update) {
        accept(Token.SET);

        for (; ; ) {
            SQLUpdateSetItem item = this.exprParser.parseUpdateSetItem();
            update.addItem(item);

            if (lexer.token() != Token.COMMA) {
                break;
            }

            lexer.nextToken();
        }
    }

    public SQLStatement parseAlterDatabase() {
        if (lexer.token() == Token.SCHEMA) {
            lexer.nextToken();
        } else {
            accept(Token.DATABASE);
        }

        SQLAlterDatabaseStatement stmt = new SQLAlterDatabaseStatement(dbType);

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        if (lexer.token() == Token.SET) {
            lexer.nextToken();
            MySqlAlterDatabaseSetOption option = new MySqlAlterDatabaseSetOption();
            for (; ; ) {
                SQLName key = this.exprParser.name();
                accept(Token.EQ);
                SQLExpr value = this.exprParser.expr();

                option.getOptions().add(new SQLAssignItem(key, value));

                if (lexer.token() == Token.EOF || lexer.token() == ON) {
                    break;
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                }
            }
            stmt.setItem(option);

            if (lexer.token() == ON) {
                lexer.nextToken();
                SQLName on = this.exprParser.name();
                option.setOn(on);
            }

            return stmt;
        }

        if (lexer.token() == Token.KILL) {
            MySqlAlterDatabaseKillJob item = new MySqlAlterDatabaseKillJob();
            lexer.nextToken();
            SQLName jobType = this.exprParser.name();
            SQLName jobId = this.exprParser.name();
            item.setJobType(jobType);
            item.setJobId(jobId);

            stmt.setItem(item);
        }

        if (lexer.identifierEquals("UPGRADE")) {
            lexer.nextToken();
            acceptIdentifier("DATA");
            acceptIdentifier("DIRECTORY");
            acceptIdentifier("NAME");
            stmt.setUpgradeDataDirectoryName(true);
        }

        if (lexer.token() == Token.DEFAULT) {
            lexer.nextToken();
            if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
                SQLAlterCharacter item = alterTableCharacter();
                stmt.setCharacter(item);
            } else {
                throw new ParserException("TODO " + lexer.info());
            }
        } else if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
            SQLAlterCharacter item = alterTableCharacter();
            stmt.setCharacter(item);
        }

        return stmt;
    }

    public MySqlAlterUserStatement parseAlterUser() {
        accept(Token.USER);

        MySqlAlterUserStatement stmt = new MySqlAlterUserStatement();

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.EXISTS);
            stmt.setIfExists(true);
        }

        for (; ; ) {
            MySqlAlterUserStatement.AlterUser alterUser = new MySqlAlterUserStatement.AlterUser();

            SQLExpr user = this.exprParser.expr();
            alterUser.setUser(user);

            if (lexer.identifierEquals("IDENTIFIED")) {
                lexer.nextToken();
                accept(Token.BY);

                MySqlAlterUserStatement.AuthOption authOption = new MySqlAlterUserStatement.AuthOption();
                SQLCharExpr authString = this.exprParser.charExpr();
                authOption.setAuthString(authString);

                alterUser.setAuthOption(authOption);
            }

            if (lexer.identifierEquals("PASSWORD")) {
                lexer.nextToken();
                if (lexer.identifierEquals("EXPIRE")) {
                    lexer.nextToken();

                    MySqlAlterUserStatement.PasswordOption passwordOption =
                        new MySqlAlterUserStatement.PasswordOption();

                    if (lexer.token() == Token.DEFAULT) {
                        lexer.nextToken();
                        passwordOption.setExpire(MySqlAlterUserStatement.PasswordExpire.PASSWORD_EXPIRE_DEFAULT);
                    } else if (lexer.identifierEquals("NEVER")) {
                        lexer.nextToken();
                        passwordOption.setExpire(MySqlAlterUserStatement.PasswordExpire.PASSWORD_EXPIRE_NEVER);
                    } else if (lexer.token() == Token.INTERVAL) {
                        lexer.nextToken();
                        passwordOption.setExpire(MySqlAlterUserStatement.PasswordExpire.PASSWORD_EXPIRE_INTERVAL);
                        SQLIntegerExpr days = this.exprParser.integerExpr();
                        passwordOption.setIntervalDays(days);
                        acceptIdentifier("DAY");
                    } else {
                        passwordOption.setExpire(MySqlAlterUserStatement.PasswordExpire.PASSWORD_EXPIRE);
                    }
                    stmt.setPasswordOption(passwordOption);
                }
            }

            stmt.getAlterUsers().add(alterUser);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }
        return stmt;
    }

    @Override
    public MySqlExprParser getExprParser() {
        return (MySqlExprParser) exprParser;
    }

    public SQLCreateFunctionStatement parseCreateFunction() {
        SQLCreateFunctionStatement stmt = new SQLCreateFunctionStatement();
        stmt.setDbType(dbType);

        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();

            if (lexer.token() == Token.OR) {
                lexer.nextToken();
                accept(Token.REPLACE);
                stmt.setOrReplace(true);
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
            lexer.nextToken();
            accept(Token.EQ);
            SQLName definer = this.getExprParser().userName();
            stmt.setDefiner(definer);
        }

        accept(Token.FUNCTION);

        stmt.setName(SQLUtils.rewriteUdfName(this.exprParser.name()));

        if (lexer.token() == Token.LPAREN) {// match "("
            lexer.nextToken();
            parserParameters(stmt.getParameters(), stmt);
            accept(Token.RPAREN);// match ")"
        }

        acceptIdentifier("RETURNS");
        SQLDataType dataType = this.exprParser.parseDataType();

        // ignore character set within enum type
        if ("ENUM".equalsIgnoreCase(dataType.getName())) {
            // ignore character set xxx
            if (lexer.identifierEquals("CHARACTER")) {
                lexer.nextToken();
                if (lexer.identifierEquals("SET") || lexer.token() == Token.SET) {
                    lexer.nextToken();
                    if (lexer.token() != Token.EOF) {
                        lexer.nextToken();
                    } else {
                        throw new RuntimeException("charset expected, but not found");
                    }
                }
            }
            // ignore charset xxx
            if (lexer.identifierEquals("CHARSET")) {
                // skip charset keyword
                lexer.nextToken();
                // skip defined charset
                if (lexer.token() != Token.EOF) {
                    lexer.nextToken();
                } else {
                    throw new RuntimeException("charset expected, but not found");
                }
            }
            // ignore collate xxx
            if (lexer.identifierEquals("COLLATE")) {
                // skip collate keyword
                lexer.nextToken();
                // skip defined collation
                if (lexer.token() != Token.EOF) {
                    lexer.nextToken();
                } else {
                    throw new RuntimeException("collation expected, but not found");
                }
            }
        }

        stmt.setReturnDataType(dataType);

        for (; ; ) {
            if (lexer.token() == Token.NOT) {
                lexer.nextToken();
                acceptIdentifier("DETERMINISTIC");
                stmt.setDeterministic(false);
                continue;
            }
            if (lexer.identifierEquals("DETERMINISTIC")) {
                lexer.nextToken();
                stmt.setDeterministic(true);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.CONTAINS) || lexer.token() == Token.CONTAINS) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setSqlDataAccess(SqlDataAccess.CONTAINS_SQL);
                continue;
            }

            if (lexer.identifierEquals("NO")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setSqlDataAccess(SqlDataAccess.NO_SQL);
                continue;
            }

            if (lexer.identifierEquals("READS")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                stmt.setSqlDataAccess(SqlDataAccess.READS_SQL_DATA);
                continue;
            }

            if (lexer.identifierEquals("MODIFIES")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                stmt.setSqlDataAccess(SqlDataAccess.MODIFIES_SQL_DATA);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
                lexer.nextToken();
                acceptIdentifier("SECURITY");
                if (lexer.identifierEquals("DEFINER")) {
                    stmt.setSqlSecurity(SqlSecurity.DEFINER);
                    lexer.nextToken();
                } else if (lexer.identifierEquals("INVOKER")) {
                    stmt.setSqlSecurity(SqlSecurity.INVOKER);
                    lexer.nextToken();
                } else {
                    throw new RuntimeException("sql security can only be set to definer or invoker");
                }
                continue;
            }

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                stmt.setComment(exprParser.charExpr());
                continue;
            }

            if (lexer.identifierEquals("LANGUAGE")) {
                lexer.nextToken();
                stmt.setLanguage(lexer.stringVal());
                lexer.nextToken();
                continue;
            }

            break;
        }

        if (lexer.token() == Token.EOF) {
            throw new RuntimeException("function content should not empty");
        }

        SQLStatement block = this.parseImplicitBlock();

        stmt.setBlock(block);

        return stmt;
    }

    private void checkParametersRedefine(List<SQLParameter> parameters) {
        Set<String> names =
            parameters.stream().map(t -> t.getName().getSimpleName().toLowerCase()).collect(Collectors.toSet());
        if (names.size() != parameters.size()) {
            throw new RuntimeException("Duplicate procedure parameters found");
        }
    }

    /**
     * parse create procedure statement
     */
    public SQLCreateProcedureStatement parseCreateProcedure() {
        /**
         * CREATE OR REPALCE PROCEDURE SP_NAME(parameter_list) BEGIN block_statement END
         */
        SQLCreateProcedureStatement stmt = new SQLCreateProcedureStatement();
        stmt.setDbType(dbType);

        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();

            if (lexer.token() == Token.OR) {
                lexer.nextToken();
                accept(Token.REPLACE);
                stmt.setOrReplace(true);
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
            lexer.nextToken();
            accept(Token.EQ);
            SQLName definer = this.getExprParser().userName();
            stmt.setDefiner(definer);
        }

        accept(Token.PROCEDURE);

        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.LPAREN) {// match "("
            lexer.nextToken();
            parserParameters(stmt.getParameters(), stmt);
            checkParametersRedefine(stmt.getParameters());
            accept(Token.RPAREN);// match ")"
        }

        for (; ; ) {
            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                stmt.setComment(this.exprParser.charExpr());
            } else if (lexer.identifierEquals(FnvHash.Constants.LANGUAGE)) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setLanguageSql(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.DETERMINISTIC)) {
                lexer.nextToken();
                stmt.setDeterministic(true);
            } else if (lexer.token() == Token.NOT) {
                lexer.nextToken();
                acceptIdentifier("DETERMINISTIC");
                stmt.setDeterministic(false);
            } else if (lexer.identifierEquals(FnvHash.Constants.CONTAINS) || lexer.token() == Token.CONTAINS) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setSqlDataAccess(SqlDataAccess.CONTAINS_SQL);
            } else if (lexer.identifierEquals("NO")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setSqlDataAccess(SqlDataAccess.NO_SQL);
            } else if (lexer.identifierEquals("READS")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                stmt.setSqlDataAccess(SqlDataAccess.READS_SQL_DATA);
            } else if (lexer.identifierEquals("MODIFIES")) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                acceptIdentifier("DATA");
                stmt.setSqlDataAccess(SqlDataAccess.MODIFIES_SQL_DATA);
            } else if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
                lexer.nextToken();
                acceptIdentifier("SECURITY");
                if (lexer.identifierEquals("DEFINER")) {
                    stmt.setSqlSecurity(SqlSecurity.DEFINER);
                    lexer.nextToken();
                } else if (lexer.identifierEquals("INVOKER")) {
                    stmt.setSqlSecurity(SqlSecurity.INVOKER);
                    lexer.nextToken();
                } else {
                    throw new RuntimeException("sql security can only be set to definer or invoker");
                }
            } else {
                break;
            }
        }

        if (lexer.token() == Token.EOF) {
            throw new RuntimeException("procedure content should not empty");
        }

        SQLStatement block = this.parseImplicitBlock();

        stmt.setBlock(block);

        return stmt;
    }

    /**
     * parse create procedure parameters
     */
    private void parserParameters(List<SQLParameter> parameters, SQLObject parent) {
        if (lexer.token() == Token.RPAREN) {
            return;
        }

        for (; ; ) {
            SQLParameter parameter = new SQLParameter();

            if (lexer.token() == Token.CURSOR) {
                lexer.nextToken();

                parameter.setName(this.exprParser.name());

                accept(Token.IS);
                SQLSelect select = this.createSQLSelectParser().select();

                SQLDataTypeImpl dataType = new SQLDataTypeImpl();
                dataType.setName("CURSOR");
                parameter.setDataType(dataType);

                parameter.setDefaultValue(new SQLQueryExpr(select));

            } else if (lexer.token() == Token.IN || lexer.token() == Token.OUT || lexer.token() == Token.INOUT) {

                if (lexer.token() == Token.IN) {
                    parameter.setParamType(ParameterType.IN);
                } else if (lexer.token() == Token.OUT) {
                    parameter.setParamType(ParameterType.OUT);
                } else if (lexer.token() == Token.INOUT) {
                    parameter.setParamType(ParameterType.INOUT);
                }
                lexer.nextToken();

                parameter.setName(this.exprParser.name());

                parameter.setDataType(this.exprParser.parseDataType());
            } else {
                parameter.setParamType(ParameterType.DEFAULT);// default parameter type is in
                parameter.setName(this.exprParser.name());
                parameter.setDataType(this.exprParser.parseDataType());

                if (lexer.token() == Token.COLONEQ) {
                    lexer.nextToken();
                    parameter.setDefaultValue(this.exprParser.expr());
                }
            }

            parameters.add(parameter);
            if (lexer.token() == Token.COMMA || lexer.token() == Token.SEMI) {
                lexer.nextToken();
            }

            if (lexer.token() != Token.BEGIN && lexer.token() != Token.RPAREN) {
                continue;
            }

            break;
        }
    }

    /**
     * parse procedure statement block
     */
    private void parseProcedureStatementList(List<SQLStatement> statementList) {
        parseProcedureStatementList(statementList, -1);
    }

    /**
     * parse procedure statement block
     */
    private void parseProcedureStatementList(List<SQLStatement> statementList, int max) {

        for (; ; ) {
            if (max != -1) {
                if (statementList.size() >= max) {
                    return;
                }
            }

            if (lexer.token() == Token.EOF) {
                return;
            }
            if (lexer.token() == Token.END) {
                return;
            }
            if (lexer.token() == Token.ELSE) {
                return;
            }
            if (lexer.token() == (Token.SEMI)) {
                lexer.nextToken();
                continue;
            }
            if (lexer.token() == Token.WHEN) {
                return;
            }
            if (lexer.token() == Token.UNTIL) {
                return;
            }
            // select into
            if (lexer.token() == (Token.SELECT)) {
                statementList.add(this.parseSelectInto());
                continue;
            }

            // update
            if (lexer.token() == (Token.UPDATE)) {
                statementList.add(parseUpdateStatement());
                continue;
            }

            // create
            if (lexer.token() == (Token.CREATE)) {
                statementList.add(parseCreate());
                continue;
            }

            // insert
            if (lexer.token() == Token.INSERT) {
                SQLStatement stmt = parseInsert();
                statementList.add(stmt);
                continue;
            }

            // delete
            if (lexer.token() == (Token.DELETE)) {
                statementList.add(parseDeleteStatement());
                continue;
            }

            // call
            if (lexer.token() == Token.LBRACE || lexer.identifierEquals("CALL")) {
                statementList.add(this.parseCall());
                continue;
            }

            // begin
            if (lexer.token() == Token.BEGIN) {
                statementList.add(this.parseBlock());
                continue;
            }

            if (lexer.token() == Token.VARIANT) {
                SQLExpr variant = this.exprParser.primary();
                if (variant instanceof SQLBinaryOpExpr) {
                    SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) variant;
                    if (binaryOpExpr.getOperator() == SQLBinaryOperator.Assignment) {
                        SQLSetStatement stmt = new SQLSetStatement(binaryOpExpr.getLeft(), binaryOpExpr.getRight(),
                            getDbType());
                        statementList.add(stmt);
                        continue;
                    }
                }
                accept(Token.COLONEQ);
                SQLExpr value = this.exprParser.expr();

                SQLSetStatement stmt = new SQLSetStatement(variant, value, getDbType());
                statementList.add(stmt);
                continue;
            }

            // select
            if (lexer.token() == Token.LPAREN) {
                char ch = lexer.current();
                int bp = lexer.bp();
                lexer.nextToken();

                if (lexer.token() == Token.SELECT) {
                    lexer.reset(bp, ch, Token.LPAREN);
                    statementList.add(this.parseSelect());
                    continue;
                } else {
                    throw new ParserException("TODO. " + lexer.info());
                }
            }
            // assign statement
            if (lexer.token() == Token.SET) {
                statementList.add(this.parseAssign());
                continue;
            }

            // while statement
            if (lexer.token() == Token.WHILE) {
                SQLStatement stmt = this.parseWhile();
                statementList.add(stmt);
                continue;
            }

            // loop statement
            if (lexer.token() == Token.LOOP) {
                statementList.add(this.parseLoop());
                continue;
            }

            // if statement
            if (lexer.token() == Token.IF) {
                statementList.add(this.parseIf());
                continue;
            }

            // case statement
            if (lexer.token() == Token.CASE) {
                statementList.add(this.parseCase());
                continue;
            }

            // declare statement
            if (lexer.token() == Token.DECLARE) {
                SQLStatement stmt = this.parseDeclare();
                statementList.add(stmt);
                continue;
            }

            // leave statement
            if (lexer.token() == Token.LEAVE) {
                statementList.add(this.parseLeave());
                continue;
            }

            // iterate statement
            if (lexer.token() == Token.ITERATE) {
                statementList.add(this.parseIterate());
                continue;
            }

            // repeat statement
            if (lexer.token() == Token.REPEAT) {
                statementList.add(this.parseRepeat());
                continue;
            }

            // open cursor
            if (lexer.token() == Token.OPEN) {
                statementList.add(this.parseOpen());
                continue;
            }

            // close cursor
            if (lexer.token() == Token.CLOSE) {
                statementList.add(this.parseClose());
                continue;
            }

            // fetch cursor into
            if (lexer.token() == Token.FETCH) {
                statementList.add(this.parseFetch());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.CHECKSUM)) {
                statementList.add(this.parseChecksum());
                continue;
            }

            if (lexer.token() == Token.IDENTIFIER) {
                String label = lexer.stringVal();
                char ch = lexer.current();
                int bp = lexer.bp();
                lexer.setLexSingleColon(true);
                lexer.nextToken();
                lexer.setLexSingleColon(false);
                if (lexer.token() == Token.COLON) {
                    lexer.nextToken();
                    if (lexer.token() == Token.LOOP) {
                        // parse loop statement
                        statementList.add(this.parseLoop(label));
                    } else if (lexer.token() == Token.WHILE) {
                        // parse while statement with label
                        statementList.add(this.parseWhile(label));
                    } else if (lexer.token() == Token.BEGIN) {
                        // parse begin-end statement with label
                        statementList.add(this.parseBlock(label));
                    } else if (lexer.token() == Token.REPEAT) {
                        // parse repeat statement with label
                        statementList.add(this.parseRepeat(label));
                    }
                    continue;
                } else {
                    lexer.reset(bp, ch, Token.IDENTIFIER);
                }

            }
            throw new ParserException("TODO, " + lexer.info());
        }

    }

    public MySqlChecksumTableStatement parseChecksum() {
        MySqlChecksumTableStatement stmt = new MySqlChecksumTableStatement();
        if (lexer.identifierEquals(FnvHash.Constants.CHECKSUM)) {
            lexer.nextToken();
        } else {
            throw new ParserException("TODO " + lexer.info());
        }

        accept(Token.TABLE);

        for (; ; ) {
            SQLName table = this.exprParser.name();
            stmt.addTable(new SQLExprTableSource(table));

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        return stmt;
    }

    /**
     * parse if statement
     *
     * @return MySqlIfStatement
     */
    public SQLIfStatement parseIf() {
        accept(Token.IF);

        SQLIfStatement stmt = new SQLIfStatement();

        stmt.setCondition(this.exprParser.expr());

        accept(Token.THEN);

        this.parseStatementList(stmt.getStatements(), -1, stmt);

        while (lexer.token() == Token.ELSEIF) {
            lexer.nextToken();

            SQLIfStatement.ElseIf elseIf = new SQLIfStatement.ElseIf();

            elseIf.setCondition(this.exprParser.expr());
            elseIf.setParent(stmt);

            accept(Token.THEN);
            this.parseStatementList(elseIf.getStatements(), -1, elseIf);

            stmt.getElseIfList().add(elseIf);
        }

        if (lexer.token() == Token.ELSE) {
            lexer.nextToken();
            SQLIfStatement.Else elseItem = new SQLIfStatement.Else();
            this.parseStatementList(elseItem.getStatements(), -1, elseItem);
            stmt.setElseItem(elseItem);
        }

        accept(Token.END);
        accept(Token.IF);
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        stmt.setAfterSemi(true);

        return stmt;
    }

    /**
     * parse while statement
     *
     * @return MySqlWhileStatement
     */
    public SQLWhileStatement parseWhile() {
        accept(Token.WHILE);
        SQLWhileStatement stmt = new SQLWhileStatement();

        stmt.setCondition(this.exprParser.expr());

        accept(Token.DO);

        this.parseStatementList(stmt.getStatements(), -1, stmt);

        accept(Token.END);

        accept(Token.WHILE);

        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }

        stmt.setAfterSemi(true);

        checkStatementsNonEmpty(stmt.getStatements(), "While Loop");

        return stmt;

    }

    /**
     * parse while statement with label
     *
     * @return MySqlWhileStatement
     */
    public SQLWhileStatement parseWhile(String label) {
        accept(Token.WHILE);

        SQLWhileStatement stmt = new SQLWhileStatement();

        stmt.setLabelName(label);

        stmt.setCondition(this.exprParser.expr());

        accept(Token.DO);

        this.parseStatementList(stmt.getStatements(), -1, stmt);

        accept(Token.END);

        accept(Token.WHILE);

        if (lexer.identifierEquals(label)) {
            lexer.nextToken();
        }

        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }

        stmt.setAfterSemi(true);

        checkStatementsNonEmpty(stmt.getStatements(), "While Loop");

        return stmt;

    }

    private void checkStatementsNonEmpty(List<SQLStatement> stmts, String type) {
        if (null == stmts || stmts.size() == 0) {
            throw new RuntimeException(String.format("statements within %s should not empty", type));
        }
    }

    /**
     * parse case statement
     *
     * @return MySqlCaseStatement
     */
    public MySqlCaseStatement parseCase() {
        MySqlCaseStatement stmt = new MySqlCaseStatement();
        accept(Token.CASE);

        if (lexer.token() == Token.WHEN)// grammar 1
        {
            while (lexer.token() == Token.WHEN) {
                accept(Token.WHEN);

                MySqlWhenStatement when = new MySqlWhenStatement();
                // when expr
                when.setCondition(exprParser.expr());

                accept(Token.THEN);

                // when block
                this.parseStatementList(when.getStatements(), -1, when);

                stmt.addWhenStatement(when);
            }
            if (lexer.token() == Token.ELSE) {
                accept(Token.ELSE);
                // parse else block
                SQLIfStatement.Else elseStmt = new SQLIfStatement.Else();
                this.parseStatementList(elseStmt.getStatements(), -1, elseStmt);
                stmt.setElseItem(elseStmt);
            }
        } else// grammar 2
        {
            // case expr
            stmt.setCondition(exprParser.expr());

            while (lexer.token() == Token.WHEN) {
                accept(Token.WHEN);
                MySqlWhenStatement when = new MySqlWhenStatement();
                // when expr
                when.setCondition(exprParser.expr());

                accept(Token.THEN);

                // when block
                this.parseStatementList(when.getStatements(), -1, when);

                stmt.addWhenStatement(when);
            }
            if (lexer.token() == Token.ELSE) {
                accept(Token.ELSE);
                // else block
                SQLIfStatement.Else elseStmt = new SQLIfStatement.Else();
                this.parseStatementList(elseStmt.getStatements(), -1, elseStmt);
                stmt.setElseItem(elseStmt);
            }
        }
        accept(Token.END);
        accept(Token.CASE);
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        stmt.setAfterSemi(true);
        return stmt;

    }

    /**
     * parse declare statement
     */
    public SQLStatement parseDeclare() {
        char markChar = lexer.current();
        int markBp = lexer.bp();

        lexer.nextToken();

        if (lexer.token() == Token.CONTINUE) {
            lexer.reset(markBp, markChar, Token.DECLARE);
            return this.parseDeclareHandler();
        }

        lexer.nextToken();
        if (lexer.token() == Token.CURSOR) {
            lexer.reset(markBp, markChar, Token.DECLARE);
            return this.parseCursorDeclare();
        } else if (lexer.identifierEquals("HANDLER")) {
            //DECLARE异常处理程序 [add by zhujun 2016-04-16]
            lexer.reset(markBp, markChar, Token.DECLARE);
            return this.parseDeclareHandler();
        } else if (lexer.token() == Token.CONDITION) {
            //DECLARE异常 [add by zhujun 2016-04-17]
            lexer.reset(markBp, markChar, Token.DECLARE);
            return this.parseDeclareCondition();
        } else {
            lexer.reset(markBp, markChar, Token.DECLARE);
        }

        MySqlDeclareStatement stmt = new MySqlDeclareStatement();
        accept(Token.DECLARE);
        // lexer.nextToken();
        for (; ; ) {
            SQLDeclareItem item = new SQLDeclareItem();
            item.setName(exprParser.name());

            stmt.addVar(item);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                stmt.setAfterSemi(true);
                continue;
            } else if (lexer.token() != Token.EOF) {
                // var type
                SQLDataType dataType = exprParser.parseDataType();
                item.setDataType(dataType);

                if ("ENUM".equalsIgnoreCase(dataType.getName())) {
                    // ignore character set xxx
                    if (lexer.identifierEquals("CHARACTER")) {
                        lexer.nextToken();
                        if (lexer.identifierEquals("SET") || lexer.token() == Token.SET) {
                            lexer.nextToken();
                            if (lexer.token() != Token.EOF) {
                                lexer.nextToken();
                            } else {
                                throw new RuntimeException("charset expected, but not found");
                            }
                        }
                    }
                    // ignore charset xxx
                    if (lexer.identifierEquals("CHARSET")) {
                        // skip charset keyword
                        lexer.nextToken();
                        // skip defined charset
                        if (lexer.token() != Token.EOF) {
                            lexer.nextToken();
                        } else {
                            throw new RuntimeException("charset expected, but not found");
                        }
                    }
                    // ignore collate xxx
                    if (lexer.identifierEquals("COLLATE")) {
                        // skip collate keyword
                        lexer.nextToken();
                        // skip defined collation
                        if (lexer.token() != Token.EOF) {
                            lexer.nextToken();
                        } else {
                            throw new RuntimeException("collation expected, but not found");
                        }
                    }
                }

                if (lexer.token() == Token.DEFAULT) {
                    lexer.nextToken();
                    SQLExpr defaultValue = this.exprParser.primary();
                    item.setValue(defaultValue);
                }
                if (lexer.token() == COMMA) {
                    lexer.nextToken();
                    continue;
                }
//                lexer.nextToken();
//                if (lexer.token() == Token.SEMI) {
                break;
//                }
            } else {
                throw new ParserException("TODO. " + lexer.info());
            }
        }
        return stmt;
    }

    /**
     * parse assign statement
     */
    public SQLSetStatement parseAssign() {
        accept(Token.SET);
        SQLSetStatement stmt = new SQLSetStatement(getDbType());
        parseAssignItems(stmt.getItems(), stmt);
        return stmt;
    }

    /**
     * parse select into
     */
    public MySqlSelectIntoStatement parseSelectInto() {
        MySqlSelectIntoParser parse = new MySqlSelectIntoParser(this.exprParser);
        return parse.parseSelectInto();
    }

    /**
     * parse loop statement
     */
    public SQLLoopStatement parseLoop() {
        SQLLoopStatement loopStmt = new SQLLoopStatement();
        accept(Token.LOOP);
        this.parseStatementList(loopStmt.getStatements(), -1, loopStmt);
        accept(Token.END);
        accept(Token.LOOP);
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        loopStmt.setAfterSemi(true);
        checkStatementsNonEmpty(loopStmt.getStatements(), "Loop");
        return loopStmt;
    }

    /**
     * parse loop statement with label
     */
    public SQLLoopStatement parseLoop(String label) {
        SQLLoopStatement loopStmt = new SQLLoopStatement();
        loopStmt.setLabelName(label);
        accept(Token.LOOP);
        this.parseStatementList(loopStmt.getStatements(), -1, loopStmt);
        accept(Token.END);
        accept(Token.LOOP);
        if (lexer.token() == Token.EOF) {
            loopStmt.setAfterSemi(true);
            return loopStmt;
        }
        if (lexer.token() != Token.SEMI) {
            acceptIdentifier(label);
        }
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        loopStmt.setAfterSemi(true);
        checkStatementsNonEmpty(loopStmt.getStatements(), "Loop");
        return loopStmt;
    }

    /**
     * parse loop statement with label
     */
    public SQLBlockStatement parseBlock(String label) {
        SQLBlockStatement block = new SQLBlockStatement();
        block.setLabelName(label);
        accept(Token.BEGIN);
        this.parseStatementList(block.getStatementList(), -1, block);
        accept(Token.END);
        // label is optional, not required
        if (lexer.identifierEquals(label)) {
            lexer.nextToken();
        }
        checkDuplicateDeclareItems(block);

        block.setAfterSemi(true);
        return block;
    }

    /**
     * parse leave statement
     */
    public MySqlLeaveStatement parseLeave() {
        accept(Token.LEAVE);
        MySqlLeaveStatement leaveStmt = new MySqlLeaveStatement();
        leaveStmt.setLabelName(exprParser.name().getSimpleName());
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        leaveStmt.setAfterSemi(true);
        return leaveStmt;
    }

    /**
     * parse iterate statement
     */
    public MySqlIterateStatement parseIterate() {
        accept(Token.ITERATE);
        MySqlIterateStatement iterateStmt = new MySqlIterateStatement();
        iterateStmt.setLabelName(exprParser.name().getSimpleName());
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        iterateStmt.setAfterSemi(true);
        return iterateStmt;
    }

    /**
     * parse repeat statement
     */
    public MySqlRepeatStatement parseRepeat() {
        MySqlRepeatStatement stmt = new MySqlRepeatStatement();
        accept(Token.REPEAT);
        parseStatementList(stmt.getStatements(), -1, stmt);
        accept(Token.UNTIL);
        stmt.setCondition(exprParser.expr());
        accept(Token.END);
        accept(Token.REPEAT);
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        stmt.setAfterSemi(true);
        checkStatementsNonEmpty(stmt.getStatements(), "Repeat loop");
        return stmt;
    }

    /**
     * parse repeat statement with label
     */
    public MySqlRepeatStatement parseRepeat(String label) {
        MySqlRepeatStatement repeatStmt = new MySqlRepeatStatement();
        repeatStmt.setLabelName(label);
        accept(Token.REPEAT);
        this.parseStatementList(repeatStmt.getStatements(), -1, repeatStmt);
        accept(Token.UNTIL);
        repeatStmt.setCondition(exprParser.expr());
        accept(Token.END);
        accept(Token.REPEAT);
        if (lexer.identifierEquals(label)) {
            lexer.nextToken();
        }
        if (lexer.token() != Token.EOF) {
            accept(Token.SEMI);
        }
        repeatStmt.setAfterSemi(true);
        checkStatementsNonEmpty(repeatStmt.getStatements(), "Repeat loop");
        return repeatStmt;
    }

    /**
     * parse cursor declare statement
     */
    public MySqlCursorDeclareStatement parseCursorDeclare() {
        MySqlCursorDeclareStatement stmt = new MySqlCursorDeclareStatement();
        accept(Token.DECLARE);

        stmt.setCursorName(exprParser.name());

        accept(Token.CURSOR);

        accept(FOR);

        //SQLSelectStatement selelctStmt = (SQLSelectStatement) parseSelect();
        SQLSelect select = this.createSQLSelectParser().select();
        stmt.setSelect(select);

        accept(Token.SEMI);
        stmt.setAfterSemi(true);

        return stmt;
    }

    /**
     * zhujun [455910092@qq.com]
     * parse spstatement
     */
    public SQLStatement parseSpStatement() {
        if (lexer.token() == Token.SELECT) {
            return parseSelect();
        }

        // update
        if (lexer.token() == (Token.UPDATE)) {
            return parseUpdateStatement();
        }

        // create
        if (lexer.token() == (Token.CREATE)) {
            return parseCreate();
        }

        // insert
        if (lexer.token() == Token.INSERT) {
            return parseInsert();
        }

        // delete
        if (lexer.token() == (Token.DELETE)) {
            return parseDeleteStatement();
        }

        // begin
        if (lexer.token() == Token.BEGIN) {
            return this.parseBlock();
        }

        // select
        if (lexer.token() == Token.LPAREN) {
            char ch = lexer.current();
            int bp = lexer.bp();
            lexer.nextToken();

            if (lexer.token() == Token.SELECT) {
                lexer.reset(bp, ch, Token.LPAREN);
                return this.parseSelect();
            } else {
                throw new ParserException("TODO. " + lexer.info());
            }
        }
        // assign statement
        if (lexer.token() == Token.SET) {
            return parseAssign();
        }

        if (lexer.token() == Token.LEAVE) {
            return parseLeave();
        }

        if (lexer.token() == Token.ITERATE) {
            return parseIterate();
        }

        throw new ParserException("error sp_statement. " + lexer.info());
    }

    /**
     * 定义异常处理程序
     */
    public MySqlDeclareHandlerStatement parseDeclareHandler() {
        //DECLARE handler_type HANDLER FOR condition_value[,...] sp_statement
        //handler_type 取值为 CONTINUE | EXIT | UNDO
        //condition_value 取值为 SQLWARNING | NOT FOUND | SQLEXCEPTION | SQLSTATE value(异常码 e.g 1062)

        MySqlDeclareHandlerStatement stmt = new MySqlDeclareHandlerStatement();
        accept(Token.DECLARE);
        //String handlerType = exprParser.name().getSimpleName();
        if (lexer.token() == Token.CONTINUE || lexer.identifierEquals("CONTINUE")) {
            stmt.setHandleType(MySqlHandlerType.CONTINUE);
        } else if (lexer.token() == Token.EXIT || lexer.identifierEquals("EXIT")) {
            stmt.setHandleType(MySqlHandlerType.EXIT);
        } else if (lexer.token() == Token.UNDO || lexer.identifierEquals("UNDO")) {
            stmt.setHandleType(MySqlHandlerType.UNDO);
        } else {
            throw new ParserException("unkown handle type. " + lexer.info());
        }
        lexer.nextToken();

        acceptIdentifier("HANDLER");

        accept(FOR);

        for (; ; ) {
            String tokenName = lexer.stringVal();
            ConditionValue condition = new ConditionValue();

            if (tokenName.equalsIgnoreCase("NOT")) {//for 'NOT FOUND'
                lexer.nextToken();
                acceptIdentifier("FOUND");
                condition.setType(ConditionType.SYSTEM);
                condition.setValue("NOT FOUND");

            } else if (tokenName.equalsIgnoreCase("SQLSTATE")) { //for SQLSTATE (SQLSTATE '10001')
                condition.setType(ConditionType.SQLSTATE);
                lexer.nextToken();
                //condition.setValue(lexer.stringVal());
                //lexer.nextToken();
                condition.setValue(exprParser.name().toString());
            } else if (lexer.identifierEquals("SQLEXCEPTION")) { //for SQLEXCEPTION
                condition.setType(ConditionType.SYSTEM);
                condition.setValue(lexer.stringVal());
                lexer.nextToken();
            } else if (lexer.identifierEquals("SQLWARNING")) { //for SQLWARNING
                condition.setType(ConditionType.SYSTEM);
                condition.setValue(lexer.stringVal());
                lexer.nextToken();
            } else { //for condition_name or mysql_error_code
                if (lexer.token() == Token.LITERAL_INT) {
                    condition.setType(ConditionType.MYSQL_ERROR_CODE);
                    condition.setValue(lexer.integerValue().toString());
                } else {
                    condition.setType(ConditionType.SELF);
                    condition.setValue(tokenName);
                }
                lexer.nextToken();
            }
            stmt.getConditionValues().add(condition);
            if (lexer.token() == Token.COMMA) {
                accept(Token.COMMA);
                continue;
            } else if (lexer.token() != Token.EOF) {
                break;
            } else {
                throw new ParserException("declare handle not eof");
            }
        }

        stmt.setSpStatement(parseSpStatement());
        stmt.getSpStatement().setParent(stmt);
        if (!(stmt.getSpStatement() instanceof SQLBlockStatement) && lexer.token() == Token.SEMI) {
            accept(Token.SEMI);
        }
        stmt.setAfterSemi(true);

        return stmt;
    }

    /**
     * zhujun [455910092@qq.com]
     * 2016-04-17
     * 定义条件
     */
    public MySqlDeclareConditionStatement parseDeclareCondition() {
        /*
        DECLARE condition_name CONDITION FOR condition_value

    	condition_value:
    	    SQLSTATE [VALUE] sqlstate_value
    	  | mysql_error_code
    	*/
        MySqlDeclareConditionStatement stmt = new MySqlDeclareConditionStatement();
        accept(Token.DECLARE);

        stmt.setConditionName(exprParser.name().toString());

        accept(Token.CONDITION);

        accept(FOR);

        String tokenName = lexer.stringVal();
        ConditionValue condition = new ConditionValue();
        if (tokenName.equalsIgnoreCase("SQLSTATE")) { //for SQLSTATE (SQLSTATE '10001')
            condition.setType(ConditionType.SQLSTATE);
            lexer.nextToken();
            condition.setValue(exprParser.name().toString());
        } else if (lexer.token() == Token.LITERAL_INT) {
            condition.setType(ConditionType.MYSQL_ERROR_CODE);
            condition.setValue(lexer.integerValue().toString());
            lexer.nextToken();
        } else {
            throw new ParserException("declare condition grammer error. " + lexer.info());
        }

        stmt.setConditionValue(condition);

        accept(Token.SEMI);

        stmt.setAfterSemi(true);
        return stmt;
    }

    @Override
    public SQLStatement parseFlashback() {
        MySqlFlashbackStatement stmt = new MySqlFlashbackStatement();
        acceptIdentifier("FLASHBACK");
        accept(Token.TABLE);

        SQLName name = this.exprParser.name();
        stmt.setName(name);
        accept(Token.TO);
        acceptIdentifier("BEFORE");
        accept(Token.DROP);

        if (lexer.identifierEquals(FnvHash.Constants.RENAME)) {
            lexer.nextToken();
            accept(Token.TO);
            SQLName to = this.exprParser.name();
            stmt.setRenameTo(to);
        }

        return stmt;
    }

    public java.sql.Timestamp getCurrentTimestamp() {
        return now;
    }

    public java.sql.Date getCurrentDate() {
        return currentDate;
    }

    public MySqlCreateTableParser getSQLCreateTableParser() {
        return new MySqlCreateTableParser(this.exprParser);
    }

    @Override
    public SQLStatement parseCopy() {
        acceptIdentifier("COPY");

        SQLCopyFromStatement stmt = new SQLCopyFromStatement();

        SQLExpr table = this.exprParser.name();
        stmt.setTable(new SQLExprTableSource(table));

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            this.exprParser.names(stmt.getColumns(), stmt);
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();
            this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
        }

        accept(Token.FROM);
        SQLExpr from = this.exprParser.expr();
        stmt.setFrom(from);

        if (lexer.identifierEquals(FnvHash.Constants.CREDENTIALS)) {
            lexer.nextToken();

            for (; ; ) {
                if (lexer.identifierEquals(FnvHash.Constants.ACCESS_KEY_ID)) {
                    lexer.nextToken();
                    SQLExpr accessKeyId = this.exprParser.primary();
                    stmt.setAccessKeyId(accessKeyId);
                } else if (lexer.identifierEquals(FnvHash.Constants.ACCESS_KEY_SECRET)) {
                    lexer.nextToken();
                    SQLExpr accessKeySecret = this.exprParser.primary();
                    stmt.setAccessKeySecret(accessKeySecret);
                } else {
                    break;
                }
            }
        }

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            accept(Token.LPAREN);
            for (; ; ) {
                SQLName name = this.exprParser.name();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr value = this.exprParser.expr();
                SQLAssignItem item = new SQLAssignItem(name, value);
                item.setParent(stmt);
                stmt.getOptions().add(item);
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }
            accept(Token.RPAREN);
        }
        return stmt;
    }

    private SQLStatement parseChangeRuleVersion(boolean upgrading) {
        // UPGRADE/DOWNGRADE RULE VERSION TO <version>
        acceptIdentifier("RULE");
        acceptIdentifier("VERSION");
        accept(Token.TO);

        SQLIntegerExpr versionExpr = this.exprParser.integerExpr();

        DrdsChangeRuleVersionStatement stmt = new DrdsChangeRuleVersionStatement();
        stmt.setUpgrading(upgrading);
        stmt.setVersion(versionExpr);
        return stmt;
    }

    public void parseGenericDdlCommand(DrdsGenericDDLJob stmt) {
        parseGenericDdlCommand(stmt, false);
    }

    public boolean parseGenericDdlCommand(DrdsGenericDDLJob stmt, List<SQLStatement> statementList) {
        parseGenericDdlCommand(stmt, false);
        statementList.add(stmt);
        return true;
    }

    public void parseGenericDdlCommand(DrdsGenericDDLJob stmt, boolean allowEmptyParams) {
        if (Token.ALL == lexer.token()) {
            lexer.nextToken();
            stmt.setAllJobs(true);
        } else {
            if (allowEmptyParams && (lexer.token() == Token.EOF || lexer.token() == Token.SEMI)) {
                return;
            }
            while (true) {
                stmt.addJobId(lexer.integerValue().longValue());
                accept(Token.LITERAL_INT);
                if (Token.COMMA == lexer.token()) {
                    lexer.nextToken();
                } else if (lexer.token() == Token.EOF || lexer.token() == Token.SEMI) {
                    break;
                } else {
                    throw new ParserException(
                        "syntax error, expect job id, actual " + lexer.token() + ", " + lexer.info());
                }
            }
        }
    }

    protected DrdsPauseScheduleStatement parsePauseSchedule() {
        acceptIdentifier("SCHEDULE");

        DrdsPauseScheduleStatement stmt = new DrdsPauseScheduleStatement();
        stmt.setDbType(dbType);
        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.EXISTS);
            stmt.setIfExist(true);
        }

        if (lexer.token() == FOR) {
            lexer.nextToken();
            accept(ALL);
            acceptIdentifier("LOCAL_PARTITION");
            stmt.setForLocalPartition(true);
            return stmt;
        }

        stmt.setScheduleId(lexer.integerValue().longValue());
        accept(Token.LITERAL_INT);

        return stmt;
    }

    protected DrdsFireScheduleStatement parseFireSchedule() {
        acceptIdentifier("SCHEDULE");
        DrdsFireScheduleStatement stmt = new DrdsFireScheduleStatement();
        stmt.setDbType(dbType);
        stmt.setScheduleId(lexer.integerValue().longValue());
        accept(Token.LITERAL_INT);
        return stmt;
    }

    protected DrdsContinueScheduleStatement parseContinueSchedule() {
        acceptIdentifier("SCHEDULE");

        DrdsContinueScheduleStatement stmt = new DrdsContinueScheduleStatement();
        stmt.setDbType(dbType);
        stmt.setScheduleId(lexer.integerValue().longValue());
        accept(Token.LITERAL_INT);

        return stmt;
    }

}
