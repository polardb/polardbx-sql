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

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.eagleeye.EagleeyeHelper;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentUserExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeRefExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexOptions;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByList;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionValue;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByList;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLWindow;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateOption;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExtractExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMatchAgainstExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNotExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSequenceExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSomeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableAllocateLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableExpireLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetPartitionsLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsExtractHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsMergePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsMovePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsRenamePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSetPartitionsLocality;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterJoinGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterSystemSetConfigStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddExtPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableConvertCharSet;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDisableKeys;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropExtPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropFile;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableEnableKeys;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableExchangePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupAddTable;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableModifyPartitionValues;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTablePartitionCount;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSetComment;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableSetOption;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableTruncatePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLChangeRoleStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraintImpl;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateJoinGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTriggerStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropJoinGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTriggerStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLLoopStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLMergeTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPurgeRecyclebinStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPurgeTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRebalanceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSavePointStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement.Option;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowOutlinesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowPartitionsStmt;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowRecyclebinStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatisticStmt;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWhileStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SqlDataAccess;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIndexHintImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.CreateFileStorageStatement;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsContinueScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsConvertAllSequencesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsDropCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsDropCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsDropFileStorageStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsDropScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsFireScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectDDLJobCache;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectGroupSeqRangeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectRuleVersionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsMoveDataBase;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPushDownUdfStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRecoverDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRefreshLocalRulesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRefreshTopology;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsSlowSqlCclStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsUnArchiveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySql8ShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAlterColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeMasterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeReplicationFilterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCheckTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExtPartition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlFlashbackStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlOptimizeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameSequenceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlResetSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetDefaultRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBroadcastsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateEventStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabaseStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowDatasourcesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowDdlStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowDsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowEngineStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowEventsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowFilesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionCodeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPhysicalProcesslistStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPrivilegesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureCodeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProfileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProfilesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowRuleStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSequencesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveHostsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSlowStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowTableInfoStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowTopologyStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowTraceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStartSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStopSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowDbLockStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowHtcStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowStcStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.SQLShowPartitionsHeatmapStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.SeqTypeUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter.HintCollection;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.parse.FastSqlParserException;
import com.alibaba.polardbx.optimizer.parse.FastSqlParserException.ExceptionType;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TDDLSqlFunction;
import com.alibaba.polardbx.optimizer.parse.TypeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.FieldMetaData;
import com.alibaba.polardbx.optimizer.parse.bean.NumberParser;
import com.alibaba.polardbx.optimizer.parse.bean.Sequence;
import com.alibaba.polardbx.optimizer.parse.bean.TableMetaData;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.newrule.RuleUtils;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.GroupConcatCall;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.OutFileParams;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAddFullTextIndex;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddPrimaryKey;
import org.apache.calcite.sql.SqlAddSpatialIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterColumnDefaultVal;
import org.apache.calcite.sql.SqlAlterFileStorage;
import org.apache.calcite.sql.SqlAlterFunction;
import org.apache.calcite.sql.SqlAlterJoinGroup;
import org.apache.calcite.sql.SqlAlterProcedure;
import org.apache.calcite.sql.SqlAlterRule;
import org.apache.calcite.sql.SqlAlterSequence;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterSystemSetConfig;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlAlterTable.ColumnOpt;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableAllocateLocalPartition;
import org.apache.calcite.sql.SqlAlterTableDropFile;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlAlterTableExpireLocalPartition;
import org.apache.calcite.sql.SqlAlterTableExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupAddTable;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSetLocality;
import org.apache.calcite.sql.SqlAlterTableGroupSetPartitionsLocality;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTablePartitionCount;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableRemoveLocalPartition;
import org.apache.calcite.sql.SqlAlterTableRemovePartitioning;
import org.apache.calcite.sql.SqlAlterTableRenameIndex;
import org.apache.calcite.sql.SqlAlterTableRenamePartition;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlAlterTableRepartitionLocalPartition;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlAnalyzeTable;
import org.apache.calcite.sql.SqlBaseline;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCancelDdlJob;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlChangeConsensusRole;
import org.apache.calcite.sql.SqlChangeDdlJob;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCheckGlobalIndex;
import org.apache.calcite.sql.SqlCheckTable;
import org.apache.calcite.sql.SqlClearCclRules;
import org.apache.calcite.sql.SqlClearCclTriggers;
import org.apache.calcite.sql.SqlClearDdlJobCache;
import org.apache.calcite.sql.SqlClearSeqCache;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlContinueDdlJob;
import org.apache.calcite.sql.SqlContinueSchedule;
import org.apache.calcite.sql.SqlConvertAllSequences;
import org.apache.calcite.sql.SqlConvertToCharacterSet;
import org.apache.calcite.sql.SqlCreateCclRule;
import org.apache.calcite.sql.SqlCreateCclTrigger;
import org.apache.calcite.sql.SqlCreateFileStorage;
import org.apache.calcite.sql.SqlCreateFunction;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateIndex.SqlIndexAlgorithmType;
import org.apache.calcite.sql.SqlCreateIndex.SqlIndexConstraintType;
import org.apache.calcite.sql.SqlCreateIndex.SqlIndexLockType;
import org.apache.calcite.sql.SqlCreateProcedure;
import org.apache.calcite.sql.SqlCreateSchedule;
import org.apache.calcite.sql.SqlCreateSequence;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateTableGroup;
import org.apache.calcite.sql.SqlCreateTrigger;
import org.apache.calcite.sql.SqlCreateView;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDesc;
import org.apache.calcite.sql.SqlDropCclRule;
import org.apache.calcite.sql.SqlDropCclTrigger;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlDropFileStorage;
import org.apache.calcite.sql.SqlDropFunction;
import org.apache.calcite.sql.SqlDropFunction;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDropPrimaryKey;
import org.apache.calcite.sql.SqlDropProcedure;
import org.apache.calcite.sql.SqlDropSchedule;
import org.apache.calcite.sql.SqlDropSequence;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlDropTableGroup;
import org.apache.calcite.sql.SqlDropTrigger;
import org.apache.calcite.sql.SqlDropView;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlEnableKeys;
import org.apache.calcite.sql.SqlEnableKeys.EnableType;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFireSchedule;
import org.apache.calcite.sql.SqlFlashbackTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFlashbackTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexType;
import org.apache.calcite.sql.SqlIndexHintOperator;
import org.apache.calcite.sql.SqlIndexOption;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlInspectDdlJobCache;
import org.apache.calcite.sql.SqlInspectGroupSeqRange;
import org.apache.calcite.sql.SqlInspectRuleVersion;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKill;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlLockTable;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlMoveDatabase;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOptimizeTableDdl;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionByHash;
import org.apache.calcite.sql.SqlPartitionByList;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlPauseDdlJob;
import org.apache.calcite.sql.SqlPauseSchedule;
import org.apache.calcite.sql.SqlPurge;
import org.apache.calcite.sql.SqlPushDownUdf;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.calcite.sql.SqlRecoverDdlJob;
import org.apache.calcite.sql.SqlReferenceDefinition;
import org.apache.calcite.sql.SqlReferenceDefinition.MatchType;
import org.apache.calcite.sql.SqlReferenceOption;
import org.apache.calcite.sql.SqlReferenceOption.OnType;
import org.apache.calcite.sql.SqlReferenceOption.ReferenceOptionType;
import org.apache.calcite.sql.SqlRefreshLocalRules;
import org.apache.calcite.sql.SqlRefreshTopology;
import org.apache.calcite.sql.SqlRemoveDdlJob;
import org.apache.calcite.sql.SqlRenameSequence;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlRollbackDdlJob;
import org.apache.calcite.sql.SqlSavepoint;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSet;
import org.apache.calcite.sql.SqlSetCharacterSet;
import org.apache.calcite.sql.SqlSetDefaultRole;
import org.apache.calcite.sql.SqlSetNames;
import org.apache.calcite.sql.SqlSetRole;
import org.apache.calcite.sql.SqlSetTransaction;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowAuthors;
import org.apache.calcite.sql.SqlShowBinaryLogs;
import org.apache.calcite.sql.SqlShowBinlogEvents;
import org.apache.calcite.sql.SqlShowBroadcasts;
import org.apache.calcite.sql.SqlShowCclRule;
import org.apache.calcite.sql.SqlShowCclTrigger;
import org.apache.calcite.sql.SqlShowCreateDatabase;
import org.apache.calcite.sql.SqlShowCreateFunction;
import org.apache.calcite.sql.SqlShowCreateProcedure;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlShowCreateView;
import org.apache.calcite.sql.SqlShowDS;
import org.apache.calcite.sql.SqlShowDatasources;
import org.apache.calcite.sql.SqlShowDbStatus;
import org.apache.calcite.sql.SqlShowDdlJobs;
import org.apache.calcite.sql.SqlShowDdlResults;
import org.apache.calcite.sql.SqlShowDdlStatus;
import org.apache.calcite.sql.SqlShowFiles;
import org.apache.calcite.sql.SqlShowFunctionStatus;
import org.apache.calcite.sql.SqlShowGlobalDeadlocks;
import org.apache.calcite.sql.SqlShowGlobalIndex;
import org.apache.calcite.sql.SqlShowGrants;
import org.apache.calcite.sql.SqlShowGrantsLegacy;
import org.apache.calcite.sql.SqlShowHtc;
import org.apache.calcite.sql.SqlShowIndex;
import org.apache.calcite.sql.SqlShowLocalDeadlocks;
import org.apache.calcite.sql.SqlShowMasterStatus;
import org.apache.calcite.sql.SqlShowMetadataLock;
import org.apache.calcite.sql.SqlShowPartitions;
import org.apache.calcite.sql.SqlShowProcedureStatus;
import org.apache.calcite.sql.SqlShowProcesslist;
import org.apache.calcite.sql.SqlShowProfile;
import org.apache.calcite.sql.SqlShowRebalanceBackFill;
import org.apache.calcite.sql.SqlShowRecyclebin;
import org.apache.calcite.sql.SqlShowRule;
import org.apache.calcite.sql.SqlShowScheduleResults;
import org.apache.calcite.sql.SqlShowSequences;
import org.apache.calcite.sql.SqlShowSlow;
import org.apache.calcite.sql.SqlShowStats;
import org.apache.calcite.sql.SqlShowStc;
import org.apache.calcite.sql.SqlShowTableInfo;
import org.apache.calcite.sql.SqlShowTableStatus;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.SqlShowTopology;
import org.apache.calcite.sql.SqlShowTrace;
import org.apache.calcite.sql.SqlShowTrans;
import org.apache.calcite.sql.SqlShowVariables;
import org.apache.calcite.sql.SqlSlowSqlCcl;
import org.apache.calcite.sql.SqlSpecialIdentifier;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.SqlSubPartitionBy;
import org.apache.calcite.sql.SqlSubPartitionByHash;
import org.apache.calcite.sql.SqlSystemVar;
import org.apache.calcite.sql.SqlTableOptions;
import org.apache.calcite.sql.SqlTableOptions.PackKeys;
import org.apache.calcite.sql.SqlTableOptions.RowFormat;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.SqlUnArchive;
import org.apache.calcite.sql.SqlUnlockTable;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUserDefVar;
import org.apache.calcite.sql.SqlUserName;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.VariableScope;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.DEFAULT_EXPIRE_AFTER;
import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.DEFAULT_PRE_ALLOCATE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.NOW_FUNCTION;
import static com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator.Escape;
import static com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator.SubGt;
import static com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator.SubGtGt;
import static com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement.Type.GLOBAL_TEMPORARY;
import static com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement.Type.LOCAL_TEMPORARY;
import static com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement.Type.TEMPORARY;

/**
 * 提供Fast SQL Parser AST到Calcite AST的转换
 *
 * @author hongxi.chx on 2017/11/21.
 * @since 6.0.0
 */
public class FastSqlToCalciteNodeVisitor extends CalciteVisitor implements MySqlASTVisitor {
    private static final BigInteger MAX_UNSIGNED_INT64 = new BigInteger(Long.toUnsignedString(0xffffffffffffffffL));

    private static final BigInteger MIN_SIGNED_INT64 = new BigInteger(Long.toString(-0x7fffffffffffffffL - 1));

    private static final BigInteger MAX_SIGNED_INT64 = BigInteger.valueOf(Long.MAX_VALUE);

    private SqlNode sqlNode = null;
    private ContextParameters context;
    private boolean top = true;
    private Map<String, String> tb2TestNames;
    private ExecutionContext ec;
    private Map<SQLVariantRefExpr, ColumnMeta> bindMapTypes;

    public FastSqlToCalciteNodeVisitor(
        ContextParameters context, ExecutionContext ec) {
        this.context = context;
        this.tb2TestNames = context != null ? context.getTb2TestNames() : null;
        this.ec = ec;
        this.bindMapTypes = context != null ?
            (Map<SQLVariantRefExpr, ColumnMeta>) context.getParameter(ContextParameterKey.BIND_TYPE_PARAMS) : null;
    }

    @Override
    public boolean visit(MySqlOutFileExpr x) {
        return true;
    }

    public boolean visit(SQLPurgeTableStatement stmt) {
        SqlIdentifier name = (SqlIdentifier) (convertToSqlNode(stmt.getTable()));
        this.sqlNode = new SqlPurge(SqlParserPos.ZERO, name);
        return false;
    }

    public boolean visit(SQLPurgeRecyclebinStatement stmt) {
        this.sqlNode = new SqlPurge(SqlParserPos.ZERO, null);
        return false;
    }

    public boolean visit(MySqlFlashbackStatement stmt) {
        SqlIdentifier name = (SqlIdentifier) convertToSqlNode(stmt.getName());
        SqlIdentifier renameTo = null;
        if (stmt.getRenameTo() != null) {
            renameTo = (SqlIdentifier) convertToSqlNode(stmt.getRenameTo());
        }
        this.sqlNode = new SqlFlashbackTable(SqlParserPos.ZERO, name, renameTo);
        return false;
    }

    public boolean visit(SQLSelectStatement stmt) {
        SQLSelect select = stmt.getSelect();
        this.visit(select);
        return false;
    }

    public boolean visit(MySqlExplainStatement var1) {
        if (var1.isDescribe() && null != var1.getTableName()) {
            this.sqlNode = new SqlDesc(SqlParserPos.ZERO, convertToSqlNode(var1.getTableName()));
        } else {

            final SqlExplainLevel detailLevel = SqlExplainLevel.NO_ATTRIBUTES;
            final SqlExplain.Depth depth = SqlExplain.Depth.TYPE;
            final SqlExplainFormat format = SqlExplainFormat.TEXT;
            SqlExplain sqlExplain = new SqlExplain(SqlParserPos.ZERO,
                convertToSqlNode(var1.getStatement()),
                detailLevel.symbol(SqlParserPos.ZERO),
                depth.symbol(SqlParserPos.ZERO),
                format.symbol(SqlParserPos.ZERO),
                0);
            List<SQLCommentHint> oriHints = var1.getHints();
            if (null == oriHints) {
                oriHints = var1.getHeadHintsDirect();
            }

            SqlNodeList hints = FastSqlConstructUtils.convertHints(oriHints, context, ec);
            if (null != hints) {
                sqlExplain.setHints(hints);
            }
            sqlExplain.setSimpleExplain(true);
            sqlExplain.setType(var1.getType());

            this.sqlNode = sqlExplain;
        }

        return false;
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        // if
        // (context.contextParameters.containsKey(ContextParameterKey.DO_HAVING))
        // {
        // throw new
        // FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
        // "Having does not support subqueries");
        // }
        List<SQLCommentHint> oriHints = x.getHints();
        if (top && (null == oriHints || oriHints.size() <= 0) && null != context.getHeadHints()) {
            oriHints = context.getHeadHints();
        }
        top = false;

        // window
        List<SQLWindow> windows = x.getWindows();
        context.putParameter(ContextParameterKey.WINDOW, windows);
        //
        SqlNodeList keywordList = FastSqlConstructUtils.constructKeywords(x, context);
        // select
        SqlNodeList selectList = FastSqlConstructUtils.constructSelectList(x.getSelectList(), context, ec);

        // from
        SqlNode fromCalcite = FastSqlConstructUtils.constructFrom(x.getFrom(), context, ec);

        // into file
        OutFileParams outFileParams = FastSqlConstructUtils.constructOutFile(x.getInto(), context, ec);

        // 如果有outFile字段，则不应该走planCache，借用hint标记以达到相似的效果
        if (outFileParams != null) {
            ec.setUseHint(true);
        }

        // where
        SqlNode whereCalcite = FastSqlConstructUtils.constructWhere(x.getWhere(), context, ec);

        // groupBy
        SQLSelectGroupByClause groupBy = x.getGroupBy();

        SqlNodeList groupBySqlNode = FastSqlConstructUtils.constructGroupBy(groupBy, context, ec);

        SqlNode having = FastSqlConstructUtils.constructHaving(groupBy, context, ec);

        // orderBy
        SqlNodeList orderBySqlNode = FastSqlConstructUtils.constructOrderBy(x.getOrderBy(), context, ec);

        // limit
        SqlNodeList limitNodes = FastSqlConstructUtils.constructLimit(x.getLimit(), context, ec);

        // for update
        // SqlNode forUpdate =
        // FastSqlConstructUtils.constructForUpdate(x.lockMode() ,context);

        checkSubqueryIn(groupBySqlNode, orderBySqlNode, limitNodes);

        SqlNodeList hints = FastSqlConstructUtils.convertHints(oriHints, context, ec);

        SqlNode offset = null;
        SqlNode limit = null;
        if (limitNodes != null) {
            offset = limitNodes.get(0);
            limit = limitNodes.get(1);
        }

        LockMode lockMode = LockMode.UNDEF;
        if (x.isForUpdate()) {
            lockMode = LockMode.EXCLUSIVE_LOCK;
        } else if (x.isLockInShareMode()) {
            lockMode = LockMode.SHARED_LOCK;
        }

        final TDDLSqlSelect select = new TDDLSqlSelect(SqlParserPos.ZERO,
            keywordList,
            selectList,
            fromCalcite,
            whereCalcite,
            groupBySqlNode,
            having,
            null,
            orderBySqlNode,
            offset,
            limit,
            hints,
            lockMode,
            outFileParams);
        final OptimizerHint optimizerHint =
            FastSqlConstructUtils.convertHints(x.getHints(), new OptimizerHint());
        for (String hint : optimizerHint.getHints()) {
            select.getOptimizerHint().addHint(hint);
        }
        this.sqlNode = select;

        // fast mock wont process privilege verify
        if (!ConfigDataMode.isFastMock() && x.getFrom() != null && x.getFrom().getClass() == SQLExprTableSource.class) {
            if (null == ((SQLExprTableSource) x.getFrom()).getName()) {
                throw new InvalidParameterException(
                    "You have an error in your SQL syntax; maybe miss table reference.");
            }
            String tableName = ((SQLExprTableSource) x.getFrom()).getName().getSimpleName();
            String dbName = ((SQLExprTableSource) x.getFrom()).getSchema();
            addPrivilegeVerifyItem(dbName, tableName, PrivilegePoint.SELECT);

            if (x.isForUpdate()) {
                // FOR UPDATE 暂时不需要 UPDATE 权限。
            }
        }

        return false;
    }

    private void checkSubqueryIn(SqlNodeList groupBySqlNode, SqlNodeList orderBySqlNode, SqlNodeList limitNodes) {
        if (hasSubquery(groupBySqlNode) || hasSubquery(orderBySqlNode) || hasSubquery(limitNodes)) {
            throw new NotSupportException("subuqery in group by/order by/limit ");
        }
    }

    public boolean visit(MySqlInsertStatement x) {
        if (x.getPartitions() != null && !x.getPartitions().isEmpty()) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Do not support table with mysql partition.");
        }
        SqlNodeList keywords = FastSqlConstructUtils.constructKeywords(x);
        SQLExprTableSource tableSource = x.getTableSource();

        SqlNode targetTable = convertToSqlNode(tableSource.getExpr());
        if (context.isTestMode() && tableSource.getTableName() != null) {
            String tableName = EagleeyeHelper.rebuildTableName(tableSource.getTableName(), true);
            if (tb2TestNames != null) {
                tb2TestNames.put(tableSource.getTableName(), tableName);
            }

            targetTable = ((SqlIdentifier) targetTable)
                .setName(((SqlIdentifier) targetTable).names.size() - 1, tableName);
        }

        List<SQLExpr> columns = x.getColumns();
        List<SqlNode> nodes = new ArrayList<SqlNode>(columns.size());
        for (int i = 0; i < columns.size(); ++i) {
            SQLExpr expr = columns.get(i);
            SqlNode node = convertToSqlNode(expr);
            nodes.add(node);
        }
        SqlNodeList columnList = null;
        if (nodes.size() > 0) {
            columnList = new SqlNodeList(nodes, SqlParserPos.ZERO);
        }

        SqlNode source = null;
        List<ValuesClause> valuesList = x.getValuesList();
        SQLSelect query = x.getQuery();

        // handle hints
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);

        if (null != x.getHints() && x.getHints().size() > 0) {
            hints = FastSqlConstructUtils.convertHints(x.getHints(), this.context, ec);
        } else if (null != x.getHeadHintsDirect()) {
            hints = FastSqlConstructUtils.convertHints(x.getHeadHintsDirect(), this.context, ec);
        }

        final HintCollection hintCollection =
            HintUtil.collectHint(hints, new HintCollection(), context.isTestMode(), ec);

        int batchSize = 0;
        if (valuesList != null && valuesList.size() > 0) {
            if (visit(valuesList, columns.size(), hintCollection)) {
                batchSize = valuesList.size();
            }
            source = this.sqlNode;
        } else if (query != null) {
            source = convertToSqlNode(query);
        } else {
            throw new InvalidParameterException(
                "You have an error in your SQL syntax;maybe miss values or query expression.");
        }

        List<SQLExpr> duplicateUpdateList = x.getDuplicateKeyUpdate();
        SqlNodeList updateList = new SqlNodeList(SqlParserPos.ZERO);
        for (SQLExpr updateExpr : duplicateUpdateList) {
            updateList.add(convertToSqlNode(updateExpr));
        }

        this.sqlNode = new SqlInsert(SqlParserPos.ZERO,
            keywords,
            targetTable,
            source,
            columnList,
            updateList,
            batchSize,
            hints);

        addPrivilegeVerifyItem(tableSource.getSchema(), tableSource.getName().getSimpleName(), PrivilegePoint.INSERT);

        return false;
    }

    public boolean visit(SQLReplaceStatement x) {
        if (x.getPartitions() != null && !x.getPartitions().isEmpty()) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Do not support table with mysql partition.");
        }
        SqlNodeList keywords = FastSqlConstructUtils.constructKeywords(x);
        SQLExprTableSource tableSource = x.getTableSource();
        SqlNode targetTable = convertToSqlNode(tableSource.getExpr());

        if (context.isTestMode() && tableSource.getTableName() != null) {
            String tableName = EagleeyeHelper.rebuildTableName(tableSource.getTableName(), true);
            if (tb2TestNames != null) {
                tb2TestNames.put(tableSource.getTableName(), tableName);
            }
            targetTable = ((SqlIdentifier) targetTable)
                .setName(((SqlIdentifier) targetTable).names.size() - 1, tableName);
        }

        List<SQLExpr> columns = x.getColumns();
        List<SqlNode> nodes = new ArrayList<SqlNode>(columns.size());
        for (int i = 0; i < columns.size(); ++i) {
            SQLExpr expr = columns.get(i);
            SqlNode node = convertToSqlNode(expr);
            nodes.add(node);
        }
        SqlNodeList columnList = null;
        if (nodes.size() > 0) {
            columnList = new SqlNodeList(nodes, SqlParserPos.ZERO);
        }

        SqlNode source = null;
        List<ValuesClause> valuesList = x.getValuesList();
        SQLQueryExpr query = x.getQuery();

        // handle hints
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (null != x.getHints() && x.getHints().size() > 0) {
            hints = FastSqlConstructUtils.convertHints(x.getHints(), this.context, ec);
        } else if (null != x.getHeadHintsDirect()) {
            hints = FastSqlConstructUtils.convertHints(x.getHeadHintsDirect(), this.context, ec);
        }

        final HintCollection hintCollection =
            HintUtil.collectHint(hints, new HintCollection(), context.isTestMode(), ec);

        int batchSize = 0;
        if (valuesList != null && valuesList.size() > 0) {
            if (visit(valuesList, columns.size(), hintCollection)) {
                batchSize = valuesList.size();
            }
            source = this.sqlNode;
        } else if (query != null) {
            source = convertToSqlNode(query);
        }

        this.sqlNode = new SqlReplace(SqlParserPos.ZERO, keywords, targetTable, source, columnList, batchSize, hints);

        addPrivilegeVerifyItem(tableSource.getSchema(), tableSource.getName().getSimpleName(), PrivilegePoint.INSERT);
        addPrivilegeVerifyItem(tableSource.getSchema(), tableSource.getName().getSimpleName(), PrivilegePoint.DELETE);

        return false;
    }

    private boolean visit(List<ValuesClause> valuesList, int columnCount, HintCollection hintCollection) {
        boolean isBatch = false;
        // If it's using hint or in prepare mode, don't optimize.
        if (!hintCollection.pushdownOriginSql() && !context.isPrepareMode()) {
            List<ValuesClause> newValuesList =
                FastSqlConstructUtils.convertToSingleValuesIfNeed(valuesList, columnCount, context);
            if (newValuesList.size() < valuesList.size()) {
                isBatch = true;
                valuesList = newValuesList;
            }
        }

        SqlNode[] rows = new SqlNode[valuesList.size()];
        for (int j = 0; j < valuesList.size(); j++) {

            List<SQLExpr> values = valuesList.get(j).getValues();

            SqlNode[] valueNodes = new SqlNode[values.size()];
            for (int i = 0; i < values.size(); i++) {
                SqlNode valueNode = convertToSqlNode(values.get(i));
                valueNodes[i] = valueNode;
            }
            SqlBasicCall row = new SqlBasicCall(SqlStdOperatorTable.ROW, valueNodes, SqlParserPos.ZERO);
            rows[j] = row;
        }

        this.sqlNode = new SqlBasicCall(SqlStdOperatorTable.VALUES, rows, SqlParserPos.ZERO);
        return isBatch;
    }

    @Override
    public boolean visit(MySqlUpdateStatement x) {

        if (x.isIgnore()) {
            throw new FastSqlParserException(ExceptionType.NOT_SUPPORT,
                "Do not support update with ignore");
        }
        SqlNodeList keywords = FastSqlConstructUtils.constructKeywords(x);
        final SQLTableSource tableSource = x.getTableSource();

        SqlNode targetTable = null;
        SqlIdentifier alias = null;
        if (context.isTestMode()) {
            if (tableSource.getAlias() == null && tableSource instanceof SQLExprTableSource) {
                targetTable = convertToSqlNode(((SQLExprTableSource) tableSource).getExpr());
                String tableName = EagleeyeHelper.rebuildTableName(
                    ((SQLExprTableSource) tableSource).getTableName(), true);
                if (tb2TestNames != null) {
                    tb2TestNames.put(((SQLExprTableSource) tableSource).getTableName(), tableName);
                }

                targetTable = ((SqlIdentifier) targetTable)
                    .setName(((SqlIdentifier) targetTable).names.size() - 1, tableName);

                alias = new SqlIdentifier(SQLUtils.normalizeNoTrim(
                    ((SQLExprTableSource) tableSource).getTableName()), SqlParserPos.ZERO);

            } else {
                targetTable = FastSqlConstructUtils.constructFrom(tableSource, context, ec);
                if ((tableSource instanceof SQLExprTableSource) && (tableSource.getAlias() != null)) {
                    targetTable = ((SqlBasicCall) targetTable).operands[0];
                    alias = new SqlIdentifier(SQLUtils.normalizeNoTrim(tableSource.getAlias()), SqlParserPos.ZERO);
                }
            }
        } else {
            targetTable = FastSqlConstructUtils.constructFrom(tableSource, context, ec);

//            if (tableSource instanceof SQLExprTableSource) {
//                if (((SQLExprTableSource) tableSource).getPartitions() != null && !((SQLExprTableSource) tableSource.getPartitions()
//                    .isEmpty()) {
//                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
//                        "Do not support table with mysql partition.");
//                }
//            }

            if ((tableSource instanceof SQLExprTableSource) && (tableSource.getAlias() != null)) {

                targetTable = ((SqlBasicCall) targetTable).operands[0];
                alias = new SqlIdentifier(SQLUtils.normalizeNoTrim(tableSource.getAlias()), SqlParserPos.ZERO);
            }
        }

        List<SqlNode> columns = new ArrayList<>();
        List<SqlNode> values = new ArrayList<>();
        for (SQLUpdateSetItem item : x.getItems()) {
            columns.add(convertToSqlNode(item.getColumn()));
            values.add(convertToSqlNode(item.getValue()));
        }

        SqlNodeList targetColumnList = new SqlNodeList(columns, SqlParserPos.ZERO);
        SqlNodeList sourceExpressList = new SqlNodeList(values, SqlParserPos.ZERO);

        SqlNode condition = convertToSqlNode(x.getWhere());

        // orderBy
        SqlNodeList orderBySqlNode = FastSqlConstructUtils.constructOrderBy(x.getOrderBy(), context, ec);

        // limit
        SqlNodeList limitNodes = FastSqlConstructUtils.constructLimit(x.getLimit(), context, ec);
        SqlNode limit = FastSqlConstructUtils.getLimitForUpdateOrDelete(limitNodes);

        // handle hints
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);

        if (null != x.getHints() && x.getHints().size() > 0) {
            hints = FastSqlConstructUtils.convertHints(x.getHints(), this.context, ec);
        } else if (null != x.getHeadHintsDirect()) {
            hints = FastSqlConstructUtils.convertHints(x.getHeadHintsDirect(), this.context, ec);
        }
        final OptimizerHint hintContext =
            FastSqlConstructUtils.convertHints(x.getHints(), new OptimizerHint());
        this.sqlNode = FastSqlConstructUtils.constructUpdate(keywords,
            targetTable,
            targetColumnList,
            sourceExpressList,
            condition,
            alias,
            orderBySqlNode,
            limit,
            hints,
            hintContext, ec);

        for (SQLUpdateSetItem item : x.getItems()) {
            if (item.getColumn() instanceof SQLIdentifierExpr) {
                // update tb1 set value = 1 where id = 1;
                if (tableSource instanceof SQLExprTableSource) {
                    addPrivilegeVerifyItem(((SQLExprTableSource) tableSource).getSchema(),
                        ((SQLExprTableSource) tableSource).getName().getSimpleName(),
                        PrivilegePoint.UPDATE);
                }
                break;
            } else if (item.getColumn() instanceof SQLPropertyExpr) {
                SQLPropertyExpr column = (SQLPropertyExpr) item.getColumn();
                if (column.getOwner() instanceof SQLPropertyExpr) {
                    // update db.tb1 set db.tb1.value = 1 where id = 1;
                    SQLPropertyExpr owner = (SQLPropertyExpr) column.getOwner();
                    addPrivilegeVerifyItem(owner.getOwnernName(), owner.getName(), PrivilegePoint.UPDATE);
                } else if (column.getOwner() instanceof SQLIdentifierExpr) {
                    // update db.tb1 set tb1.value = 1 where id = 1;
                    // update tb1 set tb1.value = 1 where id = 1;
                    // update tb1 a set a.value = 1 where a.id = 1;
                    String tableAlias = ((SQLIdentifierExpr) column.getOwner()).getName();
                    String tableName = tableAlias;
                    String dbName = "";
                    if (tableSource != null && tableSource.containsAlias(tableAlias)) {
                        SQLTableSource targetTableSource = tableSource.findTableSource(tableAlias);
                        if (targetTableSource instanceof SQLExprTableSource) {
                            tableName = ((SQLExprTableSource) targetTableSource).getTableName();
                            dbName = ((SQLExprTableSource) targetTableSource).getSchema();
                        }
                    }
                    addPrivilegeVerifyItem(dbName, tableName, PrivilegePoint.UPDATE);
                }
            }
        }

        return false;
    }

    private List<SqlNode> getDeleteTargetTables(final SQLTableSource x, List<SqlNode> targetTables, boolean exitFrom,
                                                boolean existsUsing) {
        final boolean existsFromOrUsing = exitFrom || existsUsing;
        final boolean tableSourceWithAlias = null != x.getAlias();

        if (x instanceof SQLExprTableSource) {
            boolean containPartitions = false;
            SqlNodeList partitions = null;
            if (((SQLExprTableSource) x).getPartitionSize() > 0) {
//                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
//                    "Do not support table with mysql partition.");
                containPartitions = true;
                List<SQLName> partNames = ((SQLExprTableSource) x).getPartitions();
                List<SqlNode> partNameList = new ArrayList<>();
                for (int i = 0; i < partNames.size(); i++) {
                    SQLName partName = partNames.get(i);
                    SqlNode partNameId = convertToSqlNode(partName);
                    partNameList.add(partNameId);
                }
                partitions = new SqlNodeList(partNameList, SqlParserPos.ZERO);
            }

            SqlIdentifier targetTable = null;
            if (tableSourceWithAlias) {
                if (existsFromOrUsing) {
                    // Syntax error like 'DELETE t1 AS a FROM t1'
                    throw new TddlNestableRuntimeException(
                        "You have an error in your SQL syntax; Error occurs around this fragment: "
                            + x.toString());
                } else {
                    // DELETE FROM t1 AS a
                    final String trimmed = SQLUtils.normalizeNoTrim(x.getAlias());
                    targetTable = new SqlIdentifier(trimmed, SqlParserPos.ZERO);
                }
            } else {
                // DELETE FROM t1
                // DELETE a FROM t1 AS a
                // DELETE FROM a USING t1 AS a
                // DELETE a FROM t1 AS a JOIN t1 AS b
                // DELETE FROM a USING t1 AS a JOIN t1 AS b
                targetTable = (SqlIdentifier) convertToSqlNode(((SQLExprTableSource) x).getExpr());
            }

            if (targetTable.isStar()) {
                // remove star
                targetTable = new SqlIdentifier(targetTable.names.subList(0, targetTable.names.size() - 1),
                    targetTable.getParserPosition());
            }

            if (containPartitions) {
                targetTable.partitions = partitions;
            }

            targetTables.add(targetTable);
        } else if (x instanceof SQLJoinTableSource) {
            // delete from multi table
            final SQLJoinTableSource joinTableSource = (SQLJoinTableSource) x;

            if (null != joinTableSource.getLeft().getAlias() || null != joinTableSource.getRight().getAlias()) {
                // Syntax error like 'DELETE t1 AS a FROM t1 AS a JOIN t1 AS b'
                throw new TddlNestableRuntimeException(
                    "You have an error in your SQL syntax; Error occurs around this fragment: "
                        + x.toString());
            }
            getDeleteTargetTables(joinTableSource.getLeft(), targetTables, exitFrom, existsUsing);
            getDeleteTargetTables(joinTableSource.getRight(), targetTables, exitFrom, existsUsing);
        }
        return targetTables;
    }

    public boolean visit(MySqlDeleteStatement x) {
        SqlNodeList keywords = FastSqlConstructUtils.constructKeywords(x);

        final boolean existsFrom = null != x.getFrom();
        final boolean existsUsing = null != x.getUsing();
        final SQLTableSource target = x.getTableSource();
        final SQLTableSource source = existsUsing ? x.getUsing() : (existsFrom ? x.getFrom() : target);

        final List<SqlNode> targetTables = getDeleteTargetTables(target, new LinkedList<>(), existsFrom, existsUsing);
        final SqlNode targetTable = targetTables.get(0);

        // DELETE FROM t1
        // DELETE FROM t1 AS a
        // /* //1/ */DELETE FROM t1
        // /* //1/ */DELETE FROM t1 AS a
        final boolean singleTableDelete =
            !existsFrom && !existsUsing && (null != target.getAlias() || context.isTestMode());

        SqlNode from = null;
        SqlNode using = null;
        SqlIdentifier alias = null;
        if (singleTableDelete) {
            // DELETE FROM t1 AS a
            // Convert to
            // DELETE a FROM t1 AS a
            from = convertToSqlNode(source);
            assert from != null;
            alias = ((SqlCall) from).operand(1);
        } else {
            using = convertToSqlNode(x.getUsing());
            from = convertToSqlNode(x.getFrom());
        }

        // where
        SqlNode condition = convertToSqlNode(x.getWhere());

        // orderBy
        SqlNodeList orderBySqlNode = FastSqlConstructUtils.constructOrderBy(x.getOrderBy(), context, ec);

        // limit
        SqlNodeList limitNodes = FastSqlConstructUtils.constructLimit(x.getLimit(), context, ec);
        SqlNode limit = FastSqlConstructUtils.getLimitForUpdateOrDelete(limitNodes);

        // handle hints
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);

        if (null != x.getHints() && x.getHints().size() > 0) {
            hints = FastSqlConstructUtils.convertHints(x.getHints(), this.context, ec);
        } else if (null != x.getHeadHintsDirect()) {
            hints = FastSqlConstructUtils.convertHints(x.getHeadHintsDirect(), this.context, ec);
        }

        this.sqlNode = FastSqlConstructUtils.constructDelete(keywords,
            targetTables,
            targetTable,
            from,
            using,
            alias,
            condition,
            orderBySqlNode,
            limit,
            hints, ec);

        for (SqlNode tt : targetTables) {
            String tableAlias = RelUtils.lastStringValue(tt);
            String tableName = tableAlias;
            String dbName = "";

            // for: delete from schema.table where a.id = 1
            if (tt instanceof SqlIdentifier) {
                ImmutableList<String> names = ((SqlIdentifier) tt).names;
                if (names != null && names.size() > 1) {
                    dbName = names.get(0);
                }
            }

            // for: delete a.*,b.* from app_1_table a, dml_table_1 b where a.id
            // = 1
            if (source != null && source.containsAlias(tableAlias)) {
                SQLTableSource targetTableSource = source.findTableSource(tableAlias);
                if (targetTableSource instanceof SQLExprTableSource) {
                    tableName = ((SQLExprTableSource) targetTableSource).getTableName();
                    dbName = ((SQLExprTableSource) targetTableSource).getSchema();
                }
            }
            addPrivilegeVerifyItem(dbName, tableName, PrivilegePoint.DELETE);
        }

        return false;
    }

    public boolean visit(SQLDropViewStatement x) {
        this.sqlNode = new SqlDropView((SqlIdentifier) (convertToSqlNode(x.getTableSources().get(0))), x.isIfExists());
        String viewName = x.getTableSources().get(0).getTableName();
        String dbName = x.getTableSources().get(0).getSchema();
        addPrivilegeVerifyItem(dbName, viewName, PrivilegePoint.DROP);
        return false;
    }

    public boolean visit(SQLAlterViewStatement x) {
        SQLSelect subQuery = x.getSubQuery();
        if (subQuery == null) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "select is empty");
        }

        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        final List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        SqlNode sqlNode = convertToSqlNode(subQuery);
        SQLExprTableSource tableSource = x.getTableSource();
        SqlNode tableSourceSqlNode = convertToSqlNode(tableSource);
        SqlNodeList sqlNodeList = null;
        if (x.getColumns() != null) {
            sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
            for (int i = 0; i < x.getColumns().size(); i++) {
                SQLTableElement sqlTableElement = x.getColumns().get(i);
                sqlNodeList.add(new SqlIdentifier(sqlTableElement.toString(), SqlParserPos.ZERO));
            }
        }
        SqlCreateView sqlCreateView = new SqlCreateView(SqlParserPos.ZERO,
            true,
            (SqlIdentifier) tableSourceSqlNode,
            sqlNodeList,
            sqlNode);
        sqlCreateView.setHints(hints);
        this.sqlNode = sqlCreateView;

        if (tableSource != null) {
            addPrivilegeVerifyItem(tableSource.getSchema(),
                tableSource.getName().getSimpleName(),
                PrivilegePoint.CREATE_VIEW);
            addPrivilegeVerifyItem(tableSource.getSchema(),
                tableSource.getName().getSimpleName(),
                PrivilegePoint.DROP);
        }

        return false;
    }

    public boolean visit(SQLCreateViewStatement x) {
        SQLSelect subQuery = x.getSubQuery();
        if (subQuery == null) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "select is empty");
        }

        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        final List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        SqlNode sqlNode = convertToSqlNode(subQuery);
        SQLExprTableSource tableSource = x.getTableSource();
        SqlNode tableSourceSqlNode = convertToSqlNode(tableSource);
        SqlNodeList sqlNodeList = null;
        if (x.getColumns() != null) {
            sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
            for (int i = 0; i < x.getColumns().size(); i++) {
                SQLTableElement sqlTableElement = x.getColumns().get(i);
                sqlNodeList.add(new SqlIdentifier(sqlTableElement.toString(), SqlParserPos.ZERO));
            }
        }
        SqlCreateView sqlCreateView = new SqlCreateView(SqlParserPos.ZERO,
            x.isOrReplace(),
            (SqlIdentifier) tableSourceSqlNode,
            sqlNodeList,
            sqlNode);
        sqlCreateView.setHints(hints);
        this.sqlNode = sqlCreateView;

        if (tableSource != null) {
            addPrivilegeVerifyItem(tableSource.getSchema(),
                tableSource.getName().getSimpleName(),
                PrivilegePoint.CREATE_VIEW);
            if (x.isOrReplace()) {
                addPrivilegeVerifyItem(tableSource.getSchema(),
                    tableSource.getName().getSimpleName(),
                    PrivilegePoint.DROP);
            }
        }

        return false;
    }

    /**
     * 解析建表语句中的热点映射规则
     */
    private List<MappingRule> getMappingRules(MySqlExtPartition extPartition, SqlNode dbPartitionBy,
                                              SqlNode tbPartitionBy, boolean needValidate) {
        List<MappingRule> mappingRuleList = new ArrayList<>();
        if (extPartition != null && extPartition.getItems() != null && extPartition.getItems().size() > 0) {
            if (needValidate && dbPartitionBy == null) {
                throw new TddlNestableRuntimeException("ExtPartition is not allowed when DbPartitionBy not exist");
            }

            for (MySqlExtPartition.Item item : extPartition.getItems()) {
                // extDbPartitionBy would never be null
                SqlBasicCall extDbPartitionBy = (SqlBasicCall) convertToSqlNode(item.getDbPartitionBy());
                SqlBasicCall extTbPartitionBy = (SqlBasicCall) convertToSqlNode(item.getTbPartitionBy());
                final String dbKeyValue;
                if (extDbPartitionBy.getOperandList().get(0) instanceof SqlCharStringLiteral) {
                    dbKeyValue =
                        ((SqlCharStringLiteral) extDbPartitionBy.getOperandList().get(0)).getNlsString().getValue();
                } else {
                    // Note: normalize only work for identifier, not string.
                    dbKeyValue = SQLUtils.normalizeNoTrim(extDbPartitionBy.getOperandList().get(0).toString());
                }
                MappingRule mappingRule = new MappingRule(dbKeyValue, "", item.getDbPartition().getSimpleName(), "");

                // Currently do not allow this since fastsql failed to remove \
                // from \" during parsing
                if (dbKeyValue.contains("\"")) {
                    throw new TddlNestableRuntimeException("\" is not supported as a value for dbpartition key");
                }

                if (extTbPartitionBy != null) {
                    final String tbKeyValue;
                    if (extTbPartitionBy.getOperandList().get(0) instanceof SqlCharStringLiteral) {
                        tbKeyValue =
                            ((SqlCharStringLiteral) extTbPartitionBy.getOperandList().get(0)).getNlsString().getValue();
                    } else {
                        // Note: normalize only work for identifier, not string.
                        tbKeyValue = SQLUtils.normalizeNoTrim(extTbPartitionBy.getOperandList().get(0).toString());
                    }

                    if (tbPartitionBy != null) {
                        if (needValidate && dbPartitionBy.toString().equalsIgnoreCase(tbPartitionBy.toString())
                            && !dbKeyValue.equals(tbKeyValue)) {
                            throw new TddlNestableRuntimeException(
                                "ExtDbPartition key should be the same with ExTbPartition key when DbPartitionBy is the same with TbPartitionBy");
                        }

                        if (tbKeyValue.contains("\"")) {
                            throw new TddlNestableRuntimeException(
                                "\" is not supported as a value for tbpartition key");
                        }
                    }
                    mappingRule.setTb(item.getTbPartition().getSimpleName());
                    mappingRule.setTbKeyValue(tbKeyValue);
                }
                mappingRuleList.add(mappingRule);
            }
        }

        return mappingRuleList;
    }

    public boolean visit(SQLDropTableStatement x) {
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);

        if (x.isTemporary()) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Do not support temporary table.");
        }

        if (x.getTableSources().size() > 1) {
            throw new TddlNestableRuntimeException("Only support DROP one table.");
        }

        SQLExprTableSource tableSource = x.getTableSources().get(0);

        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        if (x.getHints() != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        SqlDropTable sqlDropTable = SqlDdlNodes.dropTable(SqlParserPos.ZERO,
            x.isIfExists(),
            (SqlIdentifier) (convertToSqlNode(tableSource)),
            x.isPurge());
        sqlDropTable.setHints(hints);
        this.sqlNode = sqlDropTable;

        addPrivilegeVerifyItem(tableSource.getSchema(), tableSource.getName().getSimpleName(), PrivilegePoint.DROP);

        return false;
    }

    public boolean visit(SQLTruncateStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        final List<SQLExprTableSource> tableSources = x.getTableSources();

        final SqlNode tableNode = convertToSqlNode(tableSources.get(0));
        SqlIdentifier tableIdentifer = null;
        String tableName = tableSources.get(0).getTableName();
        if (context.isTestMode()) {
            tableName = EagleeyeHelper.rebuildTableName(tableName, true);
            tb2TestNames.put(tableSources.get(0).getTableName(), tableName);
            tableIdentifer = ((SqlCall) tableNode).operand(0);
        } else {
            tableIdentifer = (SqlIdentifier) tableNode;
        }

        SqlTruncateTable sqlTruncateTable = SqlDdlNodes.truncateTable(SqlParserPos.ZERO,
            x.isIfExists(), tableIdentifer, false);
        sqlTruncateTable.setHints(hints);
        this.sqlNode = sqlTruncateTable;

        addPrivilegeVerifyItem(tableSources.get(0).getSchema(),
            tableName,
            PrivilegePoint.DROP);

        return false;
    }

    public boolean visit(SQLCreateIndexStatement x) {
        x.setDbType(DbType.mysql); // To support mysql partition syntax.
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        final SqlIdentifier indexName = (SqlIdentifier) convertToSqlNode(x.getName());
        final SqlIdentifier tableName = (SqlIdentifier) convertToSqlNode(x.getTable());
        final List<SqlIndexColumnName> columns = FastSqlConstructUtils.constructIndexColumnNames(x.getColumns());
        final List<SqlIndexColumnName> covering =
            FastSqlConstructUtils.constructIndexCoveringNames(x.getCovering());
        final SqlIndexConstraintType constraintType =
            null == x.getType() ? null : SqlIndexConstraintType.valueOf(x.getType()
                .toUpperCase());
        final SqlIndexType indexType = null == x.getUsing() ? null : SqlIndexType.valueOf(x.getUsing()
            .toUpperCase());
        final List<SqlIndexOption> options = new LinkedList<>();
        SqlIndexAlgorithmType algorithm = null;
        SqlIndexLockType lock = null;
        // New options type since refactor of FastSql DDL.
        if (x.getIndexDefinition().hasOptions()) {
            SQLIndexOptions fastSqlOptions = x.getIndexDefinition().getOptions();
            if (fastSqlOptions.getIndexType() != null) {
                options.add(SqlIndexOption.createIndexType(SqlParserPos.ZERO,
                    SqlIndexType.valueOf(fastSqlOptions.getIndexType().toUpperCase())));
            }
            if (fastSqlOptions.getKeyBlockSize() != null) {
                options.add(SqlIndexOption.createKeyBlockSize(SqlParserPos.ZERO,
                    (SqlLiteral) convertToSqlNode(fastSqlOptions.getKeyBlockSize())));
            }
            if (fastSqlOptions.getAlgorithm() != null) {
                algorithm = SqlIndexAlgorithmType.valueOf(fastSqlOptions.getAlgorithm().toUpperCase());
            }
            if (fastSqlOptions.getLock() != null) {
                lock = SqlIndexLockType.valueOf(fastSqlOptions.getLock().toUpperCase());
            }
            if (fastSqlOptions.getComment() != null) {
                final SqlCharStringLiteral comment =
                    (SqlCharStringLiteral) convertToSqlNode(fastSqlOptions.getComment());
                options.add(SqlIndexOption.createComment(SqlParserPos.ZERO, comment));
            }
        }

        if (x.isGlobal() || x.isClustered()) {

            if (!(ConfigDataMode.isFastMock() || ConfigDataMode.isMock())) {

                boolean isNewPartitionDb = false;
                final String schemaName = x.getSchema() == null ? ec.getSchemaName() : x.getSchema();
                isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                if (x.getPartitioning() != null) {
                    if (!isNewPartitionDb) {
                        throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                            "Do not support partition by.");
                    }
                }
                if ((x.getDbPartitionBy() != null || x.getTablePartitionBy() != null) && isNewPartitionDb) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Do not support dbpartition/tbpartition by.");
                }
            }
            if (null == x.getName() || x.getName().getSimpleName().isEmpty()) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Global (clustered) secondary index must have a name.");
            }
            if (null == x.getDbPartitionBy()) {
                // Auto dbpartition assign is available.
                if (x.getTablePartitionBy() != null || x.getTablePartitions() != null) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Global (clustered) secondary index should not specify tbpartition without dbpartition.");
                }
            }

            final SqlNode dbPartitionBy = convertToSqlNode(x.getDbPartitionBy());
            final SqlNode tablePartitionBy = convertToSqlNode(x.getTablePartitionBy());
            final SqlNode tablePartitions = convertToSqlNode(x.getTablePartitions());
            final SqlNode partitioning = convertToSqlNode(x.getPartitioning());
            final SqlNode tableGroup = convertToSqlNode(x.getTableGroup());
            SqlCreateIndex sqlIndexTable;
            if (x.isClustered()) {
                sqlIndexTable = SqlCreateIndex.createClusteredIndex(SqlParserPos.ZERO,
                    indexName,
                    tableName,
                    columns,
                    constraintType,
                    indexType,
                    options,
                    algorithm,
                    lock,
                    null == covering ? null : (covering.isEmpty() ? null : covering),
                    dbPartitionBy,
                    tablePartitionBy,
                    tablePartitions,
                    partitioning,
                    x.toString(),
                    tableGroup);
            } else {
                sqlIndexTable = SqlCreateIndex.createGlobalIndex(SqlParserPos.ZERO,
                    indexName,
                    tableName,
                    columns,
                    constraintType,
                    indexType,
                    options,
                    algorithm,
                    lock,
                    null == covering ? null : (covering.isEmpty() ? null : covering),
                    dbPartitionBy,
                    tablePartitionBy,
                    tablePartitions,
                    partitioning,
                    x.toString(),
                    tableGroup);
            }
            sqlIndexTable.setHints(hints);
            this.sqlNode = sqlIndexTable;
        } else {
            // create local index
            final boolean explicitLocal = x.isLocal();
            x.setLocal(false); // Remove local flag in sql.
            SqlCreateIndex sqlIndexTable = SqlCreateIndex.createLocalIndex(indexName,
                tableName,
                columns,
                constraintType,
                explicitLocal,
                indexType,
                options,
                algorithm,
                lock,
                x.toString(), // Should not contain the local keyword.
                SqlParserPos.ZERO);
            sqlIndexTable.setHints(hints);
            this.sqlNode = sqlIndexTable;
        }

        addPrivilegeVerifyItem(x.getSchema(), tableName.getLastName(), PrivilegePoint.INDEX);

        return false;
    }

    public boolean visit(SQLDropIndexStatement x) {
        List headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        if (null == x.getTableName()) {
            throw new TddlNestableRuntimeException("Unknown table name");
        }

        final SqlIdentifier name = (SqlIdentifier) convertToSqlNode(x.getIndexName());
        final SqlIdentifier sqlTableName = (SqlIdentifier) convertToSqlNode(x.getTableName());

        this.sqlNode = SqlDdlNodes.dropIndex(name, sqlTableName, x.toString(), SqlParserPos.ZERO);
        ((SqlDropIndex) this.sqlNode).setHints(hints);

        addPrivilegeVerifyItem(x.getTableName().getSchema(), x.getTableName().getTableName(), PrivilegePoint.INDEX);

        return false;
    }

    public boolean visit(SQLCreateSequenceStatement x) {
        final String name = x.getName().getSimpleName();
        String schemaName = SQLUtils.normalizeNoTrim(x.getSchema());
        schemaName = StringUtils.isBlank(schemaName) ? getDefaultSchema() : schemaName;

        AutoIncrementType sequenceType;
        if (x.isNewSeq()) {
            sequenceType = AutoIncrementType.NEW;
        } else if (x.isGroup()) {
            sequenceType = AutoIncrementType.GROUP;
        } else if (x.isSimple()) {
            sequenceType = AutoIncrementType.SIMPLE;
        } else if (x.isTime()) {
            sequenceType = AutoIncrementType.TIME;
        } else {
            sequenceType = SeqTypeUtil.getDefaultAutoIncrementType(schemaName);
        }

        Sequence sequenceBean = Sequence.newSequence(sequenceType);
        sequenceBean.setKind(SqlKind.CREATE_SEQUENCE);
        sequenceBean.setSchemaName(schemaName);
        sequenceBean.setSequenceName(name);

        if (x.getStartWith() != null) {
            if (x.getStartWith() instanceof SQLIntegerExpr) {
                sequenceBean.setStart(((SQLIntegerExpr) x.getStartWith()).getNumber().longValue());
            } else if (x.getStartWith() instanceof SQLNumberExpr) {
                sequenceBean.setStart(((SQLNumberExpr) x.getStartWith()).getNumber().longValue());
            }
        }

        if (x.getIncrementBy() != null) {
            if (x.getIncrementBy() instanceof SQLIntegerExpr) {
                sequenceBean.setIncrement(((SQLIntegerExpr) x.getIncrementBy()).getNumber().intValue());
            } else if (x.getIncrementBy() instanceof SQLNumberExpr) {
                sequenceBean.setIncrement(((SQLNumberExpr) x.getIncrementBy()).getNumber().intValue());
            }
        }

        if (x.getMaxValue() != null) {
            if (x.getMaxValue() instanceof SQLIntegerExpr) {
                final Number number = ((SQLIntegerExpr) x.getMaxValue()).getNumber();
                if (number instanceof BigInteger) {
                    sequenceBean.setMaxValue(Long.MAX_VALUE);
                } else {
                    sequenceBean.setMaxValue(number.longValue());
                }
            } else if (x.getMaxValue() instanceof SQLNumberExpr) {
                final Number number = ((SQLIntegerExpr) x.getMaxValue()).getNumber();
                if (number instanceof BigInteger) {
                    sequenceBean.setMaxValue(Long.MAX_VALUE);
                } else {
                    sequenceBean.setMaxValue(number.longValue());
                }
            }
        }

        if (x.getCycle() != null) {
            sequenceBean.setCycle(x.getCycle());
        }

        if (x.getUnitCount() != null) {
            if (x.getUnitCount() instanceof SQLIntegerExpr) {
                sequenceBean.setUnitCount(((SQLIntegerExpr) x.getUnitCount()).getNumber().intValue());
            } else if (x.getUnitCount() instanceof SQLNumberExpr) {
                sequenceBean.setUnitCount(((SQLNumberExpr) x.getUnitCount()).getNumber().intValue());
            }

        }

        if (x.getUnitIndex() != null) {
            if (x.getUnitIndex() instanceof SQLIntegerExpr) {
                sequenceBean.setUnitIndex(((SQLIntegerExpr) x.getUnitIndex()).getNumber().intValue());
            } else if (x.getUnitIndex() instanceof SQLNumberExpr) {
                sequenceBean.setUnitIndex(((SQLNumberExpr) x.getUnitIndex()).getNumber().intValue());
            }

        }

        if (x.getStep() != null) {
            if (x.getStep() instanceof SQLIntegerExpr) {
                sequenceBean.setInnerStep(((SQLIntegerExpr) x.getStep()).getNumber().intValue());
            } else if (x.getStep() instanceof SQLNumberExpr) {
                sequenceBean.setInnerStep(((SQLNumberExpr) x.getStep()).getNumber().intValue());
            }

        }

        SqlCreateSequence sequenceNode = SqlDdlNodes.createSequence(
            SqlLiteral.createCharString(name, SqlParserPos.ZERO),
            generateSequenceTableName(schemaName),
            x.toString(),
            SqlParserPos.ZERO);
        sequenceNode.setSequenceBean(sequenceBean.convertSequenceBeanInstance());
        this.sqlNode = sequenceNode;

        addPrivilegeVerifyItem(schemaName, name, PrivilegePoint.CREATE);

        return false;
    }

    public boolean visit(SQLDropSequenceStatement x) {
        final String name = x.getName().getSimpleName();
        String schemaName = SQLUtils.normalizeNoTrim(x.getSchema());
        schemaName = StringUtils.isBlank(schemaName) ? getDefaultSchema() : schemaName;

        SequenceBean sequence = new SequenceBean();
        sequence.setKind(SqlKind.DROP_SEQUENCE);
        sequence.setSchemaName(schemaName);
        sequence.setName(name);

        SqlDropSequence sequenceNode = SqlDdlNodes.dropSequence(
            SqlLiteral.createCharString(name, SqlParserPos.ZERO),
            generateSequenceTableName(schemaName),
            x.toString(),
            SqlParserPos.ZERO);
        sequenceNode.setSequenceBean(sequence);
        this.sqlNode = sequenceNode;

        addPrivilegeVerifyItem(schemaName, name, PrivilegePoint.DROP);

        return false;
    }

    public boolean visit(MySqlRenameSequenceStatement x) {
        final SQLName name = x.getName();
        final SQLName to = x.getTo();

        String schemaName = getDefaultSchema();
        if (name instanceof SQLPropertyExpr) {
            schemaName = SQLUtils.normalizeNoTrim(((SQLPropertyExpr) name).getOwner().toString());
        }

        SequenceBean sequence = new SequenceBean();
        sequence.setKind(SqlKind.RENAME_SEQUENCE);
        sequence.setSchemaName(schemaName);
        sequence.setName(name.getSimpleName());
        sequence.setNewName(to.getSimpleName());

        SqlRenameSequence sequenceNode = SqlDdlNodes.renameSequence(
            SqlLiteral.createCharString(name.getSimpleName(), SqlParserPos.ZERO),
            SqlLiteral.createCharString(to.getSimpleName(), SqlParserPos.ZERO),
            generateSequenceTableName(schemaName),
            x.toString(),
            SqlParserPos.ZERO);
        sequenceNode.setSequenceBean(sequence);
        this.sqlNode = sequenceNode;

        addPrivilegeVerifyItem("", name.getSimpleName(), PrivilegePoint.CREATE);
        addPrivilegeVerifyItem("", name.getSimpleName(), PrivilegePoint.ALTER);

        return false;
    }

    @Override
    public boolean visit(SQLAlterSequenceStatement x) {
        final String name = x.getName().getSimpleName();
        String schemaName = SQLUtils.normalizeNoTrim(x.getSchema());
        schemaName = StringUtils.isBlank(schemaName) ? getDefaultSchema() : schemaName;

        Sequence sequenceBean = new Sequence();
        sequenceBean.setKind(SqlKind.ALTER_SEQUENCE);
        sequenceBean.setSchemaName(schemaName);
        sequenceBean.setSequenceName(name);

        if (x.isChangeToGroup()) {
            sequenceBean.setToType(AutoIncrementType.GROUP);
        } else if (x.isChangeToSimple()) {
            sequenceBean.setToType(AutoIncrementType.SIMPLE);
        } else if (x.isChangeToTime()) {
            sequenceBean.setToType(AutoIncrementType.TIME);
        } else if (x.isChangeToNew()) {
            sequenceBean.setToType(AutoIncrementType.NEW);
        }

        if (x.getStartWith() != null && x.getStartWith() instanceof SQLIntegerExpr) {
            sequenceBean.setStart(((SQLIntegerExpr) x.getStartWith()).getNumber().longValue());
        } else if (x.getStartWith() instanceof SQLNumberExpr) {
            sequenceBean.setStart(((SQLNumberExpr) x.getStartWith()).getNumber().longValue());
        }

        if (x.getIncrementBy() != null) {
            if (x.getIncrementBy() instanceof SQLIntegerExpr) {
                sequenceBean.setIncrement(((SQLIntegerExpr) x.getIncrementBy()).getNumber().intValue());
            } else if (x.getStartWith() instanceof SQLNumberExpr) {
                sequenceBean.setIncrement(((SQLNumberExpr) x.getIncrementBy()).getNumber().intValue());
            }
        }

        if (x.getMaxValue() != null) {
            if (x.getMaxValue() instanceof SQLIntegerExpr) {
                final Number number = ((SQLIntegerExpr) x.getMaxValue()).getNumber();
                if (number instanceof BigInteger) {
                    sequenceBean.setMaxValue(Long.MAX_VALUE);
                } else {
                    sequenceBean.setMaxValue(number.longValue());
                }
            } else if (x.getMaxValue() instanceof SQLNumberExpr) {
                final Number number = ((SQLIntegerExpr) x.getMaxValue()).getNumber();
                if (number instanceof BigInteger) {
                    sequenceBean.setMaxValue(Long.MAX_VALUE);
                } else {
                    sequenceBean.setMaxValue(number.longValue());
                }
            }
        }

        if (x.getCycle() != null) {
            sequenceBean.setCycle(x.getCycle());
        }

        SqlAlterSequence sequenceNode = SqlDdlNodes.alterSequence(
            SqlLiteral.createCharString(name, SqlParserPos.ZERO),
            generateSequenceTableName(schemaName),
            x.toString(),
            SqlParserPos.ZERO);
        sequenceNode.setSequenceBean(sequenceBean.convertSequenceBeanInstance());
        this.sqlNode = sequenceNode;

        addPrivilegeVerifyItem(schemaName, name, PrivilegePoint.ALTER);

        return false;
    }

    public boolean visit(MySqlRenameTableStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        final List<MySqlRenameTableStatement.Item> items = x.getItems();
        if (items.size() > 1) {
            throw new TddlNestableRuntimeException("Only support RENAME one table.");
        }
        SQLName name = null;
        SQLName to = null;
        for (int i = 0; i < items.size(); i++) {
            final MySqlRenameTableStatement.Item item = items.get(i);
            name = item.getName();
            to = item.getTo();
        }

        String schemaName = "";
        if (name instanceof SQLPropertyExpr) {
            schemaName = SQLUtils.normalizeNoTrim(((SQLPropertyExpr) name).getOwner().toString());
        }

        String tableName = name.getSimpleName();
        SqlIdentifier tableNameIdentifier = null;
        if (StringUtils.isNotBlank(schemaName)) {
            tableNameIdentifier = new SqlIdentifier(Arrays.asList(schemaName, SQLUtils.normalizeNoTrim(tableName)),
                SqlParserPos.ZERO);
        } else {
            tableNameIdentifier = new SqlIdentifier(SQLUtils.normalizeNoTrim(tableName), SqlParserPos.ZERO);
        }

        String toSchemaName = "";
        if (to instanceof SQLPropertyExpr) {
            toSchemaName = SQLUtils.normalizeNoTrim(((SQLPropertyExpr) to).getOwner().toString());
        }
        String toTableName = to.getSimpleName();

        SqlIdentifier toTableNameIdentifier;
        if (StringUtils.isNotBlank(toSchemaName)) {
            toTableNameIdentifier =
                new SqlIdentifier(Arrays.asList(toSchemaName, SQLUtils.normalizeNoTrim(toTableName)),
                    SqlParserPos.ZERO);
        } else {
            toTableNameIdentifier = new SqlIdentifier(SQLUtils.normalizeNoTrim(toTableName), SqlParserPos.ZERO);
        }

        SqlRenameTable sqlRenameTable = SqlDdlNodes.renameTable(toTableNameIdentifier,
            tableNameIdentifier,
            x.toString(),
            SqlParserPos.ZERO);
        sqlRenameTable.setHints(hints);
        this.sqlNode = sqlRenameTable;

        // MySqlRenameTableStatement 无 schema 字段
        addPrivilegeVerifyItem(schemaName, tableName, PrivilegePoint.ALTER);
        addPrivilegeVerifyItem(schemaName, to.getSimpleName(), PrivilegePoint.CREATE);
        addPrivilegeVerifyItem(schemaName, to.getSimpleName(), PrivilegePoint.INSERT);

        return false;
    }

    private void addColumnOpts(Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts, String name,
                               SqlAlterTable.ColumnOpt columnOpt) {
        if (columnOpts == null) {
            return;
        }
        if (columnOpts.get(columnOpt) == null) {
            columnOpts.put(columnOpt, new ArrayList<String>());
        }
        final List<String> strings = columnOpts.get(columnOpt);
        strings.add(name);

    }

    public boolean visit(MySqlCreateTableStatement x) {
        if (!ConfigDataMode.isFastMock() && ConfigDataMode.getConfigServerMode() == ConfigDataMode.Mode.GMS) {
            if (x.getPartitioning() != null && x.getDbPartitionBy() != null) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support mix dbpartition with range/list/hash partition.");
            }
            if (x.getSelect() != null) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support create table select.");
            }
            if (x.findForeignKey() != null && x.findForeignKey().size() > 0) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support foreign key.");
            }
            if (x.getType() != null &&
                (x.getType() == GLOBAL_TEMPORARY || x.getType() == LOCAL_TEMPORARY || x.getType() == TEMPORARY)) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support temporary table.");
            }
            String schemaName = x.getSchema() == null ? (ec == null ? "" : ec.getSchemaName()) : x.getSchema();
            schemaName = SQLUtils.normalize(schemaName);
            boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
            if (x.getPartitioning() != null) {
                if (!isNewPartitionDb) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Do not support partition by.");
                }
            }
            if ((x.getDbPartitionBy() != null || x.getTablePartitionBy() != null) && isNewPartitionDb) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support dbpartition/tbpartition by.");
            }
        }

        SQLExprTableSource tableSource = x.getTableSource();
        SQLExprTableSource likeTableSource = x.getLike();

        SqlNode tableNameSqlNode = convertToSqlNode(tableSource);
        SqlIdentifier tableName = null;
        if (tableNameSqlNode instanceof SqlIdentifier) {
            tableName = (SqlIdentifier) tableNameSqlNode;
        } else if (context.isTestMode()) {
            // For create table statement with test table hint, create table with __test_ prefix
            tableName = (SqlIdentifier) ((SqlBasicCall) tableNameSqlNode).getOperands()[0];
        } else {
            throw GeneralUtil.nestedException("Unknown SqlIdentifier tableName");
        }

        SqlNode likeTableNameSqlNode = likeTableSource == null ? null : convertToSqlNode(likeTableSource);
        SqlIdentifier likeTableName = null;
        if (likeTableNameSqlNode != null) {
            if (likeTableNameSqlNode instanceof SqlIdentifier) {
                likeTableName = (SqlIdentifier) likeTableNameSqlNode;
            } else if (context.isTestMode()) {
                likeTableName = (SqlIdentifier) ((SqlBasicCall) likeTableNameSqlNode).getOperands()[0];
            } else {
                throw GeneralUtil.nestedException("Unknown SqlIdentifier likeTableName");
            }
        }

        final SqlNode dbPartitionBy = convertToSqlNode(x.getDbPartitionBy());
        final SqlNode dbpartitions = convertToSqlNode(x.getDbpartitions());
        final SqlNode tablePartitionBy = convertToSqlNode(x.getTablePartitionBy());
        final SqlNode tbpartitions = convertToSqlNode(x.getTbpartitions());
        if (dbPartitionBy != null || dbpartitions != null || tablePartitionBy != null || tbpartitions != null) {
            if (SQLCreateTableStatement.Type.GLOBAL_TEMPORARY.equals(x.getType())) {
                throw GeneralUtil.nestedException("Do not support temporary table");
            }
        }
        if (x.getSelect() != null) {
            throw GeneralUtil.nestedException("Do not support create table select.");
        }
        final SqlNode sqlPartition = convertToSqlNode(x.getPartitioning());
        final SqlNode localPartition = convertToSqlNode(x.getLocalPartitioning());
        final SqlNode tableGroupName;
        if (x.getTableGroup() != null) {
            tableGroupName = convertToSqlNode(x.getTableGroup());
        } else {
            tableGroupName = null;
        }

        final SqlNode joinGroupName;
        if (x.getJoinGroup() != null) {
            joinGroupName = convertToSqlNode(x.getJoinGroup());
        } else {
            joinGroupName = null;
        }

        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        final TableElementBean tableElementBean = new TableElementBean();
        final SequenceBean sequence = convertTableElements(tableName,
            x.getTableElementList(),
            context,
            tableElementBean);
        if ((null == tableElementBean.colDefs || tableElementBean.colDefs.isEmpty())
            && null == x.getLike() && null == x.getSelect()) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "A table must have at least 1 column.");
        }

        String schemaName = SQLUtils.normalizeNoTrim(tableSource.getSchema());
        schemaName = StringUtils.isBlank(schemaName) ? getDefaultSchema() : schemaName;
        if (sequence != null) {
            sequence.setSchemaName(schemaName);
            sequence.setNew(true);
            final List<SQLAssignItem> tableOptions = x.getTableOptions();
            if (tableOptions != null) {
                for (int i = 0; i < tableOptions.size(); i++) {
                    final SQLAssignItem sqlAssignItem = tableOptions.get(i);
                    final SQLExpr target = sqlAssignItem.getTarget();
                    if (target instanceof SQLIdentifierExpr
                        && ((SQLIdentifierExpr) target).hashCode64() == FnvHash.Constants.AUTO_INCREMENT) {
                        final long i1 = ((SQLIntegerExpr) sqlAssignItem.getValue()).getNumber().longValue();
                        sequence.setStart(i1);
                        sequence.setNew(true);
                    }
                }
            }
        }

        if (tableSource != null) {
            addPrivilegeVerifyItem(schemaName, tableSource.getName().getSimpleName(), PrivilegePoint.CREATE);
        }

        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        final List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        if (x.getHints() != null) {
            hints = FastSqlConstructUtils.convertHints(x.getHints(), context, ec);
        }

        boolean isShadow = x.getType() == SQLCreateTableStatement.Type.SHADOW;
        if (isShadow) {
            x.setType(null);
        }
        final boolean isAutoPartition = x.isPrefixPartition();
        if (isAutoPartition) {
            x.setPrefixPartition(false); // Remove to prevent prefix partition in source sql.
        }
        final boolean isSingle = x.isSingle(); // Dealing suffix single only.
        if (isSingle) {
            x.setSingle(false); // Remove to prevent suffix single in source sql.
        }
        final SqlCreateTable table = SqlDdlNodes.createTable(SqlParserPos.ZERO,
            false,
            x.isIfNotExists(),
            tableName,
            likeTableName,
            sqlNodeList,
            null,
            dbPartitionBy,
            dbpartitions,
            tablePartitionBy,
            tbpartitions,
            x.toString(),
            x.isBroadCast(),
            sequence,
            sqlPartition,
            localPartition,
            tableGroupName,
            joinGroupName,
            null);
        if (localPartition != null) {
            setDefaultValueForLocalPartition(localPartition);
        }
        table.setShadow(isShadow);
        if (isShadow) {
            x.setType(SQLCreateTableStatement.Type.SHADOW);
        }
        table.setSingle(isSingle);
        if (isSingle) {
            x.setSingle(isSingle);
        }
        table.setAutoPartition(isAutoPartition);
        if (isAutoPartition) {
            x.setPrefixPartition(true);
        }
        if (SQLCreateTableStatement.Type.GLOBAL_TEMPORARY.equals(x.getType())
            || SQLCreateTableStatement.Type.TEMPORARY.equals(x.getType())) {
            table.setTemporary(true);
        }
        table.setAutoIncrement(sequence);
        table.setHints(hints);
        table.setColDefs(tableElementBean.colDefs);
        table.setPrimaryKey(tableElementBean.primaryKey);
        table.setUniqueKeys(tableElementBean.uniqueKeys);
        table.setGlobalKeys(tableElementBean.globalKeys);
        table.setGlobalUniqueKeys(tableElementBean.globalUniqueKeys);
        table.setClusteredKeys(tableElementBean.clusteredKeys);
        table.setClusteredUniqueKeys(tableElementBean.clusteredUniqueKeys);
        table.setKeys(tableElementBean.keys);
        table.setFullTextKeys(tableElementBean.fullTextKeys);
        table.setForeignKeys(tableElementBean.foreignKeys);
        table.setChecks(tableElementBean.checks);

        if (x.getLocality() != null) {
            table.setLocality(RelUtils.stringValue(convertToSqlNode(x.getLocality())));
        }

        if (x.getAutoSplit() != null) {
            SqlNode sqlNode = convertToSqlNode(x.getAutoSplit());
            table.setAutoSplit(RelUtils.booleanValue(sqlNode));
        }

        // Check on GSI with partition moved to validator(compatible with auto partition).

        if (x.getComment() != null) {
            table.setComment(x.getComment().toString());
        }

        // 热点映射规则
        if (x.getExtPartition() != null && x.getExtPartition().getItems() != null
            && x.getExtPartition().getItems().size() > 0) {
            List<MappingRule> mappingRules =
                getMappingRules(x.getExtPartition(), dbPartitionBy, tablePartitionBy, true);
            table.setMappingRules(mappingRules);
        }

        // set default charset for SqlCreateTable
        final List<SQLAssignItem> tableOptions = x.getTableOptions();
        if (tableOptions != null) {
            for (final SQLAssignItem sqlAssignItem : tableOptions) {
                final SQLExpr target = sqlAssignItem.getTarget();
                final SQLExpr value = sqlAssignItem.getValue();
                if (target instanceof SQLIdentifierExpr) {
                    String targetName = ((SQLIdentifierExpr) target).getSimpleName();
                    if (StringUtils.equalsIgnoreCase(targetName, "ENGINE")) {
                        if (value instanceof SQLCharExpr) {
                            table.setEngine(Engine.of(((SQLCharExpr) value).getText()));
                        } else if (value instanceof SQLIdentifierExpr) {
                            table.setEngine(Engine.of(((SQLIdentifierExpr) value).getName()));
                        } else {
                            table.setEngine(Engine.of(value.toString()));
                        }
                    } else if (StringUtils.equalsIgnoreCase(targetName, "ARCHIVE_MODE")) {
                        if (value instanceof SQLCharExpr) {
                            table.setArchiveMode(ArchiveMode.of(((SQLCharExpr) value).getText()));
                        } else if (value instanceof SQLIdentifierExpr) {
                            table.setArchiveMode(ArchiveMode.of(((SQLIdentifierExpr) value).getName()));
                        } else {
                            table.setArchiveMode(ArchiveMode.of(value.toString()));
                        }
                    } else if (StringUtils.equalsIgnoreCase(targetName, "CHARSET")
                        || StringUtils.equalsIgnoreCase(targetName, "CHARACTER SET")) {
                        table.setDefaultCharset(((SQLIdentifierExpr) value).getSimpleName().toLowerCase());
                    } else if (StringUtils.equalsIgnoreCase(targetName, "ROW_FORMAT")) {
                        table.setRowFormat(((SQLIdentifierExpr) value).getSimpleName().toLowerCase());
                    } else if (StringUtils.equalsIgnoreCase(targetName, "COLLATE")) {
                        if (value instanceof SQLIdentifierExpr) {
                            Optional.ofNullable((SQLIdentifierExpr) value)
                                .map(SQLIdentifierExpr::getSimpleName)
                                .map(String::toLowerCase)
                                .ifPresent(collation -> table.setDefaultCollation(collation));
                        } else if (value instanceof SQLCharExpr) {
                            Optional.ofNullable((SQLCharExpr) value)
                                .map(SQLTextLiteralExpr::getText)
                                .map(String::toLowerCase)
                                .ifPresent(collation -> table.setDefaultCollation(collation));
                        }
                    }
                }
            }
        }

        this.sqlNode = table;
        return false;
    }

    private void setDefaultValueForLocalPartition(SqlNode localPartition) {
        if (localPartition != null) {
            SqlNumericLiteral expireAfterLiteral = ((SqlPartitionByRange) localPartition).getExpireAfter();
            if (expireAfterLiteral == null) {
                SQLIntegerExpr defaultExpireAfterLiteral =
                    (SQLIntegerExpr) new MySqlExprParser(ByteString.from(DEFAULT_EXPIRE_AFTER)).expr();
                ((SqlPartitionByRange) localPartition)
                    .setExpireAfter((SqlNumericLiteral) convertToSqlNode(defaultExpireAfterLiteral));
            } else {
                Integer expireAfterCount = expireAfterLiteral.getValueAs(Integer.class);
                if (expireAfterCount <= 0 && expireAfterCount != -1) {
                    throw new TddlNestableRuntimeException("The value of EXPIRE AFTER must be greater than 0");
                }
            }

            SqlNumericLiteral preAllocateLiteral = ((SqlPartitionByRange) localPartition).getPreAllocate();
            if (preAllocateLiteral == null) {
                SQLIntegerExpr defaultPreAllocateLiteral =
                    (SQLIntegerExpr) new MySqlExprParser(ByteString.from(DEFAULT_PRE_ALLOCATE)).expr();
                ((SqlPartitionByRange) localPartition)
                    .setPreAllocate((SqlNumericLiteral) convertToSqlNode(defaultPreAllocateLiteral));
            }

            SqlNode pivotDateExpr = ((SqlPartitionByRange) localPartition).getPivotDateExpr();
            if (pivotDateExpr == null) {
                SQLExpr defaultPivotDateExpr = new MySqlExprParser(ByteString.from(NOW_FUNCTION)).expr();
                LocalPartitionDefinitionInfo.assertValidPivotDateExpr(defaultPivotDateExpr);
                ((SqlPartitionByRange) localPartition).setPivotDateExpr(convertToSqlNode(defaultPivotDateExpr));
            }
        }
    }

    public SqlTableOptions convertTableOptions(MySqlAlterTableOption alterTableOption,
                                               SqlTableOptions outTableOptions) {
        return convertTableOptions(alterTableOption.getName(), alterTableOption.getValue(), outTableOptions);
    }

    public SqlTableOptions convertTableOptions(String key, SQLObject valueObject, SqlTableOptions outTableOptions) {
        final SqlNode value = convertToSqlNode(valueObject);

        if (TStringUtil.equalsIgnoreCase(key, "ENGINE")) {
            outTableOptions.setEngine((SqlIdentifier) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "AUTO_INCREMENT")) {
            outTableOptions.setAutoIncrement((SqlNumericLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "AVG_ROW_LENGTH")) {
            outTableOptions.setAvgRowLength((SqlCall) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "CHARACTER SET")) {
            outTableOptions.setCharSet((SqlIdentifier) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "CHECKSUM")) {
            outTableOptions.setCheckSum((SqlLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "COLLATE")) {
            outTableOptions.setCollation((SqlIdentifier) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "COMMENT")) {
            // Fixed in fastsql since 901.
            outTableOptions.setComment((SqlCharStringLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "CONNECTION")) {
            outTableOptions.setConnection((SqlCharStringLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "DATA DIRECTORY")) {
            outTableOptions.setDataDir((SqlCharStringLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "DELAY_KEY_WRITE")) {
            outTableOptions.setDelayKeyWrite((SqlLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "INDEX DIRECTORY")) {
            outTableOptions.setIndexDir((SqlCharStringLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "INSERT_METHOD")) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "table option INSERT_METHOD");
            // outTableOptions.setInsertMethod(InsertMethod.FIRST);
        } else if (TStringUtil.equalsIgnoreCase(key, "KEY_BLOCK_SIZE")) {
            outTableOptions.setKeyBlockSize((SqlCall) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "MAX_ROWS")) {
            outTableOptions.setMaxRows((SqlCall) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "MIN_ROWS")) {
            outTableOptions.setMinRows((SqlCall) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "PACK_KEYS")) {
            if (value instanceof SqlIdentifier) {
                final String packKeys = ((SqlIdentifier) value).getLastName();
                if (TStringUtil.equalsIgnoreCase(packKeys, "DEFAULT")) {
                    outTableOptions.setPackKeys(PackKeys.DEFAULT);
                } else {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "illegal PACK_KEYS value " + packKeys);
                }
            } else if (value instanceof SqlNumericLiteral) {
                final int packKeys = ((SqlNumericLiteral) value).intValue(true);
                if (packKeys == 0) {
                    outTableOptions.setPackKeys(PackKeys.FALSE);
                } else if (packKeys == 1) {
                    outTableOptions.setPackKeys(PackKeys.TRUE);
                } else {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "illegal PACK_KEYS value " + packKeys);
                }
            }
        } else if (TStringUtil.equalsIgnoreCase(key, "PASSWORD")) {
            outTableOptions.setPassword((SqlCharStringLiteral) value);
        } else if (TStringUtil.equalsIgnoreCase(key, "ROW_FORMAT")) {
            final String rowFormatStr = ((SqlIdentifier) value).getLastName();

            try {
                outTableOptions.setRowFormat(RowFormat.valueOf(rowFormatStr.toUpperCase()));
            } catch (Exception e) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "illegal ROW_FORMAT value " + rowFormatStr);
            }
        } else if (TStringUtil.equalsIgnoreCase(key, "UNION")) {
            outTableOptions.setUnion(getUnionTables((SQLTableSource) valueObject, new ArrayList<>()));
        } else if (TStringUtil.equalsIgnoreCase(key, "ALGORITHM")) {
            outTableOptions.setAlgorithm((SqlIdentifier) value);
        }

        return outTableOptions;
    }

    private List<SqlIdentifier> getUnionTables(final SQLTableSource x, List<SqlIdentifier> targetTables) {
        if (x instanceof SQLExprTableSource) {
            if (null != x.getAlias()) {
                throw new TddlNestableRuntimeException(
                    "You have an error in your SQL syntax; Error occurs around this fragment: "
                        + x.toString());
            }

            // single table
            SqlIdentifier targetTable = (SqlIdentifier) convertToSqlNode(((SQLExprTableSource) x).getExpr());
            if (context.isTestMode() && ((SQLExprTableSource) x).getTableName() != null
                && (targetTable instanceof SqlIdentifier)) {
                String tableName = EagleeyeHelper.rebuildTableName(((SQLExprTableSource) x).getTableName(), true);
                if (tb2TestNames != null) {
                    tb2TestNames.put(((SQLExprTableSource) x).getTableName(), tableName);
                }

                targetTable = ((SqlIdentifier) targetTable)
                    .setName(((SqlIdentifier) targetTable).names.size() - 1, tableName);
            }
            targetTables.add(targetTable);
        } else if (x instanceof SQLJoinTableSource) {
            // delete from multi table
            final SQLJoinTableSource joinTableSource = (SQLJoinTableSource) x;
            getUnionTables(joinTableSource.getLeft(), targetTables);
            getUnionTables(joinTableSource.getRight(), targetTables);
        }
        return targetTables;
    }

    public boolean visit(SQLAlterTableStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }

        String schema = SQLUtils.normalizeNoTrim(x.getSchema());
        schema = StringUtils.isBlank(schema) ? getDefaultSchema() : schema;
        String tableName = SQLUtils.normalizeNoTrim(x.getTableName());
        final SqlIdentifier tableIdentifier = new SqlIdentifier(Arrays.asList(schema, tableName), SqlParserPos.ZERO);
        if (x.getPartition() != null) {
            SqlNode partition = convertToSqlNode(x.getPartition());
            SqlAlterTableRepartition alterTableNewPartition =
                new SqlAlterTableRepartition(tableIdentifier, x.toString(), new ArrayList<>(), partition, false, null);

            this.sqlNode = alterTableNewPartition;
            return false;
        }

        if (x.getAlignToTableGroup() != null) {
            String targetTableGroup = SQLUtils.normalizeNoTrim(x.getAlignToTableGroup().getTablegroup().toString());
            final SqlIdentifier targetTableGroupIdentifier = new SqlIdentifier(targetTableGroup, SqlParserPos.ZERO);
            SqlAlterTableRepartition alterTableNewPartition =
                new SqlAlterTableRepartition(tableIdentifier, x.toString(), new ArrayList<>(), null, true,
                    targetTableGroupIdentifier);

            this.sqlNode = alterTableNewPartition;
            return false;
        }

        if (x.getLocalPartition() != null) {
            SqlPartitionByRange localPartition = (SqlPartitionByRange) convertToSqlNode(x.getLocalPartition());
            if (localPartition != null) {
                setDefaultValueForLocalPartition(localPartition);
            }
            SqlAlterTableRepartitionLocalPartition sqlAlterTableRepartitionLocalPartition =
                new SqlAlterTableRepartitionLocalPartition(tableIdentifier, localPartition);
            this.sqlNode = sqlAlterTableRepartitionLocalPartition;
            return false;
        }

        if (x.isRemoveLocalPatiting()) {
            SqlAlterTableRemoveLocalPartition sqlAlterTableRemoveLocalPartition =
                new SqlAlterTableRemoveLocalPartition(tableIdentifier);
            this.sqlNode = sqlAlterTableRemoveLocalPartition;
            return false;
        }

        if (x.isRemovePatiting()) {
            this.sqlNode = new SqlAlterTableRemovePartitioning(tableIdentifier, x.toString());
            return false;
        }

        List<String> logicalReferencedTables = new ArrayList<>();

        boolean gsiExists = false;
        SequenceBean sequenceBean = null;
        Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = new HashMap<>();
        SqlTableOptions tableOptions = null;
        List<SqlAlterSpecification> alters = new ArrayList<>();
        final List<SQLAlterTableItem> items = x.getItems();
        for (int i = 0; i < items.size(); i++) {
            final SQLAlterTableItem sqlAlterTableItem = items.get(i);

            final SqlNode alterItem = convertToSqlNode(sqlAlterTableItem);
            if (alterItem instanceof SqlAlterSpecification) {
                alters.add((SqlAlterSpecification) alterItem);
            } else if (alterItem instanceof SqlNodeList) {
                for (SqlNode node : ((SqlNodeList) alterItem).getList()) {
                    if (node instanceof SqlAlterSpecification) {
                        alters.add((SqlAlterSpecification) node);
                    }
                }
            }
            if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                final List<SQLColumnDefinition> fieldList = ((SQLAlterTableAddColumn) sqlAlterTableItem).getColumns();
                for (int j = 0; j < fieldList.size(); j++) {
                    final SQLColumnDefinition sqlColumnDefinition = fieldList.get(j);
                    final boolean autoIncrement = sqlColumnDefinition.isAutoIncrement();
                    if (autoIncrement && sequenceBean != null) {
                        throw new TddlNestableRuntimeException(
                            "Incorrect table definition; there can be only one auto column and it must be defined as a key");
                    }
                    if (autoIncrement) {
                        sequenceBean = new SequenceBean();
                        sequenceBean.setType(sqlColumnDefinition.getSequenceType());
                        sequenceBean.setStart(sequenceBean.getType() == SequenceAttribute.Type.GROUP ? 0L : 1L);
                        sequenceBean.setNew(true);
                    }
                    sqlColumnDefinition.setSequenceType(null);
                    String normalize = SQLUtils.normalizeNoTrim(sqlColumnDefinition.getName().getSimpleName());
                    addColumnOpts(columnOpts, normalize, ColumnOpt.ADD);
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddConstraint &&
                ((SQLAlterTableAddConstraint) sqlAlterTableItem).getConstraint() instanceof MysqlForeignKey) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support foreign key.");
            } else if (sqlAlterTableItem instanceof MySqlAlterTableModifyColumn) {
                final SQLColumnDefinition sqlColumnDefinition =
                    ((MySqlAlterTableModifyColumn) sqlAlterTableItem).getNewColumnDefinition();
                final boolean autoIncrement = sqlColumnDefinition.isAutoIncrement();
                if (autoIncrement && sequenceBean != null) {
                    throw new TddlNestableRuntimeException(
                        "Incorrect table definition; there can be only one auto column and it must be defined as a "
                            + "key");
                }
                if (autoIncrement) {
                    sequenceBean = new SequenceBean();
                    sequenceBean.setType(sqlColumnDefinition.getSequenceType());
                    sequenceBean.setStart(sequenceBean.getType() == SequenceAttribute.Type.GROUP ? 0L : 1L);
                    sequenceBean.setNew(true);
                }
                final MySqlAlterTableModifyColumn sqlModifyColumn = (MySqlAlterTableModifyColumn) sqlAlterTableItem;
                final SQLColumnDefinition newColumnDefinition = sqlModifyColumn.getNewColumnDefinition();
                newColumnDefinition.setSequenceType(null);
                final SQLName name = newColumnDefinition.getName();
                final String normalize = SQLUtils.normalizeNoTrim(name.getSimpleName());
                addColumnOpts(columnOpts, normalize, SqlAlterTable.ColumnOpt.MODIFY);
            } else if (sqlAlterTableItem instanceof MySqlAlterTableOption) {

                // Caution removed since FastSql 901.
                // Now AUTO_INCREMENT is in table options.
                final MySqlAlterTableOption tableOp = (MySqlAlterTableOption) sqlAlterTableItem;
                if (tableOp.getName().equals("AUTO_INCREMENT")) {
                    if (sequenceBean != null) {
                        throw new TddlNestableRuntimeException(
                            "Incorrect table definition; there can be only one auto column and it must be defined as "
                                + "a key");
                    }
                    sequenceBean = new SequenceBean();
                    sequenceBean.setStart(((SQLIntegerExpr) tableOp.getValue()).getNumber().longValue());
                    sequenceBean.setNew(false);
                }

                tableOptions = convertTableOptions(tableOp,
                    Optional.ofNullable(tableOptions).orElse(new SqlTableOptions(SqlParserPos.ZERO)));
            } else if (sqlAlterTableItem instanceof MySqlAlterTableChangeColumn) {
                final MySqlAlterTableChangeColumn sqlChangeTo = (MySqlAlterTableChangeColumn) sqlAlterTableItem;
                final SQLColumnDefinition sqlColumnDefinition = sqlChangeTo.getNewColumnDefinition();
                final boolean autoIncrement = sqlColumnDefinition.isAutoIncrement();
                if (autoIncrement && sequenceBean != null) {
                    throw new TddlNestableRuntimeException(
                        "Incorrect table definition; there can be only one auto column and it must be defined as a "
                            + "key");
                }
                if (autoIncrement) {
                    sequenceBean = new SequenceBean();
                    sequenceBean.setType(sqlColumnDefinition.getSequenceType());
                    sequenceBean.setStart(sequenceBean.getType() == SequenceAttribute.Type.GROUP ? 0L : 1L);
                    sequenceBean.setNew(true);
                }
                String column = sqlChangeTo.getColumnName().getSimpleName();
                final String normalize = SQLUtils.normalizeNoTrim(column);
                addColumnOpts(columnOpts, normalize, SqlAlterTable.ColumnOpt.CHANGE);
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropColumnItem) {
                final SQLAlterTableDropColumnItem sqlDrop = (SQLAlterTableDropColumnItem) sqlAlterTableItem;
                for (int j = 0; j < sqlDrop.getColumns().size(); j++) {
                    final SQLName sqlName = sqlDrop.getColumns().get(j);
                    final String simpleName = sqlName.getSimpleName();
                    final String normalize = SQLUtils.normalizeNoTrim(simpleName);
                    addColumnOpts(columnOpts, normalize, SqlAlterTable.ColumnOpt.DROP);
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddIndex) {
                SQLAlterTableAddIndex addIndex = (SQLAlterTableAddIndex) sqlAlterTableItem;

                if (addIndex.isGlobal() || addIndex.isClustered()) {
                    gsiExists = true;
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropIndex
                || sqlAlterTableItem instanceof SQLAlterTableDropKey) {
                final SqlAlterTableDropIndex sqlDropIndex = (SqlAlterTableDropIndex) alterItem;
                final SqlIdentifier indexName = sqlDropIndex.getIndexName();

                final GsiMetaBean gsiMeta = ec.getSchemaManager(schema)
                    .getGsi(indexName.getLastName(), IndexStatus.ALL);
                if (gsiMeta.isGsi(indexName.getLastName())) {
                    gsiExists = true;
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableRenameIndex) {
                final SqlAlterTableRenameIndex sqlRenameIndex = (SqlAlterTableRenameIndex) alterItem;
                final SqlIdentifier indexName = sqlRenameIndex.getIndexName();

                final GsiMetaBean gsiMeta = ec.getSchemaManager(schema)
                    .getGsi(indexName.getLastName(), IndexStatus.ALL);
                if (gsiMeta.isGsi(indexName.getLastName())) {
                    gsiExists = true;
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddExtPartition
                || sqlAlterTableItem instanceof SQLAlterTableDropExtPartition) {
                // 只要出现 SQLAlterTableAddExtPartition，就认为是
                // SqlAlterRule，并且语法只准许出现一个 item
                SqlAlterRule sqlAlterRule =
                    SqlDdlNodes.alterRule(new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTableName()),
                            SqlParserPos.ZERO),
                        x.toString(),
                        SqlParserPos.ZERO);
                if (sqlAlterTableItem instanceof SQLAlterTableAddExtPartition) {
                    List<MappingRule> addMappingRules =
                        getMappingRules(((SQLAlterTableAddExtPartition) sqlAlterTableItem).getExtPartition(),
                            null,
                            null,
                            false);
                    sqlAlterRule.setAddMappingRules(addMappingRules);
                } else {
                    List<MappingRule> dropMappingRules =
                        getMappingRules(((SQLAlterTableDropExtPartition) sqlAlterTableItem).getExtPartition(),
                            null,
                            null,
                            false);
                    sqlAlterRule.setDropMappingRules(dropMappingRules);
                }
                this.sqlNode = sqlAlterRule;
                return false;
            } else if (sqlAlterTableItem instanceof SQLAlterTableSetOption) {
                SqlAlterRule sqlAlterRule =
                    SqlDdlNodes.alterRule(new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTableName()),
                            SqlParserPos.ZERO),
                        x.toString(),
                        SqlParserPos.ZERO);
                SQLAlterTableSetOption item = (SQLAlterTableSetOption) sqlAlterTableItem;
                if (!item.isAlterTableGroup()) {
                    this.sqlNode = sqlAlterRule;
                    if (item.getOptions() != null) {
                        for (SQLAssignItem option : item.getOptions()) {
                            String target = option.getTarget().toString();
                            int value = Boolean.valueOf(option.getValue().toString()) ? 1 : 0;
                            if (StringUtils.equalsIgnoreCase(TddlConstants.RULE_BROADCAST, target)) {
                                sqlAlterRule.setBroadcast(value);
                            } else if (StringUtils.equalsIgnoreCase(TddlConstants.RULE_ALLOW_FULL_TABLE_SCAN, target)) {
                                sqlAlterRule.setAllowFullTableScan(value);
                            } else {
                                throw new TddlRuntimeException(ErrorCode.ERR_RULE_PROPERTY_NOT_ALLOWED_TO_CHANGE,
                                    target);
                            }
                        }
                    }
                } else {
                    assert item.getOptions().size() == 1;
                    SQLAssignItem option = item.getOptions().get(0);
                    String value = SQLUtils.normalizeNoTrim(option.getValue().toString());
                    if (value.equalsIgnoreCase("NULL") || value.equalsIgnoreCase("''")) {
                        value = "";
                    }
                    List<SqlIdentifier> objectNames = new ArrayList<>();
                    SqlIdentifier tableNameIdentifier =
                        new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTableName()), SqlParserPos.ZERO);
                    objectNames.add(tableNameIdentifier);
                    if (x.getName() instanceof SQLPropertyExpr) {
                        SQLExpr owner = ((SQLPropertyExpr) x.getName()).getOwner();
                        if (owner instanceof SQLPropertyExpr) {
                            objectNames.add(
                                new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLPropertyExpr) owner).getName()),
                                    SqlParserPos.ZERO));
                            objectNames.add(new SqlIdentifier(
                                SQLUtils.normalizeNoTrim(((SQLPropertyExpr) owner).getOwner().toString()),
                                SqlParserPos.ZERO));
                        } else {
                            objectNames.add(
                                new SqlIdentifier(SQLUtils.normalizeNoTrim(owner.toString()), SqlParserPos.ZERO));
                        }
                    }
                    SqlAlterTableSetTableGroup sqlAlterTableSetTableGroup =
                        SqlDdlNodes
                            .alterTableSetTableGroup(objectNames,
                                tableNameIdentifier,
                                value,
                                x.toString(),
                                SqlParserPos.ZERO,
                                item.isForce()
                            );
                    this.sqlNode = sqlAlterTableSetTableGroup;
                }
                return false;
            } else if (sqlAlterTableItem instanceof SQLAlterTableSetComment) {
                final SQLAlterTableSetComment setComment = (SQLAlterTableSetComment) sqlAlterTableItem;
                tableOptions = convertTableOptions("COMMENT",
                    setComment.getComment(),
                    Optional.ofNullable(tableOptions).orElse(new SqlTableOptions(SqlParserPos.ZERO)));
            } else if (sqlAlterTableItem instanceof DrdsAlterTablePartition) {

                DrdsAlterTablePartition alterTablePartition = (DrdsAlterTablePartition) sqlAlterTableItem;

                final SqlNode dbPartitionBy = convertToSqlNode(alterTablePartition.getDbPartitionBy());
                final SqlNode dbpartitions = convertToSqlNode(alterTablePartition.getDbPartitions());
                final SqlNode tablePartitionBy = convertToSqlNode(alterTablePartition.getTablePartitionBy());
                final SqlNode tbpartitions = convertToSqlNode(alterTablePartition.getTablePartitions());

                SqlAlterTablePartitionKey alterTablePartitionKey =
                    new SqlAlterTablePartitionKey(tableIdentifier, x.toString(), dbPartitionBy, dbpartitions,
                        tablePartitionBy, tbpartitions);

                this.sqlNode = alterTablePartitionKey;

                return false;
            } else if (sqlAlterTableItem instanceof DrdsAlterTableBroadcast) {
                SqlAlterTablePartitionKey alterTablePartitionKey =
                    new SqlAlterTablePartitionKey(
                        tableIdentifier,
                        x.toString(),
                        null,
                        null,
                        null,
                        null
                    );
                alterTablePartitionKey.setBroadcast(true);

                if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                    this.sqlNode = SqlAlterTableRepartition.create(alterTablePartitionKey);
                } else {
                    this.sqlNode = alterTablePartitionKey;
                }

                return false;
            } else if (sqlAlterTableItem instanceof DrdsAlterTableSingle) {
                SqlAlterTablePartitionKey alterTablePartitionKey =
                    new SqlAlterTablePartitionKey(
                        tableIdentifier,
                        x.toString(),
                        null,
                        null,
                        null,
                        null
                    );
                alterTablePartitionKey.setSingle(true);

                if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                    this.sqlNode = SqlAlterTableRepartition.create(alterTablePartitionKey);
                } else {
                    this.sqlNode = alterTablePartitionKey;
                }

                return false;
            } else if (sqlAlterTableItem instanceof SQLAlterTablePartitionCount) {
                SqlAlterTablePartitionCount alterTablePartitionCount =
                    new SqlAlterTablePartitionCount(tableIdentifier, x.toString(), alters);
                int count = Integer.parseInt(
                    ((SQLAlterTablePartitionCount) sqlAlterTableItem).getCount().getValue().toString());
                alterTablePartitionCount.setPartitionCount(count);
                this.sqlNode = alterTablePartitionCount;

                return false;
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddPartition) {
                if (alters.size() > 1 && alters.get(alters.size() - 1).getKind() != SqlKind.ADD_PARTITION) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Do not support mix ADD PARTITION with other ALTER statements");
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropPartition) {
                if (alters.size() > 1 && alters.get(alters.size() - 1).getKind() != SqlKind.DROP_PARTITION) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Do not support mix DROP PARTITION with other ALTER statements");
                }
            } else if (sqlAlterTableItem instanceof SQLAlterTableTruncatePartition) {
                if (alters.size() > 1 && alters.get(alters.size() - 1).getKind() != SqlKind.DROP_PARTITION) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Do not support mix TRUNCATE PARTITION with other ALTER statements");
                }
            } else if (sqlAlterTableItem instanceof DrdsAlterTableAsOfTimeStamp) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support alter table as of timestamp");
            } else if (sqlAlterTableItem instanceof DrdsAlterTablePurgeBeforeTimeStamp) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Do not support alter table purge before timestamp");
            } else if (sqlAlterTableItem instanceof SQLAlterTableGroupAddTable) {
            }
        }

        if (gsiExists && (!columnOpts.isEmpty() || alters.size() > 1)) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Do not support mix ADD GLOBAL INDEX with other ALTER statements");
        }

        for (SQLAssignItem optionEntry : Optional.ofNullable(x.getTableOptions())
            .orElse(ImmutableList.of())) {

            // Caution all alter table options without specific class moved
            // here.
            if (optionEntry.getTarget() != null && optionEntry.getTarget().toString()
                .equalsIgnoreCase("AUTO_INCREMENT")) {
                if (sequenceBean != null) {
                    throw new TddlNestableRuntimeException(
                        "Incorrect table definition; there can be only one auto column and it must be defined as "
                            + "a key");
                }
                sequenceBean = new SequenceBean();
                sequenceBean.setStart(((SQLIntegerExpr) optionEntry.getValue()).getNumber().longValue());
                sequenceBean.setNew(false);
            }

            tableOptions = convertTableOptions(optionEntry.getTarget().toString(),
                optionEntry.getValue(),
                Optional.ofNullable(tableOptions).orElse(new SqlTableOptions(SqlParserPos.ZERO)));
        }

        List<SqlIdentifier> objectNames = new ArrayList<>();
        SqlIdentifier tableNameIdentifier =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTableName()), SqlParserPos.ZERO);
        objectNames.add(tableNameIdentifier);
        if (x.getName() instanceof SQLPropertyExpr) {
            SQLExpr owner = ((SQLPropertyExpr) x.getName()).getOwner();
            if (owner instanceof SQLPropertyExpr) {
                objectNames.add(
                    new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLPropertyExpr) owner).getName()),
                        SqlParserPos.ZERO));
                objectNames.add(new SqlIdentifier(
                    SQLUtils.normalizeNoTrim(((SQLPropertyExpr) owner).getOwner().toString()),
                    SqlParserPos.ZERO));
            } else {
                objectNames.add(
                    new SqlIdentifier(SQLUtils.normalizeNoTrim(owner.toString()), SqlParserPos.ZERO));
            }
        }

        SqlAlterTable sqlAlterTable = SqlDdlNodes.alterTable(objectNames, tableIdentifier,
            columnOpts,
            x.toString(),
            tableOptions,
            alters,
            SqlParserPos.ZERO);
        sqlAlterTable.setHints(hints);
        this.sqlNode = sqlAlterTable;

        handleForeignKeys(schema, tableName, sqlAlterTable, logicalReferencedTables);

        if (sequenceBean != null) {
            sequenceBean.setSchemaName(schema);
        }
        sqlAlterTable.setAutoIncrement(sequenceBean);

        addPrivilegeVerifyItem(schema, tableName, PrivilegePoint.ALTER);

        return false;
    }

    private void handleForeignKeys(String schemaName, String tableName, SqlAlterTable sqlAlterTable,
                                   List<String> logicalReferencedTables) {
        if (GeneralUtil.isEmpty(logicalReferencedTables)) {
            return;
        }

        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();

        TableRule tableRule = tddlRuleManager.getTableRule(tableName);
        if (tableRule.isBroadcast() || GeneralUtil.isNotEmpty(tableRule.getDbPartitionKeys())
            || GeneralUtil.isNotEmpty(tableRule.getTbPartitionKeys())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Broadcast or sharding table doesn't support foreign keys");
        }

        List<String> physicalReferencedTables = new ArrayList<>();

        for (String logicalReferencedTable : logicalReferencedTables) {
            TableRule refTableRule = tddlRuleManager.getTableRule(logicalReferencedTable);
            if (refTableRule != null && TStringUtil.isNotEmpty(refTableRule.getTbNamePattern())) {
                if (!refTableRule.isBroadcast() && GeneralUtil.isEmpty(refTableRule.getDbPartitionKeys()) &&
                    GeneralUtil.isEmpty(refTableRule.getTbPartitionKeys())) {
                    physicalReferencedTables.add(refTableRule.getTbNamePattern());
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                        "Broadcast or sharding table '" + logicalReferencedTable
                            + "' cannot be referenced as foreign key");
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                    "Referenced table '" + logicalReferencedTable + "' doesn't exist in " + schemaName);
            }
        }

        sqlAlterTable.setLogicalReferencedTables(logicalReferencedTables);
        sqlAlterTable.setPhysicalReferencedTables(physicalReferencedTables);
    }

    @Override
    public boolean visit(SQLAlterTableAddColumn x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);

        final SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);

        for (SQLColumnDefinition sqlColumnDefinition : x.getColumns()) {
            final SqlIdentifier columnName =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(sqlColumnDefinition.getColumnName()), SqlParserPos.ZERO);

            final SqlColumnDeclaration colDef = (SqlColumnDeclaration) convertToSqlNode(sqlColumnDefinition);

            final SqlIdentifier afterColumn = (SqlIdentifier) convertToSqlNode(x.getAfterColumn());

            sqlNodeList.add(
                new SqlAddColumn(tableName, columnName, colDef, x.isFirst(), afterColumn, x.getParent().toString(),
                    SqlParserPos.ZERO));
        }

        this.sqlNode = sqlNodeList;

        return false;
    }

    @Override
    public boolean visit(MySqlAlterTableModifyColumn x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);

        final SqlColumnDeclaration colDef = (SqlColumnDeclaration) convertToSqlNode(x.getNewColumnDefinition());

        final SqlIdentifier afterColumn = (SqlIdentifier) convertToSqlNode(x.getAfterColumn());

        this.sqlNode = new SqlModifyColumn(tableName, colDef.getName(), colDef, x.isFirst(), afterColumn, x.getParent()
            .toString(), SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(MySqlAlterTableChangeColumn x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);

        final SqlIdentifier oldName = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getColumnName().getSimpleName()),
            SqlParserPos.ZERO);

        final SqlColumnDeclaration colDef = (SqlColumnDeclaration) convertToSqlNode(x.getNewColumnDefinition());

        final SqlIdentifier afterColumn = (SqlIdentifier) convertToSqlNode(x.getAfterColumn());

        this.sqlNode = new SqlChangeColumn(tableName,
            oldName,
            colDef.getName(),
            colDef,
            x.isFirst(),
            afterColumn,
            x.getParent().toString(),
            SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropColumnItem x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);

        if (x.getColumns().size() > 1) {
            throw new FastSqlParserException(ExceptionType.NOT_SUPPORT,
                "cannot drop multi columns in one drop column statement");
        }

        final SqlIdentifier columnName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getColumns().get(0).getSimpleName()), SqlParserPos.ZERO);

        this.sqlNode = new SqlDropColumn(tableName, columnName, x.getParent().toString(), SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(MySqlAlterTableAlterColumn x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);

        final SqlIdentifier columnName = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getColumn().getSimpleName()),
            SqlParserPos.ZERO);

        final SqlLiteral defaultVal = (SqlLiteral) convertToSqlNode(x.getDefaultExpr());

        this.sqlNode = new SqlAlterColumnDefaultVal(tableName, columnName, defaultVal, x.isDropDefault(), x.getParent()
            .toString(), SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropIndex x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);
        final SqlIdentifier indexName = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getIndexName().getSimpleName()),
            SqlParserPos.ZERO);

        this.sqlNode = new SqlAlterTableDropIndex(tableName, indexName, x.getParent().toString(), SqlParserPos.ZERO);

        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropFile x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);
        List<SqlIdentifier> fileNames = new ArrayList<>();
        for (SQLExpr expr : x.getFiles()) {
            final SqlIdentifier fileName = new SqlIdentifier(SQLUtils.normalizeNoTrim(expr.toString()),
                SqlParserPos.ZERO);
            fileNames.add(fileName);
        }
        this.sqlNode = new SqlAlterTableDropFile(tableName, fileNames, x.getParent().toString(), SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropKey x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);
        final SqlIdentifier indexName = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getKeyName().getSimpleName()),
            SqlParserPos.ZERO);

        this.sqlNode = new SqlAlterTableDropIndex(tableName, indexName, x.getParent().toString(), SqlParserPos.ZERO);

        return false;
    }

    @Override
    public boolean visit(SQLAlterTableRenameIndex x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);
        final SqlIdentifier indexName = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getName().getSimpleName()),
            SqlParserPos.ZERO);
        final SqlIdentifier newIndexName = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTo().getSimpleName()),
            SqlParserPos.ZERO);

        this.sqlNode = new SqlAlterTableRenameIndex(tableName,
            indexName,
            newIndexName,
            x.getParent().toString(),
            SqlParserPos.ZERO);

        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAddIndex x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);
        final SqlIdentifier indexName = (SqlIdentifier) convertToSqlNode(x.getName());

        final SqlNode dbPartitionBy = convertToSqlNode(x.getDbPartitionBy());
        final SqlNode tablePartitionBy = convertToSqlNode(x.getTablePartitionBy());
        final SqlNode tablePartitions = convertToSqlNode(x.getTablePartitions());
        final SqlNode partitioning = convertToSqlNode(x.getPartitioning());
        final SqlNode tableGroup = convertToSqlNode(x.getTableGroup());
        final List<SqlIndexColumnName> columns = FastSqlConstructUtils.constructIndexColumnNames(x.getColumns());
        final List<SqlIndexColumnName> covering = FastSqlConstructUtils.constructIndexCoveringNames(x.getCovering());
        final SqlIndexConstraintType constraintType = x.isUnique() ? SqlIndexConstraintType.UNIQUE : null;
        final SqlIndexType indexType =
            (x.getIndexDefinition().hasOptions() && x.getIndexDefinition().getOptions().getIndexType() != null) ?
                SqlIndexType.from(x.getIndexDefinition().getOptions().getIndexType().toUpperCase()) :
                null;
        final List<SqlIndexOption> options = new LinkedList<>();
        if (x.getIndexDefinition().hasOptions()) {
            convertIndexOption(options, x.getIndexDefinition().getOptions());
        }
        if (null != x.getComment()) {
            final SqlCharStringLiteral comment = (SqlCharStringLiteral) convertToSqlNode(x.getComment());
            options.add(SqlIndexOption.createComment(SqlParserPos.ZERO, comment));
        }

        if (x.isGlobal() || x.isClustered()) {
            if (null == indexName || indexName.getLastName().isEmpty()) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Global (clustered) secondary index must have a name.");
            }
            if (null == x.getDbPartitionBy()) {
                // Auto dbpartition assign is available.
                if (x.getTablePartitionBy() != null || x.getTablePartitions() != null) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Global (clustered) secondary index should not specify tbpartition without dbpartition.");
                }
            }
        }
        SqlIndexDefinition indexDef = null;
        if (x.isGlobal()) {
            indexDef = SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
                false,
                null,
                x.getType(),
                indexType,
                indexName,
                tableName,
                columns,
                covering,
                dbPartitionBy,
                tablePartitionBy,
                tablePartitions,
                partitioning,
                options,
                tableGroup);
        } else if (x.isClustered()) {
            indexDef = SqlIndexDefinition.clusteredIndex(SqlParserPos.ZERO,
                false,
                null,
                x.getType(),
                indexType,
                indexName,
                tableName,
                columns,
                covering,
                dbPartitionBy,
                tablePartitionBy,
                tablePartitions,
                partitioning,
                options,
                tableGroup);
        } else {
            indexDef = SqlIndexDefinition.localIndex(SqlParserPos.ZERO,
                false,
                null,
                x.getIndexDefinition().isLocal(),
                x.getType(),
                indexType,
                indexName,
                tableName,
                columns,
                options);
            x.getIndexDefinition().setLocal(false); // Remove the flag in the sql.
        }

        if (null == constraintType) {
            this.sqlNode = new SqlAddIndex(SqlParserPos.ZERO, indexName, indexDef);
        } else {
            switch (constraintType) {
            case UNIQUE:
                this.sqlNode = new SqlAddUniqueIndex(SqlParserPos.ZERO, indexName, indexDef);
                break;
            case FULLTEXT:
                this.sqlNode = new SqlAddFullTextIndex(SqlParserPos.ZERO, indexName, indexDef);
                break;
            case SPATIAL:
                this.sqlNode = new SqlAddSpatialIndex(SqlParserPos.ZERO, indexName, indexDef);
                break;
            default:
                throw new FastSqlParserException(ExceptionType.NOT_SUPPORT, "Unsupported constraint type "
                    + constraintType);
            }
        }

        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAddConstraint x) {
        final SQLConstraint constraint = x.getConstraint();
        if (constraint instanceof MysqlForeignKey) {
            final MysqlForeignKey foreignKey = (MysqlForeignKey) constraint;

            final SqlIdentifier referencedTableName =
                (SqlIdentifier) convertToSqlNode(foreignKey.getReferencedTableName());
            final List<SqlIdentifier> referencedColumns = new ArrayList<>();
            for (SQLName column : foreignKey.getReferencedColumns()) {
                referencedColumns.add((SqlIdentifier) FastSqlConstructUtils.convertToSqlNode(column, context, ec));
            }

            MatchType matchType = null;
            if (null != foreignKey.getReferenceMatch()) {
                switch (foreignKey.getReferenceMatch()) {

                case FULL:
                    matchType = MatchType.MATCH_FULL;
                    break;
                case PARTIAL:
                    matchType = MatchType.MATCH_PARTIAL;
                    break;
                case SIMPLE:
                    matchType = MatchType.MATCH_SIMPLE;
                    break;
                default:
                    break;
                }
            }
            List<SqlReferenceOption> referenceOptions = null;
            if (null != foreignKey.getOnDelete()) {
                referenceOptions = new ArrayList<>();

                switch (foreignKey.getOnDelete()) {

                case RESTRICT:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_DELETE,
                        ReferenceOptionType.RESTRICT));
                    break;
                case CASCADE:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_DELETE,
                        ReferenceOptionType.CASCADE));
                    break;
                case SET_NULL:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_DELETE,
                        ReferenceOptionType.SET_NULL));
                    break;
                case NO_ACTION:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_DELETE,
                        ReferenceOptionType.NO_ACTION));
                    break;
                default:
                    break;
                }
            }

            if (null != foreignKey.getOnUpdate()) {
                if (null == referenceOptions) {
                    referenceOptions = new ArrayList<>();
                }
                switch (foreignKey.getOnUpdate()) {

                case RESTRICT:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_UPDATE,
                        ReferenceOptionType.RESTRICT));
                    break;
                case CASCADE:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_UPDATE,
                        ReferenceOptionType.CASCADE));
                    break;
                case SET_NULL:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_UPDATE,
                        ReferenceOptionType.SET_NULL));
                    break;
                case NO_ACTION:
                    referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                        OnType.ON_UPDATE,
                        ReferenceOptionType.NO_ACTION));
                    break;
                default:
                    break;
                }
            }
            final SqlReferenceDefinition referenceDefinition = new SqlReferenceDefinition(SqlParserPos.ZERO,
                referencedTableName,
                referencedColumns,
                matchType,
                referenceOptions);

            final SqlIdentifier tableName =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                    SqlParserPos.ZERO);
            final SqlIdentifier indexName = (SqlIdentifier) convertToSqlNode(foreignKey.getIndexName());
            final SqlIdentifier sqlConstraint = (SqlIdentifier) convertToSqlNode(foreignKey.getName());

            final List<SqlIndexColumnName> columns = new ArrayList<>();
            for (SQLName column : foreignKey.getReferencingColumns()) {
                final SqlIdentifier columnName =
                    (SqlIdentifier) FastSqlConstructUtils.convertToSqlNode(column, context, ec);
                columns.add(new SqlIndexColumnName(SqlParserPos.ZERO, columnName, null, null));
            }

            final List<SqlIndexOption> options = new LinkedList<>();
            if (null != foreignKey.getComment()) {
                final SqlCharStringLiteral comment = (SqlCharStringLiteral) convertToSqlNode(foreignKey.getComment());
                options.add(SqlIndexOption.createComment(SqlParserPos.ZERO, comment));
            }

            final SqlIndexDefinition indexDef = SqlIndexDefinition.localIndex(SqlParserPos.ZERO,
                false,
                null,
                true, // Foreign key treated as local.
                null,
                null,
                indexName,
                tableName,
                columns,
                options);

            this.sqlNode = new SqlAddForeignKey(SqlParserPos.ZERO,
                indexName,
                indexDef,
                sqlConstraint,
                referenceDefinition);

            return false;
        } else if (constraint instanceof MySqlUnique) {
            final MySqlUnique uniqueIndex = (MySqlUnique) constraint;

            final SqlIdentifier tableName =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                    SqlParserPos.ZERO);
            final SqlIdentifier indexName = (SqlIdentifier) convertToSqlNode(uniqueIndex.getName());

            final SqlNode dbPartitionBy = convertToSqlNode(uniqueIndex.getDbPartitionBy());
            final SqlNode tablePartitionBy = convertToSqlNode(uniqueIndex.getTablePartitionBy());
            final SqlNode tablePartitions = convertToSqlNode(uniqueIndex.getTablePartitions());
            final SqlNode partitioning = convertToSqlNode(uniqueIndex.getPartitioning());
            final List<SqlIndexColumnName> columns =
                FastSqlConstructUtils.constructIndexColumnNames(uniqueIndex.getColumns());
            final List<SqlIndexColumnName> covering =
                FastSqlConstructUtils.constructIndexCoveringNames(uniqueIndex.getCovering());
            final SqlIndexConstraintType constraintType = SqlIndexConstraintType.UNIQUE;
            final SqlIndexType indexType = (uniqueIndex.getIndexDefinition().hasOptions()
                && uniqueIndex.getIndexDefinition().getOptions().getIndexType() != null) ?
                SqlIndexType.from(uniqueIndex.getIndexDefinition().getOptions().getIndexType().toUpperCase()) :
                null;
            final List<SqlIndexOption> options = new LinkedList<>();
            if (uniqueIndex.getIndexDefinition().hasOptions()) {
                convertIndexOption(options, uniqueIndex.getIndexDefinition().getOptions());
            }
            if (null != uniqueIndex.getComment()) {
                final SqlCharStringLiteral comment = (SqlCharStringLiteral) convertToSqlNode(uniqueIndex.getComment());
                options.add(SqlIndexOption.createComment(SqlParserPos.ZERO, comment));
            }

            if (uniqueIndex.isGlobal() || uniqueIndex.isClustered()) {
                if (null == uniqueIndex.getName() || uniqueIndex.getName().getSimpleName().isEmpty()) {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Global (clustered) secondary index must have a name.");
                }
                if (null == uniqueIndex.getDbPartitionBy() && uniqueIndex.isGlobal()) {
                    // Auto dbpartition assign is available.
                    if (uniqueIndex.getTablePartitionBy() != null || uniqueIndex.getTablePartitions() != null) {
                        throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                            "Global (clustered) secondary index should not specify tbpartition without dbpartition.");
                    }
                }
            }
            SqlIndexDefinition indexDef = null;
            if (uniqueIndex.isGlobal()) {
                indexDef = SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
                    false,
                    null,
                    uniqueIndex.getIndexDefinition().getType(),
                    indexType,
                    indexName,
                    tableName,
                    columns,
                    covering,
                    dbPartitionBy,
                    tablePartitionBy,
                    tablePartitions,
                    partitioning,
                    options,
                    null);
            } else if (uniqueIndex.isClustered()) {
                indexDef = SqlIndexDefinition.clusteredIndex(SqlParserPos.ZERO,
                    false,
                    null,
                    uniqueIndex.getIndexDefinition().getType(),
                    indexType,
                    indexName,
                    tableName,
                    columns,
                    covering,
                    dbPartitionBy,
                    tablePartitionBy,
                    tablePartitions,
                    partitioning,
                    options,
                    null);
            } else {
                indexDef = SqlIndexDefinition.localIndex(SqlParserPos.ZERO,
                    false,
                    null,
                    uniqueIndex.isLocal(),
                    uniqueIndex.getIndexDefinition().getType(),
                    indexType,
                    indexName,
                    tableName,
                    columns,
                    options);
                uniqueIndex.setLocal(false); // Remove the flag in the sql.
            }

            if (null == constraintType) {
                this.sqlNode = new SqlAddIndex(SqlParserPos.ZERO, indexName, indexDef);
            } else {
                switch (constraintType) {
                case UNIQUE:
                    this.sqlNode = new SqlAddUniqueIndex(SqlParserPos.ZERO, indexName, indexDef);
                    break;
                case FULLTEXT:
                    this.sqlNode = new SqlAddFullTextIndex(SqlParserPos.ZERO, indexName, indexDef);
                    break;
                case SPATIAL:
                    this.sqlNode = new SqlAddSpatialIndex(SqlParserPos.ZERO, indexName, indexDef);
                    break;
                default:
                    throw new FastSqlParserException(ExceptionType.NOT_SUPPORT, "Unsupported constraint type "
                        + constraintType);
                }
            }

            return false;
        } else if (constraint instanceof MySqlPrimaryKey) {
            final MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) constraint;

            final SqlIdentifier tableName =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                    SqlParserPos.ZERO);

            final List<SqlIndexColumnName> columns =
                FastSqlConstructUtils.constructIndexColumnNames(primaryKey.getColumns());

            this.sqlNode = new SqlAddPrimaryKey(SqlParserPos.ZERO, tableName, columns);

            return false;
        } else {
            throw new AssertionError("not supported");
        }
    }

    @Override
    public boolean visit(SQLAlterTableEnableKeys x) {
        this.sqlNode = new SqlEnableKeys(SqlParserPos.ZERO, EnableType.ENABLE);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDisableKeys x) {
        this.sqlNode = new SqlEnableKeys(SqlParserPos.ZERO, EnableType.DISABLE);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropPrimaryKey x) {
        final SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLAlterTableStatement) x.getParent()).getTableName()),
                SqlParserPos.ZERO);
        this.sqlNode = new SqlDropPrimaryKey(tableName, x.getParent().toString(), SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableConvertCharSet x) {
        final SQLExpr charset = x.getCharset();
        final SQLExpr collate = x.getCollate();
        final String charsetName;
        final String collateName;
        if (charset != null) {
            if (charset instanceof SQLIdentifierExpr) {
                charsetName = ((SQLIdentifierExpr) charset).getName();
            } else {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Unknown character set name.");
            }
        } else {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Unknown character set name.");
        }
        if (collate != null) {
            if (collate instanceof SQLIdentifierExpr) {
                collateName = ((SQLIdentifierExpr) collate).getName();
            } else {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Unknown collate name.");
            }
        } else {
            collateName = null;
        }
        this.sqlNode = new SqlConvertToCharacterSet(charsetName, collateName, SqlParserPos.ZERO);
        return false;
    }

    public SequenceBean convertTableElements(SqlIdentifier tableName, List<SQLTableElement> tableElementList,
                                             ContextParameters context, TableElementBean result) {
        if (GeneralUtil.isEmpty(tableElementList)) {
            return null;
        }

        // Collect all index names and then assign no names.
        Set<String> indexNamesSet = new HashSet<>();
        for (final SQLTableElement tableElement : tableElementList) {
            if (tableElement instanceof SQLConstraintImpl) {
                final SQLConstraintImpl constraint = (SQLConstraintImpl) tableElement;
                final SqlIdentifier indexName = (SqlIdentifier) convertToSqlNode(constraint.getName());
                if (indexName != null && !indexName.getLastName().isEmpty()) {
                    indexNamesSet.add(indexName.getLastName());
                }
            }
        }

        SequenceBean sequence = null;
        for (final SQLTableElement tableElement : tableElementList) {
            if (tableElement instanceof SQLColumnDefinition) {
                final SqlColumnDeclaration sqlColumnDeclaration = (SqlColumnDeclaration) convertToSqlNode(tableElement);
                result.addColDef(sqlColumnDeclaration);

                if (sqlColumnDeclaration.isAutoIncrement()) {
                    sequence = FastSqlConstructUtils.initSequenceBean(sqlColumnDeclaration);
                }
            } else if (tableElement instanceof SQLConstraintImpl) {
                final SQLConstraintImpl constraint = (SQLConstraintImpl) tableElement;

                // Assign name if no name.
                if (!(tableElement instanceof MySqlPrimaryKey) &&
                    (null == constraint.getName() || constraint.getName().getSimpleName().isEmpty())) {
                    final String baseName = "i_";
                    int prob = 0;
                    while (indexNamesSet.contains(baseName + prob)) {
                        ++prob;
                    }
                    constraint.setName(baseName + prob);
                    indexNamesSet.add(baseName + prob);
                }

                final SqlIdentifier indexName = (SqlIdentifier) convertToSqlNode(constraint.getName());
                final List<SqlIndexOption> options = new LinkedList<>();

                if (tableElement instanceof MySqlTableIndex) {
                    final MySqlTableIndex tableIndex = (MySqlTableIndex) tableElement;

                    final SqlNode dbPartitionBy = convertToSqlNode(tableIndex.getDbPartitionBy());
                    final SqlNode tablePartitionBy = convertToSqlNode(tableIndex.getTablePartitionBy());
                    final SqlNode tablePartitions = convertToSqlNode(tableIndex.getTablePartitions());
                    final SqlNode partitioning = convertToSqlNode(tableIndex.getPartitioning());
                    final SqlNode tableGroup = convertToSqlNode(tableIndex.getTableGroup());
                    final List<SqlIndexColumnName> columns =
                        FastSqlConstructUtils.constructIndexColumnNames(tableIndex.getColumns());
                    final List<SqlIndexColumnName> covering =
                        FastSqlConstructUtils.constructIndexCoveringNames(tableIndex.getCovering());
                    final SqlIndexConstraintType constraintType = null;
                    SqlIndexType indexType = null;
                    if (tableIndex.getIndexDefinition().hasOptions()) {
                        indexType = null == tableIndex.getIndexDefinition().getOptions().getIndexType() ?
                            null :
                            SqlIndexType
                                .from(tableIndex.getIndexDefinition().getOptions().getIndexType().toUpperCase());
                    }
                    if (tableIndex.getIndexDefinition().hasOptions()) {
                        convertIndexOption(options, tableIndex.getIndexDefinition().getOptions());
                    }

                    if (tableIndex.isGlobal() || tableIndex.isClustered()) {
                        if (null == indexName || indexName.getLastName().isEmpty()) {
                            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                                "Global (clustered) secondary index must have a name.");
                        }
                        if (null == tableIndex.getDbPartitionBy()) {
                            // Auto dbpartition assign is available.
                            if (tableIndex.getTablePartitionBy() != null || tableIndex.getTablePartitions() != null) {
                                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                                    "Global (clustered) secondary index should not specify tbpartition without dbpartition.");
                            }
                        }
                    }
                    SqlIndexDefinition indexDef = null;
                    if (tableIndex.isGlobal()) {
                        indexDef = SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
                            false,
                            null,
                            tableIndex.getIndexType(),
                            indexType,
                            indexName,
                            tableName,
                            columns,
                            GeneralUtil.isEmpty(covering) ? null : covering,
                            dbPartitionBy,
                            tablePartitionBy,
                            tablePartitions,
                            partitioning,
                            options,
                            tableGroup);
                        result.addGlobalKey(indexDef);
                    } else if (tableIndex.isClustered()) {
                        indexDef = SqlIndexDefinition.clusteredIndex(SqlParserPos.ZERO,
                            false,
                            null,
                            tableIndex.getIndexType(),
                            indexType,
                            indexName,
                            tableName,
                            columns,
                            GeneralUtil.isEmpty(covering) ? null : covering,
                            dbPartitionBy,
                            tablePartitionBy,
                            tablePartitions,
                            partitioning,
                            options,
                            tableGroup);
                        result.addClusteredKey(indexDef);
                    } else {
                        indexDef = SqlIndexDefinition.localIndex(SqlParserPos.ZERO,
                            false,
                            null,
                            tableIndex.isLocal(),
                            tableIndex.getIndexType(),
                            indexType,
                            indexName,
                            tableName,
                            columns,
                            options);
                        result.addKey(indexDef);
                        tableIndex.setLocal(false); // Remove the flag in the sql.
                    }
                } else if (tableElement instanceof MySqlKey) {
                    final MySqlKey mySqlKey = (MySqlKey) tableElement;
                    final List<SqlIndexColumnName> columns =
                        FastSqlConstructUtils.constructIndexColumnNames(mySqlKey.getColumns());
                    final List<SqlIndexColumnName> covering =
                        FastSqlConstructUtils.constructIndexCoveringNames(mySqlKey.getCovering());
                    final SqlIndexType indexType =
                        null == mySqlKey.getIndexType() ? null : SqlIndexType.from(mySqlKey.getIndexType()
                            .toUpperCase());
                    if (null != mySqlKey.getKeyBlockSize()) {
                        options.add(SqlIndexOption.createKeyBlockSize(SqlParserPos.ZERO,
                            (SqlLiteral) mySqlKey.getKeyBlockSize()));
                    }

                    if (tableElement instanceof MySqlPrimaryKey) {
                        result.primaryKey = SqlIndexDefinition.localIndex(SqlParserPos.ZERO,
                            false,
                            null,
                            true, // PK treated as local.
                            "PRIMARY",
                            indexType,
                            indexName,
                            tableName,
                            columns,
                            options);
                    } else if (tableElement instanceof MySqlUnique) {
                        final MySqlUnique uniqueIndex = (MySqlUnique) tableElement;

                        if (uniqueIndex.isGlobal() || uniqueIndex.isClustered()) {
                            if (null == uniqueIndex.getName() || uniqueIndex.getName().getSimpleName().isEmpty()) {
                                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                                    "Global (clustered) secondary index must have a name.");
                            }
                            if (null == uniqueIndex.getDbPartitionBy()) {
                                // Auto dbpartition assign is available.
                                if (uniqueIndex.getTablePartitionBy() != null
                                    || uniqueIndex.getTablePartitions() != null) {
                                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                                        "Global (clustered) secondary index should not specify tbpartition without dbpartition.");
                                }
                            }
                        }
                        SqlIndexDefinition indexDef = null;
                        if (uniqueIndex.isGlobal()) {
                            final SqlNode dbPartitionBy = convertToSqlNode(uniqueIndex.getDbPartitionBy());
                            final SqlNode tablePartitionBy = convertToSqlNode(uniqueIndex.getTablePartitionBy());
                            final SqlNode tablePartitions = convertToSqlNode(uniqueIndex.getTablePartitions());
                            final SqlNode partitioning = convertToSqlNode(uniqueIndex.getPartitioning());
                            final SqlNode tableGroup = convertToSqlNode(uniqueIndex.getTableGroup());
                            indexDef = SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
                                false,
                                null,
                                uniqueIndex.getIndexDefinition().getType(),
                                indexType,
                                indexName,
                                tableName,
                                columns,
                                GeneralUtil.isEmpty(covering) ? null : covering,
                                dbPartitionBy,
                                tablePartitionBy,
                                tablePartitions,
                                partitioning,
                                options,
                                tableGroup);
                            result.addGlobalUniqueKey(indexDef);
                        } else if (uniqueIndex.isClustered()) {
                            final SqlNode dbPartitionBy = convertToSqlNode(uniqueIndex.getDbPartitionBy());
                            final SqlNode tablePartitionBy = convertToSqlNode(uniqueIndex.getTablePartitionBy());
                            final SqlNode tablePartitions = convertToSqlNode(uniqueIndex.getTablePartitions());
                            final SqlNode partitioning = convertToSqlNode(uniqueIndex.getPartitioning());
                            final SqlNode tableGroup = convertToSqlNode(uniqueIndex.getTableGroup());

                            indexDef = SqlIndexDefinition.clusteredIndex(SqlParserPos.ZERO,
                                false,
                                null,
                                uniqueIndex.getIndexDefinition().getType(),
                                indexType,
                                indexName,
                                tableName,
                                columns,
                                GeneralUtil.isEmpty(covering) ? null : covering,
                                dbPartitionBy,
                                tablePartitionBy,
                                tablePartitions,
                                partitioning,
                                options,
                                tableGroup);
                            result.addClusteredUniqueKey(indexDef);
                        } else {
                            indexDef = SqlIndexDefinition.localIndex(SqlParserPos.ZERO,
                                false,
                                null,
                                uniqueIndex.isLocal(),
                                uniqueIndex.getIndexDefinition().getType(),
                                indexType,
                                indexName,
                                tableName,
                                columns,
                                options);
                            result.addUniqueKey(indexDef);
                            uniqueIndex.setLocal(false); // Remove the flag in the sql.
                        }
                    } else {
                        final SqlIndexDefinition indexDef = SqlIndexDefinition.localIndex(SqlParserPos.ZERO,
                            false,
                            null,
                            mySqlKey.getIndexDefinition().isLocal(),
                            mySqlKey.getIndexDefinition().getType(),
                            indexType,
                            indexName,
                            tableName,
                            columns,
                            options);
                        if ("FULLTEXT".equalsIgnoreCase(mySqlKey.getIndexType())) {
                            result.addFullTextKey(indexDef);
                        } else if ("SPATIAL".equalsIgnoreCase(mySqlKey.getIndexType())) {
                            result.addSpatialKey(indexDef);
                        } else {
                            result.addKey(indexDef);
                        }
                        mySqlKey.getIndexDefinition().setLocal(false); // Remove the flag in the sql.
                    }
                } else if (tableElement instanceof MysqlForeignKey) {
                    final MysqlForeignKey foreignKey = (MysqlForeignKey) tableElement;
                    result.addLogicalReferencedTable(foreignKey.getReferencedTableName().getSimpleName());
                    // TODO
                }

                if (null != constraint.getComment()) {
                    final SqlCharStringLiteral comment =
                        (SqlCharStringLiteral) convertToSqlNode(constraint.getComment());
                    options.add(SqlIndexOption.createComment(SqlParserPos.ZERO, comment));
                }
            }

        }
        return sequence;
    }

    private void convertIndexOption(List<SqlIndexOption> options, SQLIndexOptions fastSqlOptions) {
        if (fastSqlOptions.getKeyBlockSize() != null) {
            options.add(SqlIndexOption.createKeyBlockSize(SqlParserPos.ZERO,
                (SqlLiteral) convertToSqlNode(fastSqlOptions.getKeyBlockSize())));
        }
        if (fastSqlOptions.getParserName() != null) {
            options.add(SqlIndexOption.createParserName(SqlParserPos.ZERO,
                new SqlIdentifier(fastSqlOptions.getParserName(), SqlParserPos.ZERO)));
        }
    }

    private static class TableElementBean {

        public List<Pair<SqlIdentifier, SqlColumnDeclaration>> colDefs;
        public SqlIndexDefinition primaryKey;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> uniqueKeys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> globalUniqueKeys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> clusteredKeys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> clusteredUniqueKeys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> keys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> fullTextKeys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> spatialKeys;
        public List<Pair<SqlIdentifier, SqlIndexDefinition>> foreignKeys;
        public List<SqlCall> checks;

        public List<String> logicalReferencedTables;

        public TableElementBean addColDef(SqlColumnDeclaration columnDeclaration) {
            if (null == colDefs) {
                colDefs = new ArrayList<>();
            }
            colDefs.add(Pair.of(columnDeclaration.getName(), columnDeclaration));
            return this;
        }

        public TableElementBean addUniqueKey(SqlIndexDefinition primaryKey) {
            if (null == uniqueKeys) {
                uniqueKeys = new ArrayList<>();
            }

            uniqueKeys.add(Pair.of(primaryKey.getIndexName(), primaryKey));
            return this;
        }

        public TableElementBean addGlobalKey(SqlIndexDefinition globalKey) {
            if (null == globalKeys) {
                globalKeys = new ArrayList<>();
            }

            globalKeys.add(Pair.of(globalKey.getIndexName(), globalKey));
            return this;
        }

        public TableElementBean addClusteredKey(SqlIndexDefinition clusteredKey) {
            if (null == clusteredKeys) {
                clusteredKeys = new ArrayList<>();
            }

            clusteredKeys.add(Pair.of(clusteredKey.getIndexName(), clusteredKey));
            return this;
        }

        public TableElementBean addKey(SqlIndexDefinition key) {
            if (null == keys) {
                keys = new ArrayList<>();
            }

            keys.add(Pair.of(key.getIndexName(), key));
            return this;
        }

        public TableElementBean addGlobalUniqueKey(SqlIndexDefinition globaUniquelKey) {
            if (null == globalUniqueKeys) {
                globalUniqueKeys = new ArrayList<>();
            }

            globalUniqueKeys.add(Pair.of(globaUniquelKey.getIndexName(), globaUniquelKey));
            return this;
        }

        public TableElementBean addClusteredUniqueKey(SqlIndexDefinition clusteredUniqueKey) {
            if (null == clusteredUniqueKeys) {
                clusteredUniqueKeys = new ArrayList<>();
            }

            clusteredUniqueKeys.add(Pair.of(clusteredUniqueKey.getIndexName(), clusteredUniqueKey));
            return this;
        }

        public TableElementBean addFullTextKey(SqlIndexDefinition fullTextKey) {
            if (null == fullTextKeys) {
                fullTextKeys = new ArrayList<>();
            }

            fullTextKeys.add(Pair.of(fullTextKey.getIndexName(), fullTextKey));
            return this;
        }

        public TableElementBean addSpatialKey(SqlIndexDefinition spatialKey) {
            if (null == spatialKeys) {
                spatialKeys = new ArrayList<>();
            }

            spatialKeys.add(Pair.of(spatialKey.getIndexName(), spatialKey));
            return this;
        }

        public TableElementBean addForeignKey(SqlIndexDefinition foreignKey) {
            if (null == foreignKeys) {
                foreignKeys = new ArrayList<>();
            }

            foreignKeys.add(Pair.of(foreignKey.getIndexName(), foreignKey));
            return this;
        }

        public TableElementBean addLogicalReferencedTable(String logicalReferencedTable) {
            if (null == logicalReferencedTables) {
                logicalReferencedTables = new ArrayList<>();
            }
            logicalReferencedTables.add(logicalReferencedTable);
            return this;
        }
    }

    @Override
    public boolean visit(SQLColumnReference x) {
        SqlIdentifier tableName = (SqlIdentifier) FastSqlConstructUtils.convertToSqlNode(x.getTable(), context, ec);
        List<SqlIdentifier> columns = new ArrayList<>();
        for (SQLName column : x.getColumns()) {
            columns.add((SqlIdentifier) FastSqlConstructUtils.convertToSqlNode(column, context, ec));
        }

        MatchType matchType = null;
        if (null != x.getReferenceMatch()) {
            switch (x.getReferenceMatch()) {

            case FULL:
                matchType = MatchType.MATCH_FULL;
                break;
            case PARTIAL:
                matchType = MatchType.MATCH_PARTIAL;
                break;
            case SIMPLE:
                matchType = MatchType.MATCH_SIMPLE;
                break;
            default:
                break;
            }
        }
        List<SqlReferenceOption> referenceOptions = null;
        if (null != x.getOnDelete()) {
            referenceOptions = new ArrayList<>();

            switch (x.getOnDelete()) {

            case RESTRICT:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_DELETE,
                    ReferenceOptionType.RESTRICT));
                break;
            case CASCADE:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_DELETE,
                    ReferenceOptionType.CASCADE));
                break;
            case SET_NULL:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_DELETE,
                    ReferenceOptionType.SET_NULL));
                break;
            case NO_ACTION:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_DELETE,
                    ReferenceOptionType.NO_ACTION));
                break;
            default:
                break;
            }
        }

        if (null != x.getOnUpdate()) {
            if (null == referenceOptions) {
                referenceOptions = new ArrayList<>();
            }
            switch (x.getOnUpdate()) {

            case RESTRICT:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_UPDATE,
                    ReferenceOptionType.RESTRICT));
                break;
            case CASCADE:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_UPDATE,
                    ReferenceOptionType.CASCADE));
                break;
            case SET_NULL:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_UPDATE,
                    ReferenceOptionType.SET_NULL));
                break;
            case NO_ACTION:
                referenceOptions.add(new SqlReferenceOption(SqlParserPos.ZERO,
                    OnType.ON_UPDATE,
                    ReferenceOptionType.NO_ACTION));
                break;
            default:
                break;
            }
        }
        sqlNode = new SqlReferenceDefinition(SqlParserPos.ZERO, tableName, columns, matchType, referenceOptions);
        return false;
    }

    @Override
    public boolean visit(SQLColumnDefinition x) {
        this.sqlNode = FastSqlConstructUtils.convertColumnDefinition(x, context, ec);
        return false;
    }

    @Override
    public boolean visit(SQLCreateDatabaseStatement x) {
        final SqlIdentifier dbName = (SqlIdentifier) convertToSqlNode(x.getName());
        final SqlNode localityNode = convertToSqlNode(x.getLocality());
        final String locality = localityNode == null ? "" : RelUtils.stringValue(localityNode);

        final SqlNode partMode = convertToSqlNode(x.getPartitionMode());
        final String partModeVal = partMode == null ? "" : RelUtils.stringValue(partMode);

        this.sqlNode = SqlDdlNodes.createDatabase(SqlParserPos.ZERO,
            x.isIfNotExists(),
            dbName,
            x.getCharacterSet(),
            x.getCollate(),
            locality,
            partModeVal);
        addPrivilegeVerifyItem(dbName.getSimple(), null, PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(SQLChangeRoleStatement x) {
        SqlNode targetType = convertToSqlNode(x.getTargetType());
        SqlNode target = convertToSqlNode(x.getTarget());
        SqlNode role = convertToSqlNode(x.getRole());
        this.sqlNode = new SqlChangeConsensusRole(SqlParserPos.ZERO, targetType, target, role);
        return false;
    }

    @Override
    public boolean visit(SQLAlterSystemSetConfigStatement x) {
        String primaryZone = "";
        for (SQLAssignItem assignItem : x.getOptions()) {
            String target = RelUtils.stringValue(convertToSqlNode(assignItem.getTarget()));
            if ("primary_zone".equalsIgnoreCase(target)) {
                primaryZone = RelUtils.stringValue(convertToSqlNode(assignItem.getValue()));
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "set config " + target);
            }
        }
        this.sqlNode = new SqlAlterSystemSetConfig(SqlParserPos.ZERO, primaryZone);
        return false;
    }

    @Override
    public boolean visit(SQLDropDatabaseStatement x) {
        final SqlIdentifier dbName = (SqlIdentifier) convertToSqlNode(x.getDatabase());

        this.sqlNode = SqlDdlNodes.dropDatabase(SqlParserPos.ZERO, x.isIfExists(), dbName);
        addPrivilegeVerifyItem(dbName.getSimple(), null, PrivilegePoint.DROP);
        return false;
    }

    protected List<Pair<SqlNode, SqlNode>> parseStorageSpecification(List<SQLAssignItem> storageItem) {
        final List<Pair<SqlNode, SqlNode>> storageSpec = new LinkedList<>();

        for (SQLAssignItem storageProp : storageItem) {
            final SqlNode key = convertToSqlNode(storageProp.getTarget());
            final SqlNode value = convertToSqlNode(storageProp.getValue());
            storageSpec.add(Pair.of(key, value));
        }
        return storageSpec;
    }

    @Override
    public boolean visit(SQLAggregateExpr x) {
        SqlOperator functionOperator;

        String methodName = x.getMethodName();
        if (isNotSupportFunction(methodName)) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "Not supported method:"
                + methodName);
        }

        long hashCode64 = x.methodNameHashCode64();

        if (hashCode64 == FnvHash.Constants.COUNT) {
            functionOperator = SqlStdOperatorTable.COUNT;

        } else if (hashCode64 == FnvHash.Constants.SUM) {
            functionOperator = SqlStdOperatorTable.SUM;

        } else if (hashCode64 == FnvHash.Constants.MAX) {
            functionOperator = SqlStdOperatorTable.MAX;

        } else if (hashCode64 == FnvHash.Constants.MIN) {
            functionOperator = SqlStdOperatorTable.MIN;
        } else if (hashCode64 == FnvHash.Constants.AVG) {
            functionOperator = SqlStdOperatorTable.AVG;
        } else if (hashCode64 == FnvHash.Constants.CHECK_SUM) {
            functionOperator = SqlStdOperatorTable.CHECK_SUM;
        } else if (hashCode64 == FnvHash.Constants.GROUP_CONCAT) {
            return visitGroupConcat(x);
        } else if (hashCode64 == FnvHash.Constants.ROW_NUMBER) {
            functionOperator = SqlStdOperatorTable.ROW_NUMBER;
        } else if (hashCode64 == FnvHash.Constants.RANK) {
            functionOperator = SqlStdOperatorTable.RANK;
        } else if (hashCode64 == FnvHash.Constants.DENSE_RANK) {
            functionOperator = SqlStdOperatorTable.DENSE_RANK;
        } else if (hashCode64 == FnvHash.Constants.PERCENT_RANK) {
            functionOperator = SqlStdOperatorTable.PERCENT_RANK;
        } else if (hashCode64 == FnvHash.Constants.CUME_DIST) {
            functionOperator = SqlStdOperatorTable.CUME_DIST;
        } else if (hashCode64 == FnvHash.Constants.FIRST_VALUE) {
            functionOperator = SqlStdOperatorTable.FIRST_VALUE;
        } else if (hashCode64 == FnvHash.Constants.LAST_VALUE) {
            functionOperator = SqlStdOperatorTable.LAST_VALUE;
        } else if (hashCode64 == FnvHash.Constants.NTH_VALUE) {
            functionOperator = SqlStdOperatorTable.NTH_VALUE;
        } else if (hashCode64 == FnvHash.Constants.NTILE) {
            functionOperator = SqlStdOperatorTable.NTILE;
        } else if (hashCode64 == FnvHash.Constants.LEAD) {
            functionOperator = SqlStdOperatorTable.LEAD;
        } else if (hashCode64 == FnvHash.Constants.LAG) {
            functionOperator = SqlStdOperatorTable.LAG;
        } else {
            functionOperator = new SqlUnresolvedFunction(new SqlIdentifier(methodName, SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);

        }

        SqlLiteral functionQualifier = null;

        if (x.getOption() == SQLAggregateOption.DISTINCT) {
            functionQualifier = SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO);
        }
        List<SQLExpr> arguments = x.getArguments();
        List<SqlNode> argNodes = new ArrayList<SqlNode>(arguments.size());
        for (int i = 0, size = arguments.size(); i < size; ++i) {
            argNodes.add(convertToSqlNode(arguments.get(i)));
        }
        SqlBasicCall functionNode = new SqlBasicCall(functionOperator,
            SqlParserUtil.toNodeArray(argNodes),
            SqlParserPos.ZERO,
            false,
            functionQualifier);
        SQLOver over = null;
        if (x.getOver() == null) {
            List<SQLWindow> windows = (List<SQLWindow>) context.getParameter(ContextParameterKey.WINDOW);

            if (windows != null) {
                for (int i = 0; i < windows.size(); i++) {
                    if (windows.get(i).getName().equals(x.getOverRef())) {
                        over = windows.get(i).getOver();
                    }
                }
            }
        } else {
            over = x.getOver();
        }
        if (over == null) {
            this.sqlNode = functionNode;
            return false;
        }
        if (!supportWinFunction(hashCode64)) {
            throw new UnsupportedOperationException(
                "The method '" + x.getMethodName() + "' is not yet in the window function support list.");
        }
        SqlWindow sqlWindow = getSqlWindow(x, over, functionOperator);
        SqlNode[] operands = new SqlNode[] {functionNode, sqlWindow};
        this.sqlNode = new SqlBasicCall(SqlStdOperatorTable.OVER, operands, SqlParserPos.ZERO);
        return false;
    }

    private boolean supportWinFunction(long hashCode64) {
        if (hashCode64 == FnvHash.Constants.DENSE_RANK ||
            hashCode64 == FnvHash.Constants.RANK ||
            hashCode64 == FnvHash.Constants.ROW_NUMBER ||
            hashCode64 == FnvHash.Constants.SUM ||
            hashCode64 == FnvHash.Constants.COUNT ||
            hashCode64 == FnvHash.Constants.AVG ||
            hashCode64 == FnvHash.Constants.MAX ||
            hashCode64 == FnvHash.Constants.MIN ||
            hashCode64 == FnvHash.Constants.PERCENT_RANK ||
            hashCode64 == FnvHash.Constants.CUME_DIST ||
            hashCode64 == FnvHash.Constants.FIRST_VALUE ||
            hashCode64 == FnvHash.Constants.LAST_VALUE ||
            hashCode64 == FnvHash.Constants.NTH_VALUE ||
            hashCode64 == FnvHash.Constants.NTILE ||
            hashCode64 == FnvHash.Constants.LAG ||
            hashCode64 == FnvHash.Constants.LEAD
        ) {
            return true;
        }
        return false;
    }

    private SqlWindow getSqlWindow(SQLAggregateExpr x, SQLOver over, SqlOperator functionOperator) {
        SqlNodeList partitionByList = new SqlNodeList(SqlParserPos.ZERO);
        for (SQLExpr tempNode : over.getPartitionBy()) {
            partitionByList.add(convertToSqlNode(tempNode));
        }
        SqlNodeList orderByList = new SqlNodeList(SqlParserPos.ZERO);
        if (over.getOrderBy() != null) {
            for (SQLSelectOrderByItem tempNode : over.getOrderBy().getItems()) {
                orderByList.add(convertToSqlNode(tempNode));
            }
        }
        //rank dense rank need argNode from order
        SqlNode lowerBound = null, upperBound = null;
        SQLOver.WindowingType windowingType = over.getWindowingType();
        if (functionOperator == SqlStdOperatorTable.PERCENT_RANK ||
            functionOperator == SqlStdOperatorTable.CUME_DIST ||
            functionOperator == SqlStdOperatorTable.NTILE ||
            functionOperator == SqlStdOperatorTable.LAG ||
            functionOperator == SqlStdOperatorTable.LEAD) {
            windowingType = SQLOver.WindowingType.ROWS;
            upperBound = SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
            lowerBound = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
        } else if (windowingType == null) {
            if (functionOperator == SqlStdOperatorTable.ROW_NUMBER ||
                functionOperator == SqlStdOperatorTable.RANK ||
                functionOperator == SqlStdOperatorTable.DENSE_RANK) {
                if (over.getWindowingBetweenBeginBound() != null
                    || over.getWindowingBetweenEndBound() != null) {

                    throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                        "ROW/RANGE not allowed with RANK, DENSE_RANK or ROW_NUMBER functions!");
                }
                windowingType = SQLOver.WindowingType.ROWS;
            } else if (over.getOrderBy() == null) {
                upperBound = SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
                lowerBound = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
                windowingType = SQLOver.WindowingType.RANGE;
            } else {
                upperBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
                windowingType = SQLOver.WindowingType.RANGE;
                lowerBound = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
            }
        } else {
            SQLOver.WindowingBound beginBound = over.getWindowingBetweenBeginBound();
            SQLOver.WindowingBound endBound = over.getWindowingBetweenEndBound();
            if ((beginBound == SQLOver.WindowingBound.FOLLOWING) || (beginBound
                == SQLOver.WindowingBound.UNBOUNDED_FOLLOWING)
                || ((endBound != null) && (endBound == SQLOver.WindowingBound.PRECEDING
                || endBound == SQLOver.WindowingBound.UNBOUNDED_PRECEDING))) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARSER, "Frame is invalid!");
            }
            if (over.getWindowingBetweenBeginBound() == SQLOver.WindowingBound.CURRENT_ROW) {
                lowerBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
            } else if (over.getWindowingBetweenBeginBound() == SQLOver.WindowingBound.UNBOUNDED_PRECEDING) {
                lowerBound = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
            } else {
                lowerBound = SqlWindow
                    .createPreceding(convertToSqlNode(over.getWindowingBetweenBegin()), SqlParserPos.ZERO);
            }
            if (over.getWindowingBetweenEndBound() == null) {
                upperBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
            } else if (over.getWindowingBetweenEndBound() == SQLOver.WindowingBound.CURRENT_ROW) {
                upperBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
            } else if (over.getWindowingBetweenEndBound() == SQLOver.WindowingBound.UNBOUNDED_FOLLOWING) {
                upperBound = SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
            } else {
                upperBound = SqlWindow
                    .createFollowing(convertToSqlNode(over.getWindowingBetweenEnd()), SqlParserPos.ZERO);
            }
        }

        boolean isRows = windowingType != SQLOver.WindowingType.RANGE;
        return SqlWindow.create(null, null,
            partitionByList, orderByList,
            SqlLiteral.createBoolean(
                isRows,
                SqlParserPos.ZERO),
            lowerBound, upperBound,
            null, SqlParserPos.ZERO);
    }

    @Override
    public boolean visit(SQLWindow x) {
        return super.visit(x);
    }

    private boolean visitGroupConcat(SQLAggregateExpr x) {
        SqlLiteral functionQualifier = null;

        if (x.getOption() == SQLAggregateOption.DISTINCT) {
            functionQualifier = SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO);
        }
        // to support group_concat, we need attributes from SQLAggregateExpr x
        Map<String, Object> attributes = x.getAttributes();
        String separator = null;
        ArrayList<SqlNode> orderOperands = new ArrayList<>();
        ArrayList<String> ascOrDescList = new ArrayList<>();

        if (attributes != null) {
            if (attributes.get("SEPARATOR") != null) {
                separator = attributes.get("SEPARATOR").toString();
                separator = separator.substring(1, separator.length() - 1);
            }
            if (attributes.get("ORDER BY") != null) {
                SQLOrderBy orderByNode = (SQLOrderBy) attributes.get("ORDER BY");
                List<SQLSelectOrderByItem> sqlSelectOrderByItemList = orderByNode.getItems();
                for (SQLSelectOrderByItem sqlSelectOrderByItem : sqlSelectOrderByItemList) {
                    if (!(sqlSelectOrderByItem.getExpr() instanceof SQLIntegerExpr)
                        && !(sqlSelectOrderByItem.getExpr() instanceof SQLIdentifierExpr)
                        && !(sqlSelectOrderByItem.getExpr() instanceof SQLPropertyExpr)) {
                        throw new UnsupportedOperationException(
                            "Do not support group_concat ordery expression other than integer and column name");
                    }
                    if (sqlSelectOrderByItem.getType() != null
                        && "DESC".equals(sqlSelectOrderByItem.getType().toString())) {
                        ascOrDescList.add("DESC");
                    } else {
                        ascOrDescList.add("ASC");
                    }
                    orderOperands.add(convertToSqlNode(sqlSelectOrderByItem.getExpr()));
                }
            }
        }

        List<SQLExpr> arguments = x.getArguments();
        List<SqlNode> argNodes = new ArrayList<SqlNode>(arguments.size());
        for (int i = 0, size = arguments.size(); i < size; ++i) {
            argNodes.add(convertToSqlNode(arguments.get(i)));
        }
        this.sqlNode = new GroupConcatCall(TddlOperatorTable.GROUP_CONCAT,
            SqlParserUtil.toNodeArray(argNodes),
            SqlParserPos.ZERO,
            false,
            functionQualifier,
            separator,
            orderOperands,
            ascOrDescList);
        return false;
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        SQLExpr expr = x.getExpr();
        String alias = x.getAlias();
        List<SQLName> partNames = x.getPartitions();
        SQLExpr flashbackTimestamp = x.getFlashback();

//        if (x.getPartitionSize() > 0) {
//            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
//                "Do not support table with mysql partition.");
//        }

        if (expr instanceof SQLBinaryOpExpr) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Unsupported from syntax");
        }
        SqlNode tableNameIdentifier = convertToSqlNode(expr);

        if (flashbackTimestamp != null) {
            SqlNode snapshotTimestampIdentifier = convertToSqlNode(flashbackTimestamp);

            if (snapshotTimestampIdentifier instanceof SqlDynamicParam) {
                snapshotTimestampIdentifier =
                    new SqlDynamicParam(((SqlDynamicParam) snapshotTimestampIdentifier).getIndex(),
                        SqlTypeName.TIMESTAMP, SqlParserPos.ZERO);
            }

            SqlNode sqlNodeHint = FastSqlConstructUtils.constructForceIndex(x, context, ec);
            ((SqlIdentifier) tableNameIdentifier).indexNode = sqlNodeHint;

            tableNameIdentifier = new SqlBasicCall(SqlStdOperatorTable.AS_OF, new SqlNode[] {
                tableNameIdentifier,
                snapshotTimestampIdentifier
            }, SqlParserPos.ZERO);

        }

        SqlNodeList partitions = null;
        if (x.getPartitionSize() > 0) {
            List<SqlNode> partNameList = new ArrayList<>();
            for (int i = 0; i < partNames.size(); i++) {
                SQLName partName = partNames.get(i);
                SqlNode partNameId = convertToSqlNode(partName);
                partNameList.add(partNameId);
            }
            partitions = new SqlNodeList(partNameList, SqlParserPos.ZERO);
        }

        if (alias != null) {
            if (context.isTestMode() && x.getTableName() != null && (tableNameIdentifier instanceof SqlIdentifier)) {
                String tableName = EagleeyeHelper.rebuildTableName(x.getTableName(), true);
                if (tb2TestNames != null) {
                    tb2TestNames.put(x.getTableName(), tableName);
                }
                tableNameIdentifier = ((SqlIdentifier) tableNameIdentifier)
                    .setName(((SqlIdentifier) tableNameIdentifier).names.size() - 1, tableName);
            }
            SqlNode sqlNodeHint = FastSqlConstructUtils.constructForceIndex(x, context, ec);
            // Note: Use of computeAlias will normalize(will trim) internal, so use normalizeNoTrim here.
            SqlNode aliasIdentifier = new SqlIdentifier(SQLUtils.normalizeNoTrim(alias), SqlParserPos.ZERO);

            // avoid handling table name like '?' in mock mode
            if (!(tableNameIdentifier instanceof SqlIdentifier) && ConfigDataMode.isFastMock()) {
                this.sqlNode = aliasIdentifier;
                return false;
            }

            SqlBasicCall as = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {
                tableNameIdentifier,
                aliasIdentifier}, SqlParserPos.ZERO);
            if (aliasIdentifier instanceof SqlIdentifier) {
                ((SqlIdentifier) aliasIdentifier).indexNode = sqlNodeHint;
                ((SqlIdentifier) aliasIdentifier).partitions = partitions;
            }
            this.sqlNode = as;
        } else {

            if (context.isTestMode() && x.getTableName() != null && (tableNameIdentifier instanceof SqlIdentifier)) {
                String tableName = EagleeyeHelper.rebuildTableName(x.getTableName(), true);
                if (tb2TestNames != null) {
                    tb2TestNames.put(x.getTableName(), tableName);
                }

                SqlNode aliasIdentifier = new SqlIdentifier(x.getTableName(), SqlParserPos.ZERO);
                tableNameIdentifier = ((SqlIdentifier) tableNameIdentifier)
                    .setName(((SqlIdentifier) tableNameIdentifier).names.size() - 1, tableName);

                SqlBasicCall as = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {
                    tableNameIdentifier,
                    aliasIdentifier}, SqlParserPos.ZERO);
                SqlNode sqlNodeHint = FastSqlConstructUtils.constructForceIndex(x, context, ec);

                if (aliasIdentifier instanceof SqlIdentifier) {
                    ((SqlIdentifier) aliasIdentifier).indexNode = sqlNodeHint;
                    ((SqlIdentifier) aliasIdentifier).partitions = partitions;
                }
                this.sqlNode = as;
            } else {
                SqlNode sqlNodeHint = FastSqlConstructUtils.constructForceIndex(x, context, ec);
                this.sqlNode = tableNameIdentifier;
                if (tableNameIdentifier instanceof SqlIdentifier) {
                    ((SqlIdentifier) tableNameIdentifier).indexNode = sqlNodeHint;
                    ((SqlIdentifier) tableNameIdentifier).partitions = partitions;
                }
            }
        }
        return false;
    }

    @Override
    public boolean visit(SQLSubqueryTableSource x) {
        sqlNode = convertToSqlNode(x.getSelect());

        if (x.getAlias() != null) {
            SqlIdentifier aliasIdentifier =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getAlias()), SqlParserPos.ZERO);
            sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS,
                new SqlNode[] {sqlNode, aliasIdentifier},
                SqlParserPos.ZERO);
        }

        return false;
    }

    @Override
    public boolean visit(SQLUnionQueryTableSource x) {// union temp table
        sqlNode = convertToSqlNode(x.getUnion());
        if (x.getAlias() != null) {
            SqlIdentifier aliasIdentifier =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getAlias()), SqlParserPos.ZERO);
            this.sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS,
                new SqlNode[] {this.sqlNode, aliasIdentifier},
                SqlParserPos.ZERO);
        }

        return false;
    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        SQLTableSource left = x.getLeft();
        SQLTableSource right = x.getRight();
        String alias = x.getAlias();
        List<SQLExpr> using = x.getUsing();
        SQLExpr condition = x.getCondition();
        Map<String, Object> attributes = x.getAttributes();
        SQLJoinTableSource.JoinType joinType = x.getJoinType();
        JoinType newjoinType = JoinType.COMMA;
        JoinConditionType conditionType = JoinConditionType.NONE;
        SqlNode conditionSqlNode = null;
        if (condition != null) {
            conditionType = JoinConditionType.ON;
            conditionSqlNode = convertToSqlNode(condition);
        } else if (using != null && using.size() != 0) {
            conditionType = JoinConditionType.USING;
            SqlNodeList usingSqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
            for (SQLExpr sqlExpr : using) {
                usingSqlNodeList.add(convertToSqlNode(sqlExpr));
            }
            conditionSqlNode = usingSqlNodeList;
        }
        switch (joinType) {
        case COMMA:
            break;
        case JOIN:
        case CROSS_JOIN:
        case INNER_JOIN:
            newjoinType = JoinType.INNER;
            break;
        case LEFT_OUTER_JOIN:
            newjoinType = JoinType.LEFT;
            break;
        case RIGHT_OUTER_JOIN:
            newjoinType = JoinType.RIGHT;
            break;
        default:
            throw new UnsupportedOperationException("Do not support join type:" + joinType.name());

        }

        this.sqlNode = new SqlJoin(SqlParserPos.ZERO,

            convertToSqlNode(left),
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            newjoinType.symbol(SqlParserPos.ZERO),
            convertToSqlNode(right),
            conditionType.symbol(SqlParserPos.ZERO),
            conditionSqlNode);
        if (left.getClass() == SQLExprTableSource.class) {
            addPrivilegeVerifyItem(((SQLExprTableSource) left).getSchema(), ((SQLExprTableSource) left).getName()
                .getSimpleName(), PrivilegePoint.SELECT);
        }

        if (right.getClass() == SQLExprTableSource.class) {
            addPrivilegeVerifyItem(((SQLExprTableSource) right).getSchema(), ((SQLExprTableSource) right).getName()
                .getSimpleName(), PrivilegePoint.SELECT);
        }

        return false;
    }

    public boolean visit(SQLAllColumnExpr x) {
        List<String> names = new ArrayList<String>(2);
        names.add("");
        this.sqlNode = new SqlIdentifier(names, SqlParserPos.ZERO);
        return true;
    }

    public boolean visit(SQLExtractExpr x) { // extract()
        SQLIntervalUnit unit = x.getUnit();
        TimeUnit[] timeUnits = getTimeUnit(unit);
        SQLExpr value = x.getValue();
        List<SqlNode> convertedArgs = new ArrayList<>(2);
        SqlNode unitNode = new SqlIntervalQualifier(timeUnits[0], timeUnits[1], SqlParserPos.ZERO);
        SqlNode valueNode = convertToSqlNode(value);
        convertedArgs.add(unitNode);
        convertedArgs.add(valueNode);
        this.sqlNode = SqlStdOperatorTable.EXTRACT.createCall(SqlParserPos.ZERO, convertedArgs);
        return false;
    }

    public TimeUnit[] getTimeUnit(SQLIntervalUnit unit) {
        TimeUnit[] timeUnits = new TimeUnit[2];
        switch (unit) {
        case MICROSECOND:
            timeUnits[0] = TimeUnit.MICROSECOND;
            timeUnits[1] = TimeUnit.MICROSECOND;
            break;
        case SECOND:
            timeUnits[0] = TimeUnit.SECOND;
            timeUnits[1] = TimeUnit.SECOND;
            break;
        case MINUTE:
            timeUnits[0] = TimeUnit.MINUTE;
            timeUnits[1] = TimeUnit.MINUTE;
            break;
        case HOUR:
            timeUnits[0] = TimeUnit.HOUR;
            timeUnits[1] = TimeUnit.HOUR;
            break;
        case DAY:
            timeUnits[0] = TimeUnit.DAY;
            timeUnits[1] = TimeUnit.DAY;
            break;
        case WEEK:
            timeUnits[0] = TimeUnit.WEEK;
            timeUnits[1] = TimeUnit.WEEK;
            break;
        case MONTH:
            timeUnits[0] = TimeUnit.MONTH;
            timeUnits[1] = TimeUnit.MONTH;
            break;
        case QUARTER:
            timeUnits[0] = TimeUnit.QUARTER;
            timeUnits[1] = TimeUnit.QUARTER;
            break;
        case YEAR:
            timeUnits[0] = TimeUnit.YEAR;
            timeUnits[1] = TimeUnit.YEAR;
            break;
        case SECOND_MICROSECOND:
            timeUnits[0] = TimeUnit.SECOND;
            timeUnits[1] = TimeUnit.MICROSECOND;
            break;
        case MINUTE_MICROSECOND:
            timeUnits[0] = TimeUnit.MINUTE;
            timeUnits[1] = TimeUnit.MICROSECOND;
            break;
        case MINUTE_SECOND:
            timeUnits[0] = TimeUnit.MINUTE;
            timeUnits[1] = TimeUnit.SECOND;
            break;
        case HOUR_MICROSECOND:
            timeUnits[0] = TimeUnit.HOUR;
            timeUnits[1] = TimeUnit.MICROSECOND;
            break;
        case HOUR_SECOND:
            timeUnits[0] = TimeUnit.HOUR;
            timeUnits[1] = TimeUnit.SECOND;
            break;
        case HOUR_MINUTE:
            timeUnits[0] = TimeUnit.HOUR;
            timeUnits[1] = TimeUnit.MINUTE;
            break;
        case DAY_MICROSECOND:
            timeUnits[0] = TimeUnit.DAY;
            timeUnits[1] = TimeUnit.MICROSECOND;
            break;
        case DAY_SECOND:
            timeUnits[0] = TimeUnit.DAY;
            timeUnits[1] = TimeUnit.SECOND;
            break;
        case DAY_MINUTE:
            timeUnits[0] = TimeUnit.DAY;
            timeUnits[1] = TimeUnit.MINUTE;
            break;
        case DAY_HOUR:
            timeUnits[0] = TimeUnit.DAY;
            timeUnits[1] = TimeUnit.HOUR;
            break;
        case YEAR_MONTH:
            timeUnits[0] = TimeUnit.YEAR;
            timeUnits[1] = TimeUnit.MONTH;
            break;
        default:
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Unsupported time unit");
        }
        return timeUnits;
    }

    public String getSQLIntervalUnit(SQLIntervalUnit unit) {
        return unit.name();
    }

    public boolean visit(SQLCharExpr x) {// trim
        sqlNode = SqlLiteral.createCharString(x.getText(), SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLBinaryExpr x) {
        BitString fromBitString = BitString.createFromBitString(x.getText());
        String s = fromBitString.toHexString();
        if (s.length() == 1 && (x.getText().toLowerCase().startsWith("0b"))) {
            s = "0" + s;
        }
        sqlNode = SqlLiteral.createBinaryString(s, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLHexExpr x) {
        String hex = x.getHex();
        if (hex.length() == 1) {
            hex = "0" + hex;
        }
        sqlNode = SqlLiteral.createBinaryString(hex, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(MySqlCharExpr x) {
        SqlCharStringLiteral charString;
        String text = x.getText();
        String mysqlCharset = x.getCharset();
        String mysqlCollate = x.getCollate();
        boolean isHex = x.isHex();

        if (mysqlCharset != null) {
            Charset sqlCharset = CharsetName.convertStrToJavaCharset(mysqlCharset);
            if (sqlCharset == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                    "unsupported mysql character set: " + mysqlCharset);
            } else {
                SqlCollation sqlCollation =
                    new SqlCollation(sqlCharset, mysqlCollate, SqlCollation.Coercibility.EXPLICIT);
                if (isHex) {
                    CharsetName charsetName = CharsetName.of(sqlCharset);
                    text = charsetName.toUTF16String(text);
                }
                NlsString nlsString = new NlsString(text, mysqlCharset, sqlCollation);
                charString = SqlLiteral.createCharString(nlsString, SqlParserPos.ZERO);
            }
        } else {
            charString = SqlLiteral.createCharString(text, SqlParserPos.ZERO);
        }
        sqlNode = charString;
        return false;
    }

    public boolean visit(SQLNullExpr x) {
        sqlNode = SqlLiteral.createNull(SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLIntegerExpr x) {
        if (x.getNumber() instanceof Integer || x.getNumber() instanceof Long) {
            sqlNode = SqlLiteral.createLiteralForIntTypes(
                x.getNumber().longValue(),
                SqlParserPos.ZERO,
                SqlTypeName.BIGINT);
        } else if (x.getNumber() instanceof BigInteger) {
            BigInteger number = (BigInteger) x.getNumber();

            // The boundary value of bigint is min value of longlong and max value of ulonglong.
            // otherwise, the big integer number will be recognized as decimal value.
            if (number.compareTo(MAX_UNSIGNED_INT64) > 0 || number.compareTo(MIN_SIGNED_INT64) < 0) {
                BigDecimal decimalNumber = new BigDecimal(number);
                sqlNode = SqlLiteral.createExactNumeric(decimalNumber, SqlParserPos.ZERO);
            } else if (number.compareTo(MAX_SIGNED_INT64) <= 0) {
                // for -9223372036854775808 ~ 9223372036854775807, use normal long value.
                long longVal = x.getNumber().longValue();
                sqlNode = SqlLiteral.createLiteralForIntTypes(
                    longVal,
                    SqlParserPos.ZERO,
                    SqlTypeName.BIGINT);
            } else {
                sqlNode = SqlLiteral.createLiteralForIntTypes(String.valueOf(x.getNumber()),
                    SqlParserPos.ZERO,
                    SqlTypeName.BIGINT_UNSIGNED);
            }
        }
        return false;
    }

    public boolean visit(SQLBooleanExpr x) {
        sqlNode = SqlLiteral.createBoolean(x.getBooleanValue(), SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLIntervalExpr x) {
        TimeUnit timeUnits[] = getTimeUnit(x.getUnit());
        SqlIntervalQualifier unitNode = new SqlIntervalQualifier(timeUnits[0], timeUnits[1], SqlParserPos.ZERO);
        sqlNode = new SqlBasicCall(TddlOperatorTable.INTERVAL_PRIMARY, new SqlNode[] {
            convertToSqlNode(x.getValue()),
            unitNode}, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLDateExpr x) {
        sqlNode = new SqlBasicCall(TddlOperatorTable.DATE,
            new SqlNode[] {SqlLiteral.createCharString(x.getLiteral(), SqlParserPos.ZERO)}, SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLUnaryExpr x) {// ~
        SQLUnaryOperator operator = x.getOperator();
        switch (operator) {
        case NOT:
        case Not:
            this.sqlNode = SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, convertToSqlNode(x.getExpr()));
            break;
        case Plus:
            this.sqlNode = TddlOperatorTable.UNARY_PLUS.createCall(SqlParserPos.ZERO, convertToSqlNode(x.getExpr()));
            break;
        case Negative:
            this.sqlNode = TddlOperatorTable.UNARY_MINUS.createCall(SqlParserPos.ZERO,
                convertToSqlNode(x.getExpr()));
            break;
        case Compl:
            this.sqlNode = SqlStdOperatorTable.INVERT.createCall(SqlParserPos.ZERO, convertToSqlNode(x.getExpr()));
            break;
        case BINARY:
            this.sqlNode = TddlOperatorTable.BINARY.createCall(SqlParserPos.ZERO, convertToSqlNode(x.getExpr()));
            break;
        default:
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "Does not support :"
                + operator);
        }
        return false;
    }

    public boolean visit(MySqlForceIndexHint x) {
        visit((MySqlIndexHintImpl) x);
        return false;
    }

    public boolean visit(MySqlUseIndexHint x) {
        visit((MySqlIndexHintImpl) x);
        return false;
    }

    public boolean visit(MySqlIgnoreIndexHint x) {
        visit((MySqlIndexHintImpl) x);
        return false;
    }

    public boolean visit(SQLNumberExpr x) {
        StringBuilder sb = new StringBuilder();
        x.output(sb);
        NumberParser parser = new NumberParser(sb.toString());
        boolean notation = parser.isNotation();
        if (notation) {
            sqlNode = SqlLiteral.createApproxNumeric(x.getNumber().toString(), SqlParserPos.ZERO);
        } else if (x.getNumber() instanceof BigDecimal) {
            sqlNode = SqlLiteral.createExactNumeric((BigDecimal) x.getNumber(), SqlParserPos.ZERO);
        } else {
            sqlNode = SqlLiteral.createExactNumeric(x.getNumber().toString(), SqlParserPos.ZERO);
        }
        return false;
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        String name = x.getName();
        if (StringUtils.startsWith(name, "@")) {
            if (TStringUtil.startsWith(name, "@@")) {
                String varText = TStringUtil.substring(name, 2);

                VariableScope scope = VariableScope.SESSION;
                if (x.isGlobal()) {
                    scope = VariableScope.GLOBAL;
                }
                if (x.isSession()) {
                    scope = VariableScope.SESSION;
                }
                // system var
                this.sqlNode = SqlSystemVar.create(scope, varText, SqlParserPos.ZERO);
                return false;
            } else {
                String varText = TStringUtil.substring(name, 1);
                // user var
                this.sqlNode = SqlUserDefVar.create(varText, SqlParserPos.ZERO);
                return false;
            }
        } else if (x.isGlobal()) { // fix fastsql bug
            this.sqlNode = SqlSystemVar.create(VariableScope.GLOBAL, name, SqlParserPos.ZERO);
            return false;
        } else if (x.isSession()) { // fix fastsql bug
            this.sqlNode = SqlSystemVar.create(VariableScope.SESSION, name, SqlParserPos.ZERO);
            return false;
        }

        if ("?".equals(name)) {
            int index = x.getIndex();
            ColumnMeta columnMeta = null;
            List<Object> params = context.getParameter(ContextParameterKey.PARAMS);
            boolean isBinaryType = false;
            boolean isStringType = false;
            CharsetName targetCharSet = null;
            boolean allowInferType =
                bindMapTypes != null && (CollectionUtils.isNotEmpty(params) && byte[].class.isInstance(
                    params.get(index)));
            if (allowInferType) {
                columnMeta = bindMapTypes.get(x);
                if (columnMeta != null) {
                    isBinaryType = DataTypeUtil.isBinaryType(columnMeta.getDataType());
                    isStringType = DataTypeUtil.isStringType(columnMeta.getDataType());
                }
            }
            if (CollectionUtils.isNotEmpty(params)) {
                Map<Integer, NlsString> parameterNlsStrings = context.getParameterNlsStrings();
                if (parameterNlsStrings != null && parameterNlsStrings.containsKey(index)) {
                    // handle charset & collation literal: _{charset}value collate {collation}
                    NlsString nlsString = parameterNlsStrings.get(index);
                    if (CharsetName.of(nlsString.getCharset()) == CharsetName.BINARY) {
                        this.sqlNode = new SqlDynamicParam(index,
                            SqlTypeName.BINARY,
                            SqlParserPos.ZERO,
                            null,
                            null,
                            null);
                    } else {
                        this.sqlNode = new SqlDynamicParam(index,
                            SqlTypeName.CHAR,
                            SqlParserPos.ZERO,
                            null,
                            nlsString.getCharset(),
                            nlsString.getCollation());
                    }
                } else if (params.size() > index && params.get(index) instanceof NlsString) {
                    NlsString nlsString = (NlsString) params.get(index);
                    this.sqlNode = new SqlDynamicParam(index,
                        SqlTypeName.CHAR,
                        SqlParserPos.ZERO,
                        null,
                        nlsString.getCharset(),
                        nlsString.getCollation());
                } else {
                    if (allowInferType) {
                        columnMeta = bindMapTypes.get(x);
                        if (columnMeta != null) {
                            if (isBinaryType) {
                                targetCharSet = CharsetName.BINARY;
                            } else if (isStringType) {
                                targetCharSet = columnMeta.getDataType().getCharsetName();
                            }
                        }
                    }
                    this.sqlNode = new SqlDynamicParam(index,
                        FastSqlConstructUtils.getTypeNameOfParam(params, index),
                        SqlParserPos.ZERO,
                        null,
                        targetCharSet != null ? targetCharSet.toJavaCharset() : null,
                        null);

                    if (targetCharSet != null) {
                        if (isBinaryType) {
                            this.sqlNode = SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, sqlNode,
                                new SqlDataTypeSpec(
                                    new SqlIdentifier(SqlTypeName.BINARY.getName(), SqlParserPos.ZERO),
                                    -1, -1, null, null, SqlParserPos.ZERO));
                        } else if (isStringType) {
                            NlsString nlsString = new NlsString(targetCharSet.name(),
                                null, null);
                            this.sqlNode = SqlStdOperatorTable.CONVERT.createCall(SqlParserPos.ZERO, sqlNode,
                                SqlLiteral.createCharString(nlsString, SqlParserPos.ZERO));
                        }
                    }
                }
            } else {
                this.sqlNode = new SqlDynamicParam(
                    index, SqlTypeName.ANY, SqlParserPos.ZERO, null,
                    null,
                    null);
                Object parameter = context.getParameter(ContextParameterKey.ORIGIN_PARAMETER_COUNT);
                context.putParameter(ContextParameterKey.ORIGIN_PARAMETER_COUNT,
                    parameter == null ? 1 : ((Integer) parameter).intValue() + 1);
            }
        } else {
            if (context.isUnderSet()) {
                VariableScope scope = null;
                if (x.isGlobal()) {
                    scope = VariableScope.GLOBAL;
                }

                if (x.isSession()) {
                    scope = VariableScope.SESSION;
                }
                // system var
                this.sqlNode = SqlSystemVar.create(scope, x.getName(), SqlParserPos.ZERO);
            } else {
                this.sqlNode = SqlLiteral.createCharString(name, SqlParserPos.ZERO);
            }
        }
        return false;
    }

    private FieldMetaData getParamMetaData() {
        FieldMetaData fieldMetaData = new FieldMetaData();
        fieldMetaData.setFieldName("?");
        fieldMetaData.setOriginFieldName("?");
        return fieldMetaData;
    }

    private TableMetaData getTableMetaDataFromContext() {
        TableMetaData tableMetaData = context.getParameter(ContextParameterKey.META_DATA);
        if (tableMetaData == null) {
            tableMetaData = new TableMetaData();
            context.putParameter(ContextParameterKey.META_DATA, tableMetaData);
        }
        return tableMetaData;
    }

    @Override
    public boolean visit(SQLInSubQueryExpr x) {
        SqlNode left = convertToSqlNode(x.getExpr());
        SqlBinaryOperator subOperator = SqlStdOperatorTable.IN;
        if (x.isNot()) {
            subOperator = SqlStdOperatorTable.NOT_IN;
        }
        SqlNode right = convertToSqlNode(x.subQuery);

        sqlNode = new SqlBasicCall(subOperator, new SqlNode[] {left, right}, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLBetweenExpr x) {
        SQLExpr testExpr = x.getTestExpr();
        SqlOperator sqlOperator = SqlStdOperatorTable.BETWEEN;
        if (x.isNot()) {
            sqlOperator = SqlStdOperatorTable.NOT_BETWEEN;
        }
        SqlNode sqlNode = convertToSqlNode(testExpr);
        SqlNode sqlNodeBegin = convertToSqlNode(x.getBeginExpr());
        SqlNode sqlNodeEnd = convertToSqlNode(x.getEndExpr());
        ArrayList<SqlNode> sqlNodes = new ArrayList<>(3);
        sqlNodes.add(sqlNode);
        sqlNodes.add(sqlNodeBegin);
        sqlNodes.add(sqlNodeEnd);
        this.sqlNode = new SqlBasicCall(sqlOperator, SqlParserUtil.toNodeArray(sqlNodes), SqlParserPos.ZERO);
        return false;
    }

    public void visitExpr(SQLExpr x) {
        Class<?> clazz = x.getClass();
        if (clazz == SQLIdentifierExpr.class) {
            visit((SQLIdentifierExpr) x);
        } else if (clazz == SQLPropertyExpr.class) {
            visit((SQLPropertyExpr) x);
        } else if (clazz == SQLAllColumnExpr.class) {
            visit((SQLAllColumnExpr) x);
        } else if (clazz == SQLAggregateExpr.class) {
            visit((SQLAggregateExpr) x);
        } else if (clazz == SQLBinaryOpExpr.class) {
            visit((SQLBinaryOpExpr) x);
        } else if (clazz == SQLCharExpr.class) {
            visit((SQLCharExpr) x);
        } else if (clazz == SQLNullExpr.class) {
            visit((SQLNullExpr) x);
        } else if (clazz == SQLIntegerExpr.class) {
            visit((SQLIntegerExpr) x);
        } else if (clazz == SQLNumberExpr.class) {
            visit((SQLNumberExpr) x);
        } else if (clazz == SQLMethodInvokeExpr.class) {
            visit((SQLMethodInvokeExpr) x);
        } else if (clazz == SQLVariantRefExpr.class) {
            visit((SQLVariantRefExpr) x);
        } else if (clazz == SQLBinaryOpExprGroup.class) {
            visit((SQLBinaryOpExprGroup) x);
        } else if (clazz == SQLCaseExpr.class) {
            visit((SQLCaseExpr) x);
        } else if (clazz == SQLInListExpr.class) {
            visit((SQLInListExpr) x);
        } else if (clazz == SQLNotExpr.class) {
            visit((SQLNotExpr) x);
        } else {
            x.accept(this);
        }
    }

    @Override
    public boolean visit(SQLMatchAgainstExpr x) {
        List<SqlNode> cols = x.getColumns().stream()
            .map(this::convertToSqlNode)
            .collect(Collectors.toList());
        SqlNode against = convertToSqlNode(x.getAgainst());
        if (x.getSearchModifier() == SQLMatchAgainstExpr.SearchModifier.IN_NATURAL_LANGUAGE_MODE_WITH_QUERY_EXPANSION
            || x.getSearchModifier() == SQLMatchAgainstExpr.SearchModifier.WITH_QUERY_EXPANSION) {
            GeneralUtil.nestedException("don't support mode: " + x.getSearchModifier().name);
        }
        SqlNode searchModifier = Optional.ofNullable(x.getSearchModifier())
            .map(m -> m.name)
            .map(s -> SqlLiteral.createCharString(s, SqlParserPos.ZERO))
            .orElseGet(
                () -> SqlLiteral.createCharString("", SqlParserPos.ZERO)
            );
        SqlOperator op = TddlOperatorTable.MATCH_AGAINST;
        final int sqlNodeSize = cols.size() + 2;
        SqlNode[] sqlNodes = new SqlNode[sqlNodeSize];
        for (int i = 0; i < cols.size(); i++) {
            sqlNodes[i] = cols.get(i);
        }
        sqlNodes[sqlNodeSize - 2] = against;
        sqlNodes[sqlNodeSize - 1] = searchModifier;
        this.sqlNode = new SqlBasicCall(op, sqlNodes, SqlParserPos.ZERO);

        return false;
    }

    public boolean visit(SQLSelectGroupByClause x) {
        this.context.putParameter(ContextParameterKey.DO_HAVING, null);
        int itemSize = x.getItems().size();
        if (itemSize > 0) {
            List<SqlNode> groupByNodes = new ArrayList<>(itemSize);
            for (int i = 0; i < itemSize; ++i) {
                List<SQLExpr> items = x.getItems();
                SQLExpr sqlExpr = items.get(i);
                sqlExpr.accept(this);
                SqlNode sqlNode = convertToSqlNode(sqlExpr);
                groupByNodes.add(sqlNode);
            }
        }

        SQLExpr having = x.getHaving();
        if (having != null) {
            SqlNode sqlNode = convertToSqlNode(having);
        }
        this.sqlNode = sqlNode;
        this.context.removeParameter(ContextParameterKey.DO_HAVING);
        return false;
    }

    public boolean visit(SQLBinaryOpExpr x) {// SQLBinaryOpExpr a >= 1 或 a>1 and
        // b <2,其中多个operator：>=|>|and|<
        if (x.getOperator().equals(SubGtGt)) {
            // transform ->> operator to JSON_UNQUOTE(JSON_EXTRACT())

            this.sqlNode = new SqlBasicCall(TddlOperatorTable.JSON_UNQUOTE,
                new SqlNode[] {
                    new SqlBasicCall(TddlOperatorTable.JSON_EXTRACT, new SqlNode[] {
                        convertToSqlNode(x.getLeft()), convertToSqlNode(x.getRight())}, SqlParserPos.ZERO)},
                SqlParserPos.ZERO);
            return false;
        } else if (x.getOperator().equals(SubGt)) {
            // transform -> operator to JSON_EXTRACT()
            this.sqlNode = new SqlBasicCall(TddlOperatorTable.JSON_EXTRACT, new SqlNode[] {
                convertToSqlNode(x.getLeft()), convertToSqlNode(x.getRight())}, SqlParserPos.ZERO);
            return false;
        }

        SqlOperator operator = null;
        boolean rightOperator = false;
        SQLExpr right = x.getRight();
        switch (x.getOperator()) {
        case Equality:
            if (isSqlAllExpr(right)) {
                operator = SqlStdOperatorTable.ALL_EQ;
            } else if (isAnyOrSomeExpr(right)) {
                operator = SqlStdOperatorTable.SOME_EQ;
            } else {
                operator = SqlStdOperatorTable.EQUALS;
            }
            break;
        case GreaterThan:
            if (isSqlAllExpr(right)) {
                operator = SqlStdOperatorTable.ALL_GT;
            } else if (isAnyOrSomeExpr(right)) {
                operator = SqlStdOperatorTable.SOME_GT;
            } else {
                operator = SqlStdOperatorTable.GREATER_THAN;
            }
            break;
        case GreaterThanOrEqual:
            if (isSqlAllExpr(right)) {
                operator = SqlStdOperatorTable.ALL_GE;
            } else if (isAnyOrSomeExpr(right)) {
                operator = SqlStdOperatorTable.SOME_GE;
            } else {
                operator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            }
            break;
        case LessThan:
            if (isSqlAllExpr(right)) {
                operator = SqlStdOperatorTable.ALL_LT;
            } else if (isAnyOrSomeExpr(right)) {
                operator = SqlStdOperatorTable.SOME_LT;
            } else {
                operator = SqlStdOperatorTable.LESS_THAN;
            }
            break;
        case LessThanOrEqual:
            if (isSqlAllExpr(right)) {
                operator = SqlStdOperatorTable.ALL_LE;
            } else if (isAnyOrSomeExpr(right)) {
                operator = SqlStdOperatorTable.SOME_LE;
            } else {
                operator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            }
            break;
        case NotEqual:
        case LessThanOrGreater:
            if (isSqlAllExpr(right)) {
                operator = SqlStdOperatorTable.ALL_NE;
            } else if (isAnyOrSomeExpr(right)) {
                operator = SqlStdOperatorTable.SOME_NE;
            } else {
                operator = SqlStdOperatorTable.NOT_EQUALS;
            }
            break;
        case Add:
            operator = SqlStdOperatorTable.PLUS;
            break;
        case Subtract:
            operator = SqlStdOperatorTable.MINUS;
            break;
        case Multiply:
            operator = SqlStdOperatorTable.MULTIPLY;
            break;
        case Divide:
            operator = SqlStdOperatorTable.DIVIDE;
            break;
        case Modulus:
            operator = SqlStdOperatorTable.MOD;
            break;
        case Like:
            operator = SqlStdOperatorTable.LIKE;
            break;
        case NotLike:
            operator = SqlStdOperatorTable.NOT_LIKE;
            break;
        case Escape:
            // Special case: left is like binary op and right is char expr.
            // Dealing it in left node.
            break;
        case BooleanAnd:
            operator = SqlStdOperatorTable.AND;
            break;
        case BooleanOr:
            operator = SqlStdOperatorTable.OR;
            break;
        case Is:
            rightOperator = true;
            if (right instanceof SQLNullExpr) {
                operator = SqlStdOperatorTable.IS_NULL;
            } else if (right instanceof SQLBooleanExpr) {
                if (((SQLBooleanExpr) right).getValue()) {
                    operator = SqlStdOperatorTable.IS_TRUE;
                } else {
                    operator = SqlStdOperatorTable.IS_FALSE;
                }
            } else if (right instanceof SQLIdentifierExpr) {
                String name = ((SQLIdentifierExpr) right).getName();
                if (name.equalsIgnoreCase(UNKNOWN)) {
                    operator = SqlStdOperatorTable.IS_UNKNOWN;
                } else {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Just support IS TRUE|FALSE|UNKNOWN|NULL");
                }
            }
            break;
        case IsNot:
            rightOperator = true;
            if (right instanceof SQLNullExpr) {
                operator = SqlStdOperatorTable.IS_NOT_NULL;
            } else if (right instanceof SQLBooleanExpr) {
                if (((SQLBooleanExpr) right).getValue()) {
                    operator = SqlStdOperatorTable.IS_NOT_TRUE;
                } else {
                    operator = SqlStdOperatorTable.IS_NOT_FALSE;
                }
            } else if (right instanceof SQLIdentifierExpr) {
                String name = ((SQLIdentifierExpr) right).getName();
                if (name.equalsIgnoreCase(UNKNOWN)) {
                    operator = SqlStdOperatorTable.IS_NOT_UNKNOWN;
                } else {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                        "Just support IS NOT TRUE|FALSE|UNKNOWN|NULL");
                }
            }
            break;
        case LessThanOrEqualOrGreaterThan:
            operator = TddlOperatorTable.NULL_SAFE_EQUAL;
            break;
        case BitwiseAnd:
            operator = TddlOperatorTable.BITWISE_AND;
            break;
        case BitwiseOr:
            operator = TddlOperatorTable.BITWISE_OR;
            break;
        case LeftShift:
            operator = TddlOperatorTable.BITLSHIFT;
            break;
        case RightShift:
            operator = TddlOperatorTable.BITRSHIFT;
            break;
        case BooleanXor:
            operator = TddlOperatorTable.XOR;
            break;
        case NotRegExp:
            operator = TddlOperatorTable.NOT_REGEXP;
            break;
        case RegExp:
            operator = TddlOperatorTable.REGEXP;
            break;
        case DIV:
            operator = TddlOperatorTable.DIVIDE_INTEGER;
            break;
        case BitwiseXor:
            operator = TddlOperatorTable.BITWISE_XOR;
            break;
        case SubGt:
            operator = TddlOperatorTable.JSON_EXTRACT;
            break;
        case COLLATE:
            operator = null;
            break;
        case Assignment:
            operator = TddlOperatorTable.ASSIGNMENT;
            break;
        default:
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "Does not support "
                + x.getOperator());

        }
        SqlNode sqlNode = convertToSqlNode(x.getLeft());
        if (rightOperator) {
            this.sqlNode = new SqlBasicCall(operator, new SqlNode[] {sqlNode}, SqlParserPos.ZERO);
        } else if (operator != null) {
            if (x.getRight() instanceof SQLIntervalExpr) {
                // for datetime +/- INTERVAL ? time_unit
                // convert to date_add(datetime, INTERVAL ? time_unit)
                // or date_sub(datetime, INTERVAL ? time_unit)
                if (operator == TddlOperatorTable.PLUS) {
                    operator = TddlOperatorTable.DATE_ADD;
                } else if (operator == TddlOperatorTable.MINUS) {
                    operator = TddlOperatorTable.DATE_SUB;
                }
                this.sqlNode = new SqlBasicCall(operator,
                    new SqlNode[] {sqlNode, convertToSqlNode(x.getRight())},
                    SqlParserPos.ZERO);
            } else if (x.getLeft() instanceof SQLIntervalExpr && operator == TddlOperatorTable.PLUS) {
                // for INTERVAL ? time_unit + datetime
                // convert to date_add(datetime, INTERVAL ? time_unit)
                operator = TddlOperatorTable.DATE_ADD;
                this.sqlNode = new SqlBasicCall(operator,
                    new SqlNode[] {convertToSqlNode(x.getRight()), sqlNode},
                    SqlParserPos.ZERO);
            } else {
                this.sqlNode = new SqlBasicCall(operator,
                    new SqlNode[] {sqlNode, convertToSqlNode(x.getRight())},
                    SqlParserPos.ZERO);
            }

        } else {
            if (x.getOperator() == Escape) {
                if (sqlNode instanceof SqlBasicCall &&
                    sqlNode.getKind() == SqlKind.LIKE &&
                    ((SqlBasicCall) sqlNode).getOperands().length == 2) {
                    SqlBasicCall call = (SqlBasicCall) sqlNode;
                    this.sqlNode = new SqlBasicCall(call.getOperator(),
                        new SqlNode[] {call.getOperands()[0], call.getOperands()[1], convertToSqlNode(x.getRight())},
                        SqlParserPos.ZERO);
                } else {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "Unexpected "
                        + x.getOperator());
                }
            } else {
                this.sqlNode = sqlNode;
            }
        }
        return false;
    }

    @Override
    public boolean visit(SQLNotExpr x) {// not true
        SqlCall call = SqlStdOperatorTable.NOT.createCall(new SqlNodeList(Arrays.asList(convertToSqlNode(x.getExpr())),
            SqlParserPos.ZERO));
        this.sqlNode = call;
        return false;
    }

    @Override
    public boolean visit(SQLDefaultExpr x) {
        this.sqlNode = SqlStdOperatorTable.DEFAULT.createCall(SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLUnionQuery x) {
        SQLSelectQuery left = x.getLeft();
        SQLSelectQuery right = x.getRight();
        SqlNode leftNode = convertToSqlNode(left);
        if (right instanceof SQLUnionQuery) {
            if (((SQLUnionQuery) right).getLeft() == null) {
                right = ((SQLUnionQuery) right).getRight();
            }
        }
        SqlNode rightNode = convertToSqlNode(right);
        SqlOperator operator = null;
        List<SqlNode> unionList = Arrays.asList(leftNode, rightNode);
        // orderBy
        SqlNodeList orderBySqlNode = FastSqlConstructUtils.constructOrderBy(x.getOrderBy(), context, ec);
        // limit
        SqlNodeList limitNodes = FastSqlConstructUtils.constructLimit(x.getLimit(), context, ec);
        SqlNode offset = null;
        SqlNode limit = null;
        if (limitNodes != null) {
            offset = limitNodes.get(0);
            limit = limitNodes.get(1);
        }

        switch (x.getOperator()) {
        case UNION_ALL:
            operator = SqlStdOperatorTable.UNION_ALL;
            break;
        case DISTINCT:
            /**
             * MySQL 约定 UNION 默认去除重复行
             * 参考：https://dev.mysql.com/doc/refman/5.7/en/union.html
             */
        case UNION:
            operator = SqlStdOperatorTable.UNION;
            break;
        case EXCEPT:
        case MINUS:
            operator = SqlStdOperatorTable.EXCEPT;
            break;
        case INTERSECT:
        case INTERSECT_DISTINCT:
            operator = SqlStdOperatorTable.INTERSECT;
            break;
        default:
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                String.format("%s not support!", x.getOperator().name));
        }
        SqlNode[] operands = {leftNode, rightNode};
        SqlNode unionSqlNode = operator.createCall(new SqlParserPos(1, 55, 3, 56).plusAll(operands), operands);
        if (orderBySqlNode == null && limit == null) {
            sqlNode = unionSqlNode;
        } else {
            if (orderBySqlNode == null) {
                orderBySqlNode = SqlNodeList.EMPTY;
            }
            if (x.getParent() instanceof SQLUnionQueryTableSource) {
                // Must rewrite to (select * from xxx union xxx limit xxx) as xx.

                // Gen * select list.
                SqlNodeList selectList = new SqlNodeList(new ArrayList<>(1), SqlParserPos.ZERO);
                List<String> names = new ArrayList<String>(2);
                names.add("");
                selectList.add(new SqlIdentifier(names, SqlParserPos.ZERO));

                sqlNode = new TDDLSqlSelect(SqlParserPos.ZERO,
                    null,
                    selectList,
                    new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {
                        unionSqlNode,
                        new SqlIdentifier(unionSqlNode.toString(), SqlParserPos.ZERO)}, SqlParserPos.ZERO),
                    null,
                    null,
                    null,
                    null,
                    orderBySqlNode,
                    offset,
                    limit,
                    new SqlNodeList(new ArrayList<>(), SqlParserPos.ZERO),
                    LockMode.UNDEF);
            } else {
                sqlNode = new SqlOrderBy(SqlParserPos.ZERO, unionSqlNode, orderBySqlNode, offset, limit);
            }
        }
        return false;
    }

    public boolean visit(SQLSelect x) {
        SQLWithSubqueryClause withSubQuery = x.getWithSubQuery();
        if (withSubQuery != null) {

            if (withSubQuery.getRecursive() != null && withSubQuery.getRecursive() == true) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Not supported recursive CTE (Common Table Expressions)");
            }

            SqlNodeList withList = new SqlNodeList(SqlParserPos.ZERO);
            for (SQLWithSubqueryClause.Entry e : withSubQuery.getEntries()) {

                SqlNodeList columnList = null;
                if (e.getColumns() != null && !e.getColumns().isEmpty()) {
                    columnList = new SqlNodeList(SqlParserPos.ZERO);
                    for (SQLName sqlName : e.getColumns()) {
                        columnList.add(new SqlIdentifier(sqlName.getSimpleName(), SqlParserPos.ZERO));
                    }
                }

                e.getSubQuery().accept(this);

                SqlWithItem sqlWithItem = new SqlWithItem(
                    SqlParserPos.ZERO,
                    new SqlIdentifier(SQLUtils.normalizeNoTrim(e.getAlias()), SqlParserPos.ZERO),
                    columnList,
                    this.getSqlNode());
                withList.add(sqlWithItem);
            }
            visit(x.getQuery());
            SqlWith sqlWith = new SqlWith(SqlParserPos.ZERO, withList, this.getSqlNode());
            this.sqlNode = sqlWith;
            return false;
        }
        visit(x.getQuery());
        return false;
    }

    public boolean visit(SQLSelectQuery x) {

        Class<?> clazz = x.getClass();
        if (clazz == MySqlSelectQueryBlock.class) {
            visit((MySqlSelectQueryBlock) x);
        } else if (clazz == SQLSelectQueryBlock.class) {
            visit((SQLSelectQueryBlock) x);
        } else if (clazz == SQLUnionQuery.class) {
            visit((SQLUnionQuery) x);
        } else {
            x.accept(this);
        }

        return false;
    }

    public boolean visit(SQLOrderBy x) {
        SqlNodeList orderBySqlNode;
        List<SqlNode> orderByNodes = new ArrayList<SqlNode>(x.getItems().size());

        orderBySqlNode = new SqlNodeList(orderByNodes, SqlParserPos.ZERO);
        List<SQLSelectOrderByItem> items = x.getItems();

        if (items.size() > 0) {
            for (int i = 0, size = items.size(); i < size; ++i) {
                SQLSelectOrderByItem item = items.get(i);
                SqlNode sqlNode = convertToSqlNode(item);
                orderBySqlNode.add(sqlNode);
            }
        }
        this.sqlNode = orderBySqlNode;
        return false;
    }

    public boolean visit(SQLSelectOrderByItem x) {
        SqlNode sqlNode = convertToSqlNode(x.getExpr());
        if (SQLOrderingSpecification.DESC == x.getType()) {
            sqlNode = new SqlBasicCall(SqlStdOperatorTable.DESC, new SqlNode[] {sqlNode}, SqlParserPos.ZERO);
        }
        this.sqlNode = sqlNode;
        return false;
    }

    public boolean visit(SQLInListExpr x) {
        if (context != null) {
            context.putParameter(ContextParameterKey.HAS_IN_EXPR, true);
        }
        List<SQLExpr> targetList = x.getTargetList();
        List<SqlNode> inList = new ArrayList<>(targetList.size());
        for (int i = 0; i < targetList.size(); ++i) {
            SQLExpr sqlExpr = targetList.get(i);
            SqlNode sqlNode = convertToSqlNode(sqlExpr);
            inList.add(sqlNode);
        }
        SqlNodeList sqlNodes = new SqlNodeList(inList, SqlParserPos.ZERO);
        SqlOperator sqlOperator = x.isNot() ? SqlStdOperatorTable.NOT_IN : SqlStdOperatorTable.IN;
        sqlNode = new SqlBasicCall(sqlOperator,
            new SqlNode[] {convertToSqlNode(x.getExpr()), sqlNodes},
            SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLCastExpr x) {
        SqlOperator sqlOperator = TddlOperatorTable.CAST;
        String charset = null;
        int p = -1, c = -1;
        SqlNode exprNode = convertToSqlNode(x.getExpr());
        SQLDataType dataType = x.getDataType();
        DateType dateTypeBean = new DateType((SQLDataTypeImpl) dataType).invoke();
        p = dateTypeBean.p;
        c = dateTypeBean.c;
        charset = dateTypeBean.charset;
        this.sqlNode = sqlOperator.createCall(SqlParserPos.ZERO,
            exprNode,
            new SqlDataTypeSpec(new SqlIdentifier(dataType.getName().toUpperCase(), SqlParserPos.ZERO),
                p,
                c,
                charset,
                null,
                SqlParserPos.ZERO));

        return false;
    }

    public boolean visit(SQLDataType x) {// cast() signed
        return false;
    }

    public boolean visit(SQLCaseExpr x) {// CASE WHEN
        SQLExpr valueExpr = x.getValueExpr();
        SqlNode nodeValue = null;
        SqlNodeList nodeWhen = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList nodeThen = new SqlNodeList(SqlParserPos.ZERO);
        if (valueExpr != null) {
            nodeValue = convertToSqlNode(valueExpr);
        }

        List items = x.getItems();
        int elExpr = 0;

        for (int size = items.size(); elExpr < size; ++elExpr) {
            this.visit((SQLCaseExpr.Item) items.get(elExpr));
            if (this.sqlNode != null && this.sqlNode instanceof SqlNodeList) {
                SqlNodeList nodeListTemp = (SqlNodeList) this.sqlNode;
                nodeWhen.add(nodeListTemp.get(0));
                nodeThen.add(nodeListTemp.get(1));
            }
        }
        SQLExpr elseExpr = x.getElseExpr();
        SqlNode nodeElse = convertToSqlNode(elseExpr);
        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        sqlNodeList.add(nodeValue);
        sqlNodeList.add(nodeWhen);
        sqlNodeList.add(nodeThen);
        sqlNodeList.add(nodeElse);
        sqlNode = SqlCase.createSwitched(SqlParserPos.ZERO, nodeValue, nodeWhen, nodeThen, nodeElse);
        return false;
    }

    public boolean visit(SQLCaseExpr.Item x) {
        SQLExpr conditionExpr = x.getConditionExpr();
        SqlNode sqlNode1 = convertToSqlNode(conditionExpr);
        SQLExpr valueExpr = x.getValueExpr();
        SqlNode sqlNode2 = convertToSqlNode(valueExpr);
        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        sqlNodeList.add(sqlNode1);
        sqlNodeList.add(sqlNode2);
        sqlNode = sqlNodeList;
        return false;
    }

    public boolean visit(SQLListExpr x) {
        List<SQLExpr> items = x.getItems();
        List<SqlNode> objects = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            SQLExpr sqlExpr = items.get(i);
            SqlNode sqlNode = convertToSqlNode(sqlExpr);
            objects.add(sqlNode);
        }
        sqlNode = SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, objects);
        return false;
    }

    public boolean visit(SQLPropertyExpr x) { // a.t
        String name = x.getName();
        if (context.isUnderSet()) {
            if (x.getOwner() instanceof SQLVariantRefExpr) {
                this.sqlNode = new SqlIdentifier(ImmutableList.of(((SQLVariantRefExpr) x.getOwner()).getName(), name),
                    SqlParserPos.ZERO);
                return false;
            }

            if (x.getOwner() instanceof SQLIdentifierExpr) {
                this.sqlNode = new SqlIdentifier(ImmutableList.of(((SQLIdentifierExpr) x.getOwner()).getName(), name),
                    SqlParserPos.ZERO);
                return false;
            }
        }

        SQLExpr ownerExpr = x.getOwner();
        SqlNode ownerSqlNode = null;
        List<String> nameList = new ArrayList<>();
        if (ownerExpr != null) {
            ownerSqlNode = convertToSqlNode(ownerExpr);
            if (ownerSqlNode instanceof SqlSystemVar) {
                // system var
                this.sqlNode = SqlSystemVar.create(VariableScope.SESSION, name, SqlParserPos.ZERO);
                return false;
            }
            if (ownerSqlNode instanceof SqlIdentifier) {
                nameList.addAll(((SqlIdentifier) ownerSqlNode).names);
            }
        }

        if (COLUMN_ALL.trim().equals(name)) {
            name = "";
        }
        nameList.add(SQLUtils.normalizeNoTrim(name));

        // String ownernName = x.getOwnernName();
        // SqlNode sqlNode = new SqlIdentifier(Arrays.asList(nameList,
        // SQLUtils.normalizeNoTrim(name)),
        // SqlParserPos.ZERO);

        SqlNode sqlNode = new SqlIdentifier(nameList, SqlParserPos.ZERO);
        this.sqlNode = sqlNode;
        return false;
    }

    public boolean visit(SQLExistsExpr x) {
        SqlOperator sqlOperator = SqlStdOperatorTable.EXISTS;
        SqlNode sqlNode = null;
        if (x.isNot()) {
            sqlNode = SqlStdOperatorTable.NOT_EXISTS.createCall(SqlParserPos.ZERO, convertToSqlNode(x.getSubQuery()));
        } else {
            sqlNode = sqlOperator.createCall(SqlParserPos.ZERO, convertToSqlNode(x.getSubQuery()));
        }
        this.sqlNode = sqlNode;
        return false;
    }

    @Override
    public boolean visit(SQLCurrentTimeExpr x) {
        final String methodName = x.getType().name;
        final SqlOperator functionOperator = new SqlUnresolvedFunction(new SqlIdentifier(methodName.toUpperCase(),
            SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.SYSTEM);

        final SqlBasicCall sqlBasicCall = new SqlBasicCall(functionOperator,
            SqlParserUtil.toNodeArray(ImmutableList.<SqlNode>of()),
            SqlParserPos.ZERO,
            false,
            null);
        final SqlOperator sqlOperator = TDDLSqlFunction.getInstance().matchFunction(sqlBasicCall);

        matchUnsupport(sqlOperator);

        sqlBasicCall.setOperator(sqlOperator);
        this.sqlNode = sqlBasicCall;

        return false;
    }

    @Override
    public boolean visit(SQLCurrentUserExpr x) {
        final SqlOperator functionOperator = new SqlUnresolvedFunction(new SqlIdentifier("CURRENT_USER",
            SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.SYSTEM);

        final SqlBasicCall sqlBasicCall = new SqlBasicCall(functionOperator,
            SqlParserUtil.toNodeArray(ImmutableList.<SqlNode>of()),
            SqlParserPos.ZERO,
            false,
            null);
        final SqlOperator sqlOperator = TDDLSqlFunction.getInstance().matchFunction(sqlBasicCall);

        matchUnsupport(sqlOperator);

        sqlBasicCall.setOperator(sqlOperator);
        this.sqlNode = sqlBasicCall;

        return false;
    }

    public boolean visit(SQLMethodInvokeExpr x) {
        SqlOperator functionOperator = null;
        String methodName = x.getMethodName();
        List<SQLExpr> parameters = Lists.newArrayList(x.getArguments());
        SQLExpr from = x.getFrom();
        SQLExpr forExpr = x.getFor();
        SqlLiteral direction = null;
        SQLExpr using = x.getUsing();
        List<SqlNode> argNodes = new ArrayList<>();

        // Escape function names surrounded by backquotes (`)
        if (methodName.startsWith("`") && methodName.endsWith("`")) {
            methodName = RuleUtils.unescapeName(methodName, false);
        }

        // SqlLiteral.createSymbol(CalciteUtils.timeUnitConvert(null),
        // SqlParserPos.ZERO),
        int skipIndex = -1;
        if (isNotSupportFunction(methodName)) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "Not supported method:"
                + methodName);
        }
        if (StringUtils.equalsIgnoreCase(methodName, SqlStdOperatorTable.TIMESTAMP_ADD.getName())
            || StringUtils.equalsIgnoreCase(methodName, TddlOperatorTable.TIMESTAMP_DIFF.getName())) {
            if (!(parameters.get(0) instanceof SQLIdentifierExpr)) {
                throw new FastSqlParserException(ExceptionType.PARSER_ERROR, "Unknown time unit");
            }
            SQLIdentifierExpr sqlExpr = (SQLIdentifierExpr) parameters.get(0);
            SqlLiteral symbol = SqlLiteral.createSymbol(getTimeUnit(sqlExpr.getName()), SqlParserPos.ZERO);
            argNodes.add(symbol);
            skipIndex = 0;
            functionOperator = new SqlUnresolvedFunction(new SqlIdentifier(methodName.toUpperCase(), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.SYSTEM);
            // CalciteUtils.timeUnitConvert(null);
        } else if (StringUtils.equalsIgnoreCase(methodName, TddlOperatorTable.GET_FORMAT.getName())) {
            SQLIdentifierExpr sqlExpr = (SQLIdentifierExpr) parameters.get(0);
            SqlLiteral symbol = SqlLiteral.createSymbol(SqlTypeName.valueOf(sqlExpr.getName()), SqlParserPos.ZERO);
            argNodes.add(symbol);
            skipIndex = 0;

            functionOperator = new SqlUnresolvedFunction(new SqlIdentifier(methodName.toUpperCase(), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.SYSTEM);
            // CalciteUtils.timeUnitConvert(null);

        } else if (StringUtils.equalsIgnoreCase(methodName, SqlStdOperatorTable.TRIM.getName())) {
            functionOperator = SqlStdOperatorTable.TRIM;
            if (parameters != null && parameters.size() > 0) {
                if (from == null) {
                    from = parameters.remove(parameters.size() - 1);
                }
                if (parameters.size() == 0) {
                    SQLExpr expr = new MySqlCharExpr(" ");
                    parameters.add(expr);
                }
                String trimOption = x.getTrimOption();
                long lTrimOption = FnvHash.Constants.BOTH;
                if (trimOption != null) {
                    lTrimOption = FnvHash.fnv1a_64_lower(trimOption);
                }
                if (lTrimOption == FnvHash.Constants.LEADING) {
                    direction = SqlTrimFunction.Flag.LEADING.symbol(SqlParserPos.ZERO);
                } else if (lTrimOption == FnvHash.Constants.TRAILING) {

                    direction = SqlTrimFunction.Flag.TRAILING.symbol(SqlParserPos.ZERO);
                } else if (lTrimOption == FnvHash.Constants.BOTH) {
                    direction = SqlTrimFunction.Flag.BOTH.symbol(SqlParserPos.ZERO);
                } else {
                    throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT, "not support");
                }
            }
        } else if (StringUtils.equals(methodName.toUpperCase(), SqlStdOperatorTable.CONVERT.getName())) {
            if (using != null) {
                functionOperator = SqlStdOperatorTable.CONVERT;
            } else {
                functionOperator = SqlStdOperatorTable.CAST;
            }
        } else {
            functionOperator = new SqlUnresolvedFunction(new SqlIdentifier(methodName.toUpperCase(), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.SYSTEM);
        }

        SqlLiteral functionQualifier = null;

        if (direction != null) {
            argNodes.add(direction);
        }
        if (parameters != null) {
            int index = 0;
            for (SQLExpr exp : parameters) {
                if (index++ == skipIndex) {
                    continue;
                }

                /**
                 * <pre>
                 * 1. CONVERT("XXX" USING "utf8") has only one parameter, the encoding param (in this case "utf8")
                 *    can be obtained by x.getUsing()
                 * 2. CONVERT(1, SIGNED) has two parameters, the second one is a type name(see {@link org.apache.calcite.sql.type.SqlTypeName} )
                 *    which is case insensitive by default
                 * </pre>
                 */
                if (TStringUtil.equals(methodName.toUpperCase(), SqlStdOperatorTable.CONVERT.getName()) && index == 2) {
                    SQLDataType dataType = null;
                    if (exp instanceof SQLDataTypeRefExpr) {
                        dataType = ((SQLDataTypeRefExpr) exp).getDataType();
                    }
                    /**
                     * fixed since fastsql-2.0.0_preview_949 else if (exp
                     * instanceof SQLIdentifierExpr) { dataType = new
                     * SQLDataTypeImpl(((SQLIdentifierExpr) exp).getName()); }
                     */
                    if (dataType != null) {
                        DateType dateTypeBean = new DateType((SQLDataTypeImpl) dataType).invoke();
                        String name = dataType.getName().toUpperCase();
                        argNodes.add(new SqlDataTypeSpec(new SqlIdentifier(name, SqlParserPos.ZERO),
                            dateTypeBean.p,
                            dateTypeBean.c,
                            dateTypeBean.charset,
                            null,
                            SqlParserPos.ZERO));
                    } else {
                        argNodes.add(convertToSqlNode(exp));
                    }
                } else {
                    argNodes.add(convertToSqlNode(exp));
                }
            }
        }

        if (using != null) {
            if (StringUtils.equals(methodName.toUpperCase(), SqlStdOperatorTable.CONVERT.getName())
                && using instanceof SQLIdentifierExpr) {
                String name = ((SQLIdentifierExpr) using).getSimpleName();
                argNodes.add(SqlCharStringLiteral.createCharString(name, SqlParserPos.ZERO));
            } else {
                argNodes.add(convertToSqlNode(using));
            }
        }
        if (from != null) {
            argNodes.add(convertToSqlNode(from));
        }

        if (forExpr != null) {
            argNodes.add(convertToSqlNode(forExpr));
        }

        if (!(functionOperator instanceof SqlUnresolvedFunction)) {
            this.sqlNode = functionOperator.createCall(SqlParserPos.ZERO, argNodes);
        } else {
            SqlBasicCall sqlBasicCall = new SqlBasicCall(functionOperator,
                SqlParserUtil.toNodeArray(argNodes),
                SqlParserPos.ZERO,
                false,
                functionQualifier);
            SqlOperator sqlOperator = TDDLSqlFunction.getInstance().matchFunction(sqlBasicCall);
            matchUnsupport(sqlOperator);
            sqlBasicCall.setOperator(sqlOperator);
            this.sqlNode = sqlBasicCall;
        }
        return false;
    }

    private void matchUnsupport(SqlOperator sqlOperator) {
        if (sqlOperator == null) {
            return;
        }
        if (TddlOperatorTable.instance().UNSUPPORTED_OPERATORS.contains(sqlOperator)) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Not support operation for:" + sqlOperator.getName());
        }
        // FIXME: integrate these methods to TddlOperatorTable
        if (sqlOperator.getName().toUpperCase().equals("WEIGHT_STRING")) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Not support operation for:" + sqlOperator.getName());
        }

    }

    public boolean visit(SQLSelectItem x) {
        SQLExpr expr = x.getExpr();

        if (expr instanceof SQLIdentifierExpr) {
            visit((SQLIdentifierExpr) expr);
        } else if (expr instanceof SQLPropertyExpr) {
            visit((SQLPropertyExpr) expr);
        } else if (expr instanceof SQLAggregateExpr) {
            visit((SQLAggregateExpr) expr);
        } else {
            expr.accept(this);
        } // select a + (select count(1) from b) as mm from c;
        // select a + (select COUNT(1) from b) as 'a + (select count(1) as
        // 'count(1)' from b)' from c;
        String alias = x.getAlias();
        String alias2 = x.getAlias2();
        if (StringUtils.isNotEmpty(alias) && StringUtils.isNotEmpty(alias2)) {
            // sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]
            // { sqlNode,
            // new SqlIdentifier(Identifier.unescapeName(alias2, false),
            // SqlParserPos.ZERO) }, SqlParserPos.ZERO);

            sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {
                sqlNode,
                new SqlIdentifier(SQLUtils.normalizeNoTrim(alias2), SqlParserPos.ZERO)}, SqlParserPos.ZERO);
        } else if (expr instanceof SQLMethodInvokeExpr
            || expr instanceof SQLExtractExpr
            || expr instanceof SQLAggregateExpr
            || expr instanceof SQLCastExpr
            || expr instanceof SQLBinaryExpr
            || expr instanceof SQLBinaryOpExpr) {
            // method call is not only from SQLMethodInvokeExpr but other method statements.
            sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {
                sqlNode,
                new SqlIdentifier(x.toString(), SqlParserPos.ZERO)}, SqlParserPos.ZERO);
        }

        return false;
    }

    public boolean visit(SQLIdentifierExpr expr) {
        SQLIdentifierExpr y = expr;
        String name = y.getName();
        String normalize = SQLUtils.normalizeNoTrim(name);
        if (DUAL_TABLE.equalsIgnoreCase(normalize)) {
            return false;
        }
        SqlNode sqlNode = new SqlIdentifier(normalize, SqlParserPos.ZERO);
        this.sqlNode = sqlNode;
        return false;
    }

    public boolean visit(SQLSequenceExpr x) {
        SQLName sequence = x.getSequence();
        SQLSequenceExpr.Function function = x.getFunction();
        SqlNode sequenceSqlNode = convertToSqlNode(sequence);
        String name = function.name();
        SQLPropertyExpr sqlPropertyExpr = null;
        if (sequenceSqlNode instanceof SqlIdentifier) {
            SqlIdentifier seqId = (SqlIdentifier) sequenceSqlNode;
            if (seqId.names.size() == 1) {
                // sqlPropertyExpr = new
                // SQLPropertyExpr(sequence.getSimpleName(), function.name);
                sqlPropertyExpr = new SQLPropertyExpr(seqId.names.get(0), function.name);
            } else if (seqId.names.size() == 2) {
                sqlPropertyExpr = new SQLPropertyExpr(SQLUtils.normalizeNoTrim(seqId.names.get(0)),
                    SQLUtils.normalizeNoTrim(seqId.names.get(1)),
                    function.name);
            }
        }

        this.sqlNode = convertToSqlNode(sqlPropertyExpr);
        return false;
    }

    @Override
    public boolean visit(SQLSetStatement x) {
        final List<Pair<SqlNode, SqlNode>> variableAssignmentList = new LinkedList<>();

        VariableScope scope = null;
        final Option option = x.getOption();
        if (null != option) {
            switch (option) {
            case GLOBAL:
                scope = VariableScope.GLOBAL;
                break;
            case SESSION:
            case LOCAL:
                scope = VariableScope.SESSION;
                break;
            case IDENTITY_INSERT:
            case PASSWORD:
            default:
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NEED_IMPLEMENT,
                    "Does not support set option " + x.getOption() + ".");
            } // end of switch
        } // end of if

        try {
            context.getUnderSet().push(true);
            for (SQLAssignItem item : x.getItems()) {
                SqlNode key = convertToSqlNode(item.getTarget());
                SqlNode value = convertToSqlNode(item.getValue());

                if (key instanceof SqlUserDefVar) {
                    variableAssignmentList.add(Pair.of(key, value));
                } else if (key instanceof SqlSystemVar) {
                    final String varTextUp = ((SqlSystemVar) key).getName().toUpperCase();

                    if ("NAMES".equals(varTextUp)) {

                        SqlNode collate = null;
                        if (item.getValue() instanceof MySqlCharExpr) {
                            final MySqlCharExpr charExpr = (MySqlCharExpr) item.getValue();

                            if (TStringUtil.isNotBlank(charExpr.getCollate())) {
                                collate = SqlLiteral.createCharString(charExpr.getCollate(), SqlParserPos.ZERO);
                            }
                        }

                        this.sqlNode = new SqlSetNames(SqlParserPos.ZERO, value, collate);
                        return false;
                    } else if ("CHARACTER SET".equals(varTextUp)) {
                        this.sqlNode = new SqlSetCharacterSet(SqlParserPos.ZERO, value);
                        return false;
                    } else {
                        variableAssignmentList.add(Pair.of(key, value));
                    }
                } else if (key instanceof SqlIdentifier) {
                    String varText = key.toString();

                    SqlIdentifier keyIdentifier = (SqlIdentifier) key;
                    ImmutableList<String> names = keyIdentifier.names;
                    if (names.size() > 1) {
                        if ("@@session".equalsIgnoreCase(names.get(0))) {
                            scope = VariableScope.SESSION;
                            keyIdentifier = new SqlIdentifier(names.subList(1, names.size()), SqlParserPos.ZERO);
                        } else if ("@@global".equalsIgnoreCase(names.get(0))) {
                            scope = VariableScope.GLOBAL;
                            keyIdentifier = new SqlIdentifier(names.subList(1, names.size()), SqlParserPos.ZERO);
                        }
                        varText = keyIdentifier.toString();
                    }

                    String varTextUp = varText.toUpperCase();
                    if ("CHARACTER SET".equals(varTextUp)) {
                        this.sqlNode = new SqlSetCharacterSet(SqlParserPos.ZERO, value);
                        return false;
                    } else {
                        key = SqlSystemVar.create(scope, varText, SqlParserPos.ZERO);
                        variableAssignmentList.add(Pair.of(key, value));
                    }
                } else {
                    String keyStr = RelUtils.stringValue(key);
                    key = SqlSystemVar.create(scope, keyStr, SqlParserPos.ZERO);
                    variableAssignmentList.add(Pair.of(key, value));
                }
            } // end of for
        } finally {
            context.getUnderSet().pop();
        }

        this.sqlNode = new SqlSet(SqlParserPos.ZERO, variableAssignmentList);

        return false;
    }

    @Override
    public boolean visit(MySqlSetTransactionStatement x) {
        SqlNode policy = convertToSqlNode(x.getPolicy());
        this.sqlNode = new SqlSetTransaction(SqlParserPos.ZERO,
            x.getGlobal(),
            x.getSession(),
            x.getIsolationLevel(),
            x.getAccessModel(),
            null == policy ? null : policy.toString());
        return false;
    }

    @Override
    public boolean visit(MySqlAnalyzeStatement x) {
        final List<SqlNode> tableNames = new LinkedList<>();
        if (x.getTableSources() == null || x.getTableSources().isEmpty()) {
            throw new TddlNestableRuntimeException("syntax error: analyze table need to specify a table ");
        }
        for (SQLExprTableSource tableName : x.getTableSources()) {
            tableNames.add(convertToSqlNode(tableName));
        }
        this.sqlNode = new SqlAnalyzeTable(SqlParserPos.ZERO, tableNames);
        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(MySqlOptimizeStatement x) {
        final List<SqlNode> tableNames = new LinkedList<>();
        for (SQLExprTableSource tableName : x.getTableSources()) {
            tableNames.add(convertToSqlNode(tableName));
        }
//        boolean optimizeTableUseDal = false;
//        if(ec.getOriginSql().contains("/*")){
//            Object optimizeTableUseDalObj = parseHint(ec.getSql()).get(ConnectionProperties.OPTIMIZE_TABLE_USE_DAL);
//            optimizeTableUseDal = Boolean.valueOf(String.valueOf(optimizeTableUseDalObj));
//        }
//
//        if(optimizeTableUseDal){
//            this.sqlNode = new SqlOptimizeTable(SqlParserPos.ZERO, tableNames, x.isNoWriteToBinlog(), x.isLocal());
//        }else {
//            this.sqlNode = new SqlOptimizeTableDdl(SqlParserPos.ZERO, tableNames, x.isNoWriteToBinlog(), x.isLocal());
//        }

        this.sqlNode = new SqlOptimizeTableDdl(SqlParserPos.ZERO, tableNames, x.isNoWriteToBinlog(), x.isLocal());
        return false;
    }

    private HashMap<String, Object> parseHint(ByteString sql) {
        HashMap<String, Object> cmdObjects = new HashMap<>();
        List<SQLStatement> stmtList = FastsqlUtils.parseSql(sql.substring(sql.indexOf("/*"), sql.indexOf("*/") + 2));
        ContextParameters contextParameters = new ContextParameters(false);
        for (SQLStatement statement : stmtList) {
            List<SQLCommentHint> hintList = ((MySqlHintStatement) statement).getHints();
            SqlNodeList sqlNodes =
                FastSqlConstructUtils.convertHints(hintList, contextParameters, new ExecutionContext());
            HintConverter.HintCollection collection = new HintConverter.HintCollection();
            HintUtil.collectHint(sqlNodes, collection, false, new ExecutionContext());
            List<HintCmdOperator> hintCmdOperators = collection.cmdHintResult;
            HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean("", cmdObjects, "");
            for (HintCmdOperator op : hintCmdOperators) {
                op.handle(cmdBean);
            }
        }
        return cmdObjects;
    }

    @Override
    public boolean visit(MySqlKillStatement x) {
        final SqlNode threadId = convertToSqlNode(x.getThreadId());
        this.sqlNode = new SqlKill(SqlParserPos.ZERO, threadId);
        return false;
    }

    @Override
    public boolean visit(MySqlCheckTableStatement x) {
        final List<SqlNode> tableNames = new LinkedList<>();
        for (SQLExprTableSource tableName : x.getTables()) {
            tableNames.add(convertToSqlNode(tableName));
        }
        SqlCheckTable sqlCheckTable = new SqlCheckTable(SqlParserPos.ZERO, tableNames);
        sqlCheckTable.setWithLocalPartitions(x.isWithLocalPartitions());
        sqlCheckTable.setDisplayMode(x.getDisplayMode());
        this.sqlNode = sqlCheckTable;
        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);

        return false;
    }

    @Override
    public boolean visit(DrdsShowLocality x) {
        Object schema = context.getParameter(ContextParameterKey.SCHEMA);
        final List<SqlNode> operands = new ArrayList<>();
        operands.add(convertToSqlNode(x.getName()));
        String dbName = (String) schema;
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());
        SqlShowLocalityInfo sqlShowLocalityInfo = new SqlShowLocalityInfo(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.LOCALITY), operands, null, where, orderBy, limit, dbName);

        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);
        this.sqlNode = sqlShowLocalityInfo;

        return false;
    }

    @Override
    public boolean visit(MySqlShowDatasourcesStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = new SqlShowDatasources(SqlParserPos.ZERO, where, orderBy, limit);
        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(SQLShowTablesStatement x) {
        SqlNode dbName = convertToSqlNode(x.getDatabase());
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode like = convertToSqlNode(x.getLike());

        String dbNameStr = null;
        if (dbName instanceof SqlIdentifier) {
            dbNameStr = ((SqlIdentifier) dbName).getLastName();
        }

        Object schema = context.getParameter(ContextParameterKey.SCHEMA);
        this.sqlNode = SqlShowTables.create(SqlParserPos.ZERO,
            x.isFull(),
            dbName,
            null != schema ? (String) schema : "",
            like,
            where,
            null,
            null);

        addPrivilegeVerifyItem(dbNameStr, "", PrivilegePoint.SELECT);

        return false;
    }

    @Override
    public boolean visit(MySqlShowSlowStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = SqlShowSlow.create(SqlParserPos.ZERO, x.isFull(), x.isPhysical(), where, orderBy, limit);

        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(SQLShowOutlinesStatement x) {
        throw new UnsupportedOperationException("SHOW OUTLINES not supported");
    }

    @Override
    public boolean visit(SQLShowRecyclebinStatement stmt) {
        this.sqlNode = new SqlShowRecyclebin(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.RECYCLEBIN),
            ImmutableList.<SqlNode>of());

        return false;
    }

    @Override
    public boolean visit(MysqlShowStcStatement x) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.STC);
        if (x.isHis()) {
            specialIdentifiers.add(SqlSpecialIdentifier.HIS);
        }
        this.sqlNode = new SqlShowStc(SqlParserPos.ZERO,
            specialIdentifiers,
            ImmutableList.<SqlNode>of(),
            null,
            null,
            null,
            null,
            x.isFull());

        return false;
    }

    @Override
    public boolean visit(MysqlShowHtcStatement x) {
        this.sqlNode = new SqlShowHtc(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.HTC),
            ImmutableList.<SqlNode>of(),
            null,
            null,
            null,
            null);
        return false;
    }

    @Override
    public boolean visit(SQLShowPartitionsStmt x) {
        final List<SqlNode> operands = new ArrayList<>();
        operands.add(convertToSqlNode(x.getTableSource()));

        this.sqlNode = new SqlShowPartitions(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.PARTITIONS,
            SqlSpecialIdentifier.FROM), operands, null, null, null, null);
        return false;
    }

    @Override
    public boolean visit(MySqlShowFilesStatement x) {
        final List<SqlNode> operands = new ArrayList<>();
        SqlNode targetTable = convertToSqlNode(x.getName());
        if (context.isTestMode()) {
            String tableName = EagleeyeHelper.rebuildTableName(SQLUtils.normalizeNoTrim
                (x.getName().getSimpleName()), true);
            targetTable = ((SqlIdentifier) targetTable).setName(0, tableName);
        }
        operands.add(targetTable);
        this.sqlNode = new SqlShowFiles(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.TOPOLOGY,
            SqlSpecialIdentifier.FROM), operands);
        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(MySqlShowTopologyStatement x) {
        final List<SqlNode> operands = new ArrayList<>();
        SqlNode targetTable = convertToSqlNode(x.getName());

        if (context.isTestMode()) {
            String tableName = EagleeyeHelper.rebuildTableName(SQLUtils.normalizeNoTrim
                (x.getName().getSimpleName()), true);
            targetTable = ((SqlIdentifier) targetTable).setName(0, tableName);
        }

        operands.add(targetTable);
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = new SqlShowTopology(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.TOPOLOGY,
            SqlSpecialIdentifier.FROM), operands, null, where, orderBy, limit);

        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);

        return false;
    }

    @Override
    public boolean visit(MySqlShowTableInfoStatement x) {
        final List<SqlNode> operands = new ArrayList<>();
        operands.add(convertToSqlNode(x.getName()));
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = new SqlShowTableInfo(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.TABLE, SqlSpecialIdentifier.INFO,
                SqlSpecialIdentifier.FROM), operands, null, where, orderBy, limit);

        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);

        return false;
    }

    @Override
    public boolean visit(MySqlShowBroadcastsStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = new SqlShowBroadcasts(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.BROADCASTS),
            ImmutableList.<SqlNode>of(),
            null,
            where,
            orderBy,
            limit);

        return false;
    }

    @Override
    public boolean visit(MySqlShowDatabaseStatusStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());
        SqlNode like = convertToSqlNode(x.getName());

        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        if (x.isFull()) {
            specialIdentifiers.add(SqlSpecialIdentifier.FULL);
        }
        specialIdentifiers.add(SqlSpecialIdentifier.DB);
        specialIdentifiers.add(SqlSpecialIdentifier.STATUS);
        this.sqlNode = new SqlShowDbStatus(SqlParserPos.ZERO,
            specialIdentifiers,
            ImmutableList.<SqlNode>of(),
            like,
            where,
            orderBy,
            limit,
            x.isFull());

        return false;
    }

    @Override
    public boolean visit(MySqlShowDsStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = new SqlShowDS(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.DS),
            ImmutableList.<SqlNode>of(),
            null,
            where,
            orderBy,
            limit);

        return false;
    }

    @Override
    public boolean visit(SQLShowStatisticStmt x) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        if (x.isFull()) {
            specialIdentifiers.add(SqlSpecialIdentifier.FULL);
        }
        specialIdentifiers.add(SqlSpecialIdentifier.STATS);
        this.sqlNode = new SqlShowStats(SqlParserPos.ZERO,
            specialIdentifiers,
            ImmutableList.<SqlNode>of(),
            null,
            null,
            null,
            null,
            x.isFull());

        return false;
    }

    @Override
    public boolean visit(MySqlShowTraceStatement x) {
        SqlNodeList selectList = SqlNodeList.EMPTY;
        if (x.getSelectList() != null) {
            selectList = new SqlNodeList(
                x.getSelectList().stream()
                    .map(this::convertToSqlNode)
                    .collect(Collectors.toList()), SqlParserPos.ZERO);
        }

        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = new SqlShowTrace(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.TRACE),
            ImmutableList.<SqlNode>of(),
            selectList,
            null,
            where,
            orderBy,
            limit);

        return false;
    }

    @Override
    public boolean visit(MySqlShowSequencesStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        SqlShowSequences showSequences = new SqlShowSequences(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.SEQUENCES),
            ImmutableList.<SqlNode>of(),
            null,
            where,
            orderBy,
            limit);

        this.sqlNode = showSequences;

        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);

        return false;
    }

    @Override
    public boolean visit(MySqlShowRuleStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        SqlNode tableName = convertToSqlNode(x.getName());

        this.sqlNode = SqlShowRule.create(SqlParserPos.ZERO, x.isFull(), tableName, where, orderBy, limit);

        addPrivilegeVerifyItem(null, "", PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(MySqlShowRuleStatusStatement x) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Do not support SHOW RULE STATUS related commands in PolarDB-X");
    }

    @Override
    public boolean visit(MySqlShowDdlStatusStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = new SqlShowDdlStatus(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.DDL,
            SqlSpecialIdentifier.STATUS), ImmutableList.<SqlNode>of(), null, where, orderBy, limit);

        return false;
    }

    @Override
    public boolean visit(DrdsShowDDLJobs x) {
        this.sqlNode = new SqlShowDdlJobs(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.DDL,
            SqlSpecialIdentifier.STATUS), x.isFull(), x.getJobIds());
        return false;
    }

    @Override
    public boolean visit(DrdsShowDDLResults x) {
        SqlShowDdlResults showDdlResults = new SqlShowDdlResults(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.DDL, SqlSpecialIdentifier.STATUS), x.isAllJobs());
        if (!x.isAllJobs()) {
            showDdlResults.setJobIds(x.getJobIds());
        }
        this.sqlNode = showDdlResults;
        return false;
    }

    @Override
    public boolean visit(DrdsShowRebalanceBackFill x) {
        SqlShowRebalanceBackFill showRebalanceBackFill = new SqlShowRebalanceBackFill(SqlParserPos.ZERO,
            ImmutableList.of(), ImmutableList.of(), null, null, null, null);
        this.sqlNode = showRebalanceBackFill;
        return false;
    }

    @Override
    public boolean visit(DrdsShowScheduleResultStatement x) {
        if (x.getScheduleId() == null) {
            throw new TddlNestableRuntimeException("schedule id must not be empty");
        }
        SqlShowScheduleResults showDdlResults = new SqlShowScheduleResults(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.DDL, SqlSpecialIdentifier.STATUS), x.getScheduleId());
        this.sqlNode = showDdlResults;
        return false;
    }

    @Override
    public boolean visit(DrdsCancelDDLJob x) {
        SqlCancelDdlJob cancelDdlJob = new SqlCancelDdlJob(SqlParserPos.ZERO, x.isAllJobs());
        if (!x.isAllJobs()) {
            cancelDdlJob.setJobIds(x.getJobIds());
        }
        this.sqlNode = cancelDdlJob;
        return false;
    }

    @Override
    public boolean visit(DrdsRecoverDDLJob x) {
        SqlRecoverDdlJob recoverDdlJob = new SqlRecoverDdlJob(SqlParserPos.ZERO, x.isAllJobs());
        if (!x.isAllJobs()) {
            recoverDdlJob.setJobIds(x.getJobIds());
        }
        this.sqlNode = recoverDdlJob;
        return false;
    }

    @Override
    public boolean visit(DrdsContinueDDLJob x) {
        SqlContinueDdlJob continueDdlJob = new SqlContinueDdlJob(SqlParserPos.ZERO, x.isAllJobs());
        if (!x.isAllJobs()) {
            continueDdlJob.setJobIds(x.getJobIds());
        }
        this.sqlNode = continueDdlJob;
        return false;
    }

    @Override
    public boolean visit(DrdsPauseDDLJob x) {
        SqlPauseDdlJob pauseDdlJob = new SqlPauseDdlJob(SqlParserPos.ZERO, x.isAllJobs());
        if (!x.isAllJobs()) {
            pauseDdlJob.setJobIds(x.getJobIds());
        }
        this.sqlNode = pauseDdlJob;
        return false;
    }

    @Override
    public boolean visit(DrdsRollbackDDLJob x) {
        SqlRollbackDdlJob rollbackDdlJob = new SqlRollbackDdlJob(SqlParserPos.ZERO, x.isAllJobs());
        if (!x.isAllJobs()) {
            rollbackDdlJob.setJobIds(x.getJobIds());
        }
        this.sqlNode = rollbackDdlJob;
        return false;
    }

    @Override
    public boolean visit(DrdsRemoveDDLJob x) {
        SqlRemoveDdlJob removeDdlJob = new SqlRemoveDdlJob(SqlParserPos.ZERO, x.isAllCompleted(), x.isAllPending());
        if (!x.isAllCompleted() && !x.isAllPending()) {
            removeDdlJob.setJobIds(x.getJobIds());
        }
        this.sqlNode = removeDdlJob;
        return false;
    }

    @Override
    public boolean visit(DrdsInspectDDLJobCache x) {
        this.sqlNode = new SqlInspectDdlJobCache(SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(DrdsClearDDLJobCache x) {
        SqlClearDdlJobCache clearDdlJobCache = new SqlClearDdlJobCache(SqlParserPos.ZERO, x.isAllJobs());
        if (!x.isAllJobs()) {
            clearDdlJobCache.setJobIds(x.getJobIds());
        }
        this.sqlNode = clearDdlJobCache;
        return false;
    }

    @Override
    public boolean visit(DrdsChangeDDLJob x) {
        this.sqlNode = new SqlChangeDdlJob(SqlParserPos.ZERO,
            x.getJobId(),
            x.isSkip(),
            x.isAdd(),
            x.getGroupAndTableNameList());
        return false;
    }

    @Override
    public boolean visit(DrdsBaselineStatement x) {
        if (x.getSelect() != null) {
            StringBuilder builder = new StringBuilder();
            DrdsParameterizeSqlVisitor visitor = new DrdsParameterizeSqlVisitor(builder, true, ec);
            List<Object> outParameters = new ArrayList<>();
            visitor.config(VisitorFeature.OutputParameterizedQuesUnMergeInList, true);
            visitor.config(VisitorFeature.OutputParameterizedUnMergeShardingTable, true);
            visitor.config(VisitorFeature.OutputParameterizedQuesUnMergeValuesList, true);
            visitor.config(VisitorFeature.OutputParameterizedQuesUnMergeOr, true);
            visitor.config(VisitorFeature.OutputParameterizedQuesUnMergeAnd, true);
            visitor.setOutputParameters(outParameters);
            x.getSelect().accept(visitor);

            SqlBaseline baseline = new SqlBaseline(SqlParserPos.ZERO,
                x.getOperation(),
                x.getBaselineIds(),
                (SqlSelect) convertToSqlNode(x.getSelect()));

            baseline.setParameterizedSql(builder.toString());
            List<SQLCommentHint> headHints = x.getHeadHintsDirect();
            if (headHints != null && headHints.size() == 1) {
                String hint = headHints.get(0).toString();
                baseline.setHint(hint);
            }
            Planner.processParameters(outParameters, ec);
            this.sqlNode = baseline;
        } else {
            this.sqlNode = new SqlBaseline(SqlParserPos.ZERO,
                x.getOperation(),
                x.getBaselineIds(),
                (SqlSelect) convertToSqlNode(x.getSelect()));
        }
        return false;
    }

    @Override
    public boolean visit(DrdsMoveDataBase x) {
        SqlMoveDatabase sqlMoveDatabase = new SqlMoveDatabase(SqlParserPos.ZERO,
            x.getStorageGroups(),
            x.isCleanUpCommand());
        this.sqlNode = sqlMoveDatabase;
        return false;
    }

    @Override
    public boolean visit(DrdsShowMoveDatabaseStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        List<String> names = new ArrayList<String>(2);
        names.add("");

        List<String> tableName = new ArrayList<>();
        tableName.add(InformationSchema.NAME);
        tableName.add(VirtualViewType.MOVE_DATABASE.name());
        SqlNode fromClause = new SqlIdentifier(tableName, null, SqlParserPos.ZERO, null);
        // where
        SqlNode whereCalcite = FastSqlConstructUtils.constructWhere(x.getWhere(), context, ec);

        // orderBy
        SqlNodeList orderBySqlNode = FastSqlConstructUtils.constructOrderBy(x.getOrderBy(), context, ec);

        // limit
        SqlNodeList limitNodes = FastSqlConstructUtils.constructLimit(x.getLimit(), context, ec);

        SqlNode offset = null;
        SqlNode limit = null;
        if (limitNodes != null) {
            offset = limitNodes.get(0);
            limit = limitNodes.get(1);
        }

        this.sqlNode = new TDDLSqlSelect(SqlParserPos.ZERO,
            null,
            new SqlNodeList(Lists.newArrayList(new SqlIdentifier(names, SqlParserPos.ZERO)),
                SqlParserPos.ZERO),
            fromClause,
            whereCalcite,
            null,
            null,
            null,
            orderBySqlNode,
            offset,
            limit,
            hints,
            null);
        return false;
    }

    @Override
    public boolean visit(DrdsShowGlobalIndex x) {
        this.sqlNode = new SqlShowGlobalIndex(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.GLOBAL, SqlSpecialIdentifier.INDEXES),
            convertToSqlNode(x.getTableName()));
        return false;
    }

    @Override
    public boolean visit(DrdsShowGlobalDeadlocks x) {
        this.sqlNode = new SqlShowGlobalDeadlocks(SqlParserPos.ZERO, ImmutableList.of());
        return false;
    }

    @Override
    public boolean visit(DrdsShowLocalDeadlocks x) {
        this.sqlNode = new SqlShowLocalDeadlocks(SqlParserPos.ZERO, ImmutableList.of());
        return false;
    }

    @Override
    public boolean visit(SQLShowPartitionsHeatmapStatement x) {
        SqlNode timeRange = convertToSqlNode(x.getTimeRange());
        SqlNode type = convertToSqlNode(x.getType());
        this.sqlNode = SqlShowPartitionsHeatmap.create(SqlParserPos.ZERO, ImmutableList.of(), timeRange, type);
        return false;
    }

    @Override
    public boolean visit(DrdsShowMetadataLock x) {
        this.sqlNode = new SqlShowMetadataLock(SqlParserPos.ZERO,
            ImmutableList.of(),
            convertToSqlNode(x.getSchemaName()));
        return false;
    }

    @Override
    public boolean visit(DrdsShowTransStatement x) {
        this.sqlNode = new SqlShowTrans(SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(DrdsCheckGlobalIndex x) {
        this.sqlNode = new SqlCheckGlobalIndex(SqlParserPos.ZERO,
            convertToSqlNode(x.getIndexName()),
            convertToSqlNode(x.getTableName()),
            x.getExtraCmd());
        return false;
    }

    @Override
    public boolean visit(MySqlUserName x) {
        this.sqlNode = new SqlUserName(x.getSimpleName(), x.getUserName(), x.getHost(), SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLShowGrantsStatement x) {
        if (x instanceof MySql8ShowGrantsStatement) {
            return visit(((MySql8ShowGrantsStatement) x));
        }
        return super.visit(x);
    }

    private boolean visitShowGrantsDrds(MySqlShowGrantsStatement x) {
        final MySqlUserName mysqlUserName = (MySqlUserName) x.getUser();
        final SqlNode user = convertToSqlNode(mysqlUserName);

        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.GRANTS);

        String userName = null;
        String host = null;

        if (null != mysqlUserName) {
            specialIdentifiers.add(SqlSpecialIdentifier.FOR);

            userName = mysqlUserName.getUserName();
            if (userName.startsWith("'") && userName.endsWith("'")) {
                userName = userName.substring(1, userName.length() - 1);
            }

            host = mysqlUserName.getHost();
            if (host.startsWith("'") && host.endsWith("'")) {
                host = host.substring(1, host.length() - 1);
            }
        }

        List<SqlNode> operands = new ArrayList<>();
        if (null != user) {
            operands.add(user);
        }

        this.sqlNode = new SqlShowGrantsLegacy(SqlParserPos.ZERO,
            specialIdentifiers,
            operands,
            null,
            null,
            null,
            null,
            userName,
            host);
        return false;
    }

    private boolean visitShowGrantsPolarx(MySql8ShowGrantsStatement x) {
        Optional<SqlUserName> user;
        switch (x.getUserSpec()) {
        case NONE:
        case CURRENT_USER:
        case CURRENT_USER_FUNC:
            user = Optional.empty();
            break;
        case USERNAME:
            user = Optional.of(SqlUserName.from(x.getUsername()));
            break;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_SERVER, "Unrecognized user spec: " + x.getUserSpec());
        }

        List<SqlUserName> roles = x.getRolesToUse()
            .stream()
            .map(SqlUserName::from)
            .collect(Collectors.toList());

        this.sqlNode = new SqlShowGrants(SqlParserPos.ZERO, user, roles);
        return false;
    }

    @Override
    public boolean visit(MySql8ShowGrantsStatement x) {
        return visitShowGrantsPolarx(x);
    }

    @Override
    public boolean visit(MySqlShowAuthorsStatement x) {
        this.sqlNode = new SqlShowAuthors(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.AUTHORS),
            ImmutableList.<SqlNode>of(),
            null,
            null,
            null,
            null);
        return false;
    }

    @Override
    public boolean visit(MySqlShowVariantsStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode like = convertToSqlNode(x.getLike());
        this.sqlNode = SqlShowVariables.create(SqlParserPos.ZERO, x.isSession(), x.isGlobal(), like, where);
        return false;
    }

    @Override
    public boolean visit(SQLShowCreateTableStatement x) {
        this.sqlNode = SqlShowCreateTable.create(SqlParserPos.ZERO, convertToSqlNode(x.getName()), x.isFull());
        return false;
    }

    @Override
    public boolean visit(SQLShowCreateDatabaseStatement x) {
        this.sqlNode = SqlShowCreateDatabase.create(SqlParserPos.ZERO, convertToSqlNode(x.getName()), x.ifNotExists());
        return false;
    }

    @Override
    public boolean visit(MysqlShowDbLockStatement x) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean visit(MySqlShowMasterStatusStatement x) {
        if (CdcRpcClient.useCdc()) {
            this.sqlNode = new SqlShowMasterStatus(SqlParserPos.ZERO);
            addPrivilegeVerifyItem("*", "*", PrivilegePoint.REPLICATION_CLIENT);
        } else {
            this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.MASTER,
                SqlSpecialIdentifier.STATUS));
        }
        return false;
    }

    @Override
    public boolean visit(MySqlShowBinaryLogsStatement x) {
        if (CdcRpcClient.useCdc()) {
            this.sqlNode = SqlShowBinaryLogs.create(SqlParserPos.ZERO);
            addPrivilegeVerifyItem("*", "*", PrivilegePoint.REPLICATION_CLIENT);
        } else {
            this.sqlNode = new SqlShow(SqlParserPos.ZERO,
                ImmutableList.of(SqlSpecialIdentifier.BINARY, SqlSpecialIdentifier.LOGS));
        }
        return false;
    }

    @Override
    public boolean visit(MySqlShowMasterLogsStatement x) {
        if (CdcRpcClient.useCdc()) {
            this.sqlNode = SqlShowBinaryLogs.create(SqlParserPos.ZERO);
            addPrivilegeVerifyItem("*", "*", PrivilegePoint.REPLICATION_CLIENT);
        } else {
            this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.MASTER,
                SqlSpecialIdentifier.LOGS));
        }
        return false;
    }

    @Override
    public boolean visit(MySqlShowBinLogEventsStatement x) {
        final List<SqlNode> operands = new ArrayList<>();

        final SqlNode logName = convertToSqlNode(x.getIn());
        if (null != logName) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.IN, SqlParserPos.ZERO));
            operands.add(logName);
        }

        final SqlNode from = convertToSqlNode(x.getFrom());
        if (null != from) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.FROM, SqlParserPos.ZERO));
            operands.add(from);
        }

        final SqlNode limit = convertToSqlNode(x.getLimit());
        if (null != limit) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.LIMIT, SqlParserPos.ZERO));
            operands.add(limit);
        }

        if (CdcRpcClient.useCdc()) {
            this.sqlNode = new SqlShowBinlogEvents(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.BINLOG,
                SqlSpecialIdentifier.EVENTS), operands, logName, from, limit);
            addPrivilegeVerifyItem("*", "*", PrivilegePoint.REPLICATION_SLAVE);
        } else {
            this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.BINLOG,
                SqlSpecialIdentifier.EVENTS), operands);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlShowCharacterSetStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode like = convertToSqlNode(x.getPattern());
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.CHARACTER,
            SqlSpecialIdentifier.SET), ImmutableList.<SqlNode>of(), like, where);

        return false;
    }

    @Override
    public boolean visit(MySqlShowCollationStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode like = convertToSqlNode(x.getPattern());
        this.sqlNode = new SqlShow(SqlParserPos.ZERO,
            ImmutableList.of(SqlSpecialIdentifier.COLLATION),
            ImmutableList.<SqlNode>of(),
            like,
            where);

        return false;
    }

    @Override
    public boolean visit(SQLShowColumnsStatement x) {
        final List<SqlNode> operands = new LinkedList<>();
        final SqlNode like = convertToSqlNode(x.getLike());
        final SqlNode where = convertToSqlNode(x.getWhere());

        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();

        if (x.isFull()) {
            specialIdentifiers.add(SqlSpecialIdentifier.FULL);
        }

        specialIdentifiers.add(SqlSpecialIdentifier.COLUMNS);

        int tableIndex = -1;
        final SqlNode table = convertToSqlNode(x.getTable());
        if (null != table) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.FROM, SqlParserPos.ZERO));
            operands.add(table);
            tableIndex = specialIdentifiers.size() + operands.size() - 1;
        }

        int dbIndex = -1;
        final SqlNode db = convertToSqlNode(x.getDatabase());
        if (null != db) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.FROM, SqlParserPos.ZERO));
            operands.add(db);
            dbIndex = specialIdentifiers.size() + operands.size() - 1;
        }

        this.sqlNode = new SqlShow(SqlParserPos.ZERO,
            specialIdentifiers,
            operands,
            like,
            where,
            null,
            null,
            tableIndex,
            dbIndex,
            true);

        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateDatabaseStatement x) {
        /*
        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.CREATE);
        specialIdentifiers.add(SqlSpecialIdentifier.DATABASE);

        if (x.isIfNotExists()) {
            specialIdentifiers.add(SqlSpecialIdentifier.IF);
            specialIdentifiers.add(SqlSpecialIdentifier.NOT);
            specialIdentifiers.add(SqlSpecialIdentifier.EXISTS);
        }

        final List<SqlNode> operands = new LinkedList<>();
        operands.add(convertToSqlNode(x.getDatabase()));

        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers, operands);
        */
        // TODO moyi remove above code and make sure the compatibility
        this.sqlNode = SqlShowCreateDatabase.create(SqlParserPos.ZERO, convertToSqlNode(x.getDatabase()),
            x.isIfNotExists());

        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateEventStatement x) {
        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.CREATE);
        specialIdentifiers.add(SqlSpecialIdentifier.EVENT);

        final List<SqlNode> operands = new LinkedList<>();
        operands.add(convertToSqlNode(x.getEventName()));

        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers, operands);

        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateProcedureStatement x) {
        this.sqlNode =
            SqlShowCreateProcedure.create(SqlParserPos.ZERO, convertToSqlNode(x.getName()));

        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateFunctionStatement x) {
        this.sqlNode =
            SqlShowCreateFunction.create(SqlParserPos.ZERO,
                convertToSqlNode(SQLUtils.rewriteUdfName((SQLName) x.getName())));

        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateTriggerStatement x) {
        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.CREATE);
        specialIdentifiers.add(SqlSpecialIdentifier.TRIGGER);

        final List<SqlNode> operands = new LinkedList<>();
        operands.add(convertToSqlNode(x.getName()));
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers, operands);

        return false;
    }

    @Override
    public boolean visit(SQLShowCreateViewStatement x) {
        this.sqlNode = SqlShowCreateView.create(SqlParserPos.ZERO, convertToSqlNode(x.getName()));

        SQLName tableSource = x.getName();
        if (tableSource instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) tableSource;
            addPrivilegeVerifyItem(propertyExpr.getOwnerName(), propertyExpr.getName(), PrivilegePoint.SHOW_VIEW);
            addPrivilegeVerifyItem(propertyExpr.getOwnerName(), propertyExpr.getName(), PrivilegePoint.SELECT);
        } else {
            if (context.getPrivilegeContext() != null) {
                addPrivilegeVerifyItem(context.getPrivilegeContext().getSchema(), tableSource.getSimpleName(),
                    PrivilegePoint.SHOW_VIEW);
                addPrivilegeVerifyItem(context.getPrivilegeContext().getSchema(), tableSource.getSimpleName(),
                    PrivilegePoint.SELECT);
            }
        }

        return false;
    }

    @Override
    public boolean visit(SQLShowDatabasesStatement x) {
        final SqlNode where = convertToSqlNode(x.getWhere());
        final SqlNode like = convertToSqlNode(x.getLike());

        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.DATABASES);

        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers, ImmutableList.<SqlNode>of(), like, where);

        return false;
    }

    @Override
    public boolean visit(MySqlShowEngineStatement x) {
        SqlNode engineName = convertToSqlNode(x.getName());
        SqlLiteral option = SqlLiteral.createSymbol(x.getOption(), SqlParserPos.ZERO);

        final List<SqlNode> operands = new LinkedList<>();
        operands.add(engineName);
        operands.add(option);

        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.ENGINE), operands);

        return false;
    }

    @Override
    public boolean visit(MySqlShowEnginesStatement x) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        if (x.isStorage()) {
            specialIdentifiers.add(SqlSpecialIdentifier.STORAGE);
        }
        specialIdentifiers.add(SqlSpecialIdentifier.ENGINES);

        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers);

        return false;
    }

    @Override
    public boolean visit(MySqlShowErrorsStatement x) {
        final List<SqlNode> operands = new LinkedList<>();
        if (x.isCount()) {
            operands.add(new SqlBasicCall(SqlStdOperatorTable.COUNT,
                SqlParserUtil.toNodeArray(ImmutableList.<SqlNode>of(SqlIdentifier.star(SqlParserPos.ZERO))),
                SqlParserPos.ZERO,
                false,
                null));
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.ERRORS, SqlParserPos.ZERO));
            this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.<SqlSpecialIdentifier>of(), operands);
        } else {
            SqlNode limit = convertToSqlNode(x.getLimit());
            if (null == limit) {
                this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.ERRORS));
            } else {
                operands.add(limit);
                this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.ERRORS,
                    SqlSpecialIdentifier.LIMIT), operands);
            }
        }

        return false;
    }

    @Override
    public boolean visit(MySqlShowEventsStatement x) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.EVENTS);

        List<SqlNode> operands = new LinkedList<>();
        SqlNode schema = convertToSqlNode(x.getSchema());
        if (null != schema) {
            specialIdentifiers.add(SqlSpecialIdentifier.FROM);
            operands.add(schema);
        }

        SqlNode like = convertToSqlNode(x.getLike());
        SqlNode where = convertToSqlNode(x.getWhere());
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers, operands, like, where);

        return false;
    }

    @Override
    public boolean visit(MySqlShowFunctionCodeStatement x) {
        SqlNode funcName = convertToSqlNode(x.getName());
        final List<SqlNode> operands = new LinkedList<>();
        if (null != funcName) {
            operands.add(funcName);
        }
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.FUNCTION,
            SqlSpecialIdentifier.CODE), operands);

        return false;
    }

    @Override
    public boolean visit(MySqlShowFunctionStatusStatement x) {
        SqlNode like = convertToSqlNode(x.getLike());
        SqlNode where = convertToSqlNode(x.getWhere());
        this.sqlNode = new SqlShowFunctionStatus(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.FUNCTION,
            SqlSpecialIdentifier.STATUS), ImmutableList.<SqlNode>of(), like, where, null, null);

        return false;
    }

    @Override
    public boolean visit(SQLShowIndexesStatement x) {
        final List<SqlNode> operands = new LinkedList<>();

        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.INDEXES);

        int tableIndex = -1;
        final SqlNode table = convertToSqlNode(x.getTable());
        if (null != table) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.FROM, SqlParserPos.ZERO));
            if (table instanceof SqlIdentifier) {
                operands.add(new SqlIdentifier(Util.last(((SqlIdentifier) table).names), SqlParserPos.ZERO));
            } else {
                operands.add(table);
            }
            tableIndex = specialIdentifiers.size() + operands.size() - 1;
        }

        int dbIndex = -1;
        final SqlNode db = convertToSqlNode(x.getDatabase());
        if (null != db) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.FROM, SqlParserPos.ZERO));
            operands.add(db);
            dbIndex = specialIdentifiers.size() + operands.size() - 1;
        }

        final SqlNode where = convertToSqlNode(x.getWhere());

        this.sqlNode = new SqlShowIndex(SqlParserPos.ZERO,
            specialIdentifiers,
            operands,
            null,
            where,
            null,
            null,
            tableIndex,
            dbIndex,
            true);

        return false;
    }

    @Override
    public boolean visit(MySqlShowOpenTablesStatement x) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.OPEN);
        specialIdentifiers.add(SqlSpecialIdentifier.TABLES);

        int dbIndex = -1;
        List<SqlNode> operands = new LinkedList<>();
        SqlNode database = convertToSqlNode(x.getDatabase());
        if (null != database) {
            specialIdentifiers.add(SqlSpecialIdentifier.FROM);
            operands.add(database);
            dbIndex = specialIdentifiers.size() + operands.size() - 1;
        }

        SqlNode like = convertToSqlNode(x.getLike());
        SqlNode where = convertToSqlNode(x.getWhere());
        this.sqlNode = new SqlShow(SqlParserPos.ZERO,
            specialIdentifiers,
            operands,
            like,
            where,
            null,
            null,
            -1,
            dbIndex,
            true);

        return false;
    }

//    @Override
//    public boolean visit(MySqlShowProfileStatement x) {
//        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
//        specialIdentifiers.add(SqlSpecialIdentifier.PROFILE);
//
//        final List<SqlNode> operands = new LinkedList<>();
//        if (null != x.getTypes() && x.getTypes().size() > 0) {
//            String types = TStringUtil.join(x.getTypes(), ",");
//
//            operands.add(SqlLiteral.createSymbol(types, SqlParserPos.ZERO));
//        }
//
//        final SqlNode forQuery = convertToSqlNode(x.getForQuery());
//        if (null != forQuery) {
//            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.FOR, SqlParserPos.ZERO));
//            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.QUERY, SqlParserPos.ZERO));
//            operands.add(forQuery);
//        }
//
//        final SqlNodeList limit = (SqlNodeList) convertToSqlNode(x.getLimit());
//        if (null != limit) {
//            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.LIMIT, SqlParserPos.ZERO));
//            operands.add(limit.get(1));
//
//            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.OFFSET, SqlParserPos.ZERO));
//            operands.add(limit.get(0));
//        }
//
//        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers, operands);
//
//        return false;
//    }

    @Override
    public boolean visit(MySqlShowProfileStatement x) {
        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.PROFILE);
        final List<SqlNode> operands = new LinkedList<>();
        List<String> typeList = new ArrayList<String>();
        if (null != x.getTypes() && x.getTypes().size() > 0) {
            String types = TStringUtil.join(x.getTypes(), ",");
            String[] typeArr = types.split(",");
            for (int i = 0; i < typeArr.length; i++) {
                typeList.add(typeArr[i]);
            }
        }
        final SqlNode forQuery = convertToSqlNode(x.getForQuery());
        final SqlNodeList limit = (SqlNodeList) convertToSqlNode(x.getLimit());
        this.sqlNode = new SqlShowProfile(SqlParserPos.ZERO, specialIdentifiers, typeList, forQuery, limit);
        return false;
    }

    @Override
    public boolean visit(MySqlShowPluginsStatement x) {
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.PLUGINS));

        return false;
    }

    @Override
    public boolean visit(MySqlShowPrivilegesStatement x) {
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.PRIVILEGES));

        return false;
    }

    @Override
    public boolean visit(MySqlShowProcedureCodeStatement x) {
        SqlNode funcName = convertToSqlNode(x.getName());
        final List<SqlNode> operands = new LinkedList<>();
        if (null != funcName) {
            operands.add(funcName);
        }
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.PROCEDURE,
            SqlSpecialIdentifier.CODE), operands);

        return false;
    }

    @Override
    public boolean visit(MySqlShowProcedureStatusStatement x) {
        SqlNode like = convertToSqlNode(x.getLike());
        SqlNode where = convertToSqlNode(x.getWhere());
        this.sqlNode = new SqlShowProcedureStatus(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.PROCEDURE,
            SqlSpecialIdentifier.STATUS), ImmutableList.<SqlNode>of(), like, where, null, null);

        return false;
    }

    @Override
    public boolean visit(MySqlShowProcessListStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());

        this.sqlNode = SqlShowProcesslist.create(SqlParserPos.ZERO, x.isFull(), false, null, where, orderBy, limit);

        return false;
    }

    @Override
    public boolean visit(MySqlShowPhysicalProcesslistStatement x) {
        SqlNode where = convertToSqlNode(x.getWhere());
        SqlNode orderBy = convertToSqlNode(x.getOrderBy());
        SqlNode limit = convertToSqlNode(x.getLimit());
        this.sqlNode = SqlShowProcesslist.create(SqlParserPos.ZERO, x.isFull(), true, null, where, orderBy, limit);

        return false;
    }

    @Override
    public boolean visit(MySqlShowProfilesStatement x) {
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.PROFILES));

        return false;
    }

    @Override
    public boolean visit(MySqlShowRelayLogEventsStatement x) {
        final List<SqlNode> operands = new LinkedList<>();

        final SqlNode logName = convertToSqlNode(x.getLogName());
        if (null != logName) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.IN, SqlParserPos.ZERO));
            operands.add(logName);
        }

        final SqlNode from = convertToSqlNode(x.getFrom());
        if (null != from) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.FROM, SqlParserPos.ZERO));
            operands.add(from);
        }

        final SqlNode limit = convertToSqlNode(x.getLimit());
        if (null != limit) {
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.LIMIT, SqlParserPos.ZERO));
            operands.add(limit);
        }

        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.RELAYLOG,
            SqlSpecialIdentifier.EVENTS), operands);

        return false;
    }

    @Override
    public boolean visit(MySqlShowSlaveHostsStatement x) {
        this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.SLAVE,
            SqlSpecialIdentifier.HOSTS));

        return false;
    }

    @Override
    public boolean visit(MySqlShowStatusStatement x) {
        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        if (x.isGlobal()) {
            specialIdentifiers.add(SqlSpecialIdentifier.GLOBAL);
        }
        if (x.isSession()) {
            specialIdentifiers.add(SqlSpecialIdentifier.SESSION);
        }
        specialIdentifiers.add(SqlSpecialIdentifier.STATUS);

        SqlNode like = convertToSqlNode(x.getLike());
        SqlNode where = convertToSqlNode(x.getWhere());

        this.sqlNode = new SqlShow(SqlParserPos.ZERO, specialIdentifiers, ImmutableList.<SqlNode>of(), like, where);

        return false;
    }

    @Override
    public boolean visit(MySqlShowTableStatusStatement x) {
        SqlNode dbName = convertToSqlNode(x.getDatabase());
        SqlNode like = convertToSqlNode(x.getLike());
        SqlNode where = convertToSqlNode(x.getWhere());
        this.sqlNode = SqlShowTableStatus.create(SqlParserPos.ZERO, dbName, like, where);

        return false;
    }

    @Override
    public boolean visit(MySqlShowTriggersStatement x) {
        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.TRIGGERS);

        int dbIndex = -1;
        final List<SqlNode> operands = new LinkedList<>();
        final SqlNode database = convertToSqlNode(x.getDatabase());
        if (null != database) {
            specialIdentifiers.add(SqlSpecialIdentifier.FROM);
            operands.add(database);
            dbIndex = specialIdentifiers.size() + operands.size() - 1;
        }

        final SqlNode like = convertToSqlNode(x.getLike());
        final SqlNode where = convertToSqlNode(x.getWhere());

        this.sqlNode = new SqlShow(SqlParserPos.ZERO,
            specialIdentifiers,
            operands,
            like,
            where,
            null,
            null,
            -1,
            dbIndex,
            true);

        return false;
    }

    @Override
    public boolean visit(MySqlShowWarningsStatement x) {
        final List<SqlNode> operands = new LinkedList<>();
        if (x.isCount()) {
            operands.add(new SqlBasicCall(SqlStdOperatorTable.COUNT,
                SqlParserUtil.toNodeArray(ImmutableList.<SqlNode>of(SqlIdentifier.star(SqlParserPos.ZERO))),
                SqlParserPos.ZERO,
                false,
                null));
            operands.add(SqlLiteral.createSymbol(SqlSpecialIdentifier.WARNINGS, SqlParserPos.ZERO));
            this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.<SqlSpecialIdentifier>of(), operands);
        } else {
            SqlNode limit = convertToSqlNode(x.getLimit());
            if (null == limit) {
                this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.WARNINGS));
            } else {
                operands.add(limit);
                this.sqlNode = new SqlShow(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.WARNINGS,
                    SqlSpecialIdentifier.LIMIT), operands);
            }
        }

        return false;
    }

    @Override
    public boolean visit(MySqlHintStatement x) {
        if (null == x.getHintStatements() || x.getHintStatements().size() <= 0) {
            this.sqlNode = null;
        } else {
            final List<SqlNode> sqlNodes = new ArrayList<>();
            for (SQLStatement statement : x.getHintStatements()) {
                final SqlNode sqlNode = convertToSqlNode(statement);
                if (null != sqlNode) {
                    sqlNodes.add(sqlNode);
                }
            }
            if (sqlNodes.size() <= 0) {
                this.sqlNode = null;
            } else {
                this.sqlNode = new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
            }
        }
        return false;
    }

    public SqlNode convertToSqlNode(SQLObject ast) {
        return FastSqlConstructUtils.convertToSqlNode(ast, context, ec);
    }

    private boolean isSqlAllExpr(SQLExpr x) {
        return x.getClass() == SQLAllExpr.class;
    }

    private boolean isAnyOrSomeExpr(SQLExpr x) {
        return x.getClass() == SQLAnyExpr.class || x.getClass() == SQLSomeExpr.class;
    }

    public boolean visit(SQLLimit limit) {
        List<SqlNode> limitNodes = new ArrayList<>(2);
        if (limit == null) {
            return false;
        }
        if (limit.getRowCount() != null) {
            if (limit.getOffset() == null) {
                limit.setOffset(new SQLNumberExpr(0));
            }
        }
        limitNodes.add(convertToSqlNode(limit == null ? null : limit.getOffset()));
        limitNodes.add(convertToSqlNode(limit == null ? null : limit.getRowCount()));
        this.sqlNode = new SqlNodeList(limitNodes, SqlParserPos.ZERO);
        return false;
    }

    public SqlNode getSqlNode() {
        return this.sqlNode;
    }

    public boolean visit(MySqlIndexHintImpl x) {
        List<SQLName> indexList = x.getIndexList();
        SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
        for (int i = 0; i < indexList.size(); i++) {
            SQLName sqlName = indexList.get(i);
            String simpleName = sqlName.getSimpleName();
            if (context.isTestMode() && !simpleName.equalsIgnoreCase("PRIMARY")) {
                simpleName = EagleeyeHelper.rebuildTableName(simpleName, true);
            }
            SqlCharStringLiteral charString = SqlCharStringLiteral.createCharString(simpleName, SqlParserPos.ZERO);
            list.add(charString);
        }
        String indexValue = IndexType.FORCE.getIndex();
        if (x instanceof MySqlUseIndexHint) {
            indexValue = IndexType.USE.getIndex();
        } else if (x instanceof MySqlIgnoreIndexHint) {
            indexValue = IndexType.IGNORE.getIndex();
        }
        SqlCharStringLiteral charString = SqlLiteral.createCharString(indexValue, SqlParserPos.ZERO);
        SqlCharStringLiteral forOption = null;
        if (x.getOption() != null) {
            forOption = SqlLiteral.createCharString(x.getOption().name, SqlParserPos.ZERO);
        }
        this.sqlNode = SqlIndexHintOperator.INDEX_OPERATOR.createCall(charString, forOption, list, SqlParserPos.ZERO);

        return false;

    }

    private static class DateType {

        private SQLDataTypeImpl dataType;
        private Integer p = -1;
        private Integer c = -1;
        private String charset;

        public DateType(SQLDataTypeImpl dataType) {
            this.dataType = dataType;
        }

        private void setCharset() {
            if (dataType instanceof SQLCharacterDataType) {
                SQLCharacterDataType charType = (SQLCharacterDataType) dataType;
                if (charType.getCharSetName() != null) {
                    charset = charType.getCharSetName();
                }
            }
        }

        public DateType invokeCast() {
            if (!SQLDataTypeImpl.class.isInstance(dataType)) {
                return this;
            }
            String[] signed = {"UNSIGNED", "SIGNED"};
            String name = dataType.getName();
            String dataTypeString = null;
            if (signed[1].equalsIgnoreCase(name)) {
                if (!dataType.isUnsigned()) {
                    dataTypeString = signed[1];
                } else {
                    dataTypeString = signed[0];
                }
            } else {
                dataTypeString = name.toUpperCase();
            }
            if (dataType.isZerofill()) { // Nothing
            }
            SqlTypeName castType = getCastType(dataTypeString);
            setPAndC(castType);
            setCharset();
            return this;
        }

        public DateType invoke() {
            if (!SQLDataTypeImpl.class.isInstance(dataType)) {
                return this;
            }
            String[] signed = {"UNSIGNED", "SIGNED"};
            String name = dataType.getName();
            String dataTypeString = null;
            if (signed[1].equalsIgnoreCase(name)) {
                if (!dataType.isUnsigned()) {
                    dataTypeString = signed[1];
                } else {
                    dataTypeString = signed[0];
                }
            } else {
                dataTypeString = name.toUpperCase();
            }
            if (dataType.isZerofill()) { // Nothing
            }
            final List<SQLExpr> arguments = dataType.getArguments();
            if (arguments.size() > 0) {
                final SQLExpr sqlExpr = arguments.get(0);
                if (sqlExpr instanceof SQLIntegerExpr) {
                    final Number number = ((SQLIntegerExpr) sqlExpr).getNumber();
                    this.p = number.intValue();
                }
            }
            SqlTypeName castType = SqlTypeName.getNameForJdbcType(TypeUtils.mysqlTypeToJdbcType(dataTypeString));
            setPAndC(castType);
            setCharset();
            return this;
        }

        private void setPAndC(SqlTypeName castType) {
            List<SQLExpr> arguments = dataType.getArguments();
            if (arguments == null) {
                return;
            }
            if (SqlTypeFamily.DATETIME.getTypeNames().contains(castType)) {
                // for timestamp / datetime / time (fsp)
                this.c = this.p;
                this.p = TddlRelDataTypeSystemImpl.getInstance().getMaxPrecision(castType);
            } else {
                for (int i = 0; i < arguments.size(); i++) {
                    SQLExpr sqlExprTemp = arguments.get(i);
                    if (SqlTypeName.FRACTIONAL_TYPES.contains(castType)) {
                        if (sqlExprTemp instanceof SQLIntegerExpr) {
                            SQLIntegerExpr sqlExpr = (SQLIntegerExpr) sqlExprTemp;
                            Integer v = (Integer) sqlExpr.getValue();
                            if (i == 0) {
                                p = v;
                            }
                            if (i == 1) {
                                c = v;
                            }
                        } else {
                            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                                "Not support cast parameter.");
                        }
                    }
                }
            }
        }
    }

    private void addPrivilegeVerifyItem(String dbName, String tableName, PrivilegePoint priv) {
        PrivilegeVerifyItem item = new PrivilegeVerifyItem();
        item.setDb(dbName);
        item.setTable(tableName);
        item.setPrivilegePoint(priv);
        if (context.getPrivilegeContext() != null) {
            context.getPrivilegeContext().addPrivilegeVerifyItem(item);
        }
    }

    private SqlIdentifier generateSequenceTableName(String schemaName) {
        SqlIdentifier tableName;
        if (StringUtils.isBlank(schemaName)) {
            tableName = new SqlIdentifier("dual", SqlParserPos.ZERO);
        } else {
            tableName = new SqlIdentifier(Arrays.asList(schemaName, "dual"), SqlParserPos.ZERO);
        }

        return tableName;
    }

    @Override
    public boolean visit(MySqlLockTableStatement x) {
        List<SqlNode> tables = new ArrayList<>();
        List<SqlLockTable.LockType> lockTypes = new ArrayList<>();
        for (MySqlLockTableStatement.Item lockItem : x.getItems()) {
            tables.add(convertToSqlNode(lockItem.getTableSource().getExpr()));
            lockTypes.add(convertLockType(lockItem.getLockType()));
        }
        sqlNode = new SqlLockTable(SqlParserPos.ZERO, tables, lockTypes);
        return false;
    }

    private static SqlLockTable.LockType convertLockType(MySqlLockTableStatement.LockType t) {
        switch (t) {
        case READ:
            return SqlLockTable.LockType.READ;
        case READ_LOCAL:
            return SqlLockTable.LockType.READ_LOCAL;
        case WRITE:
            return SqlLockTable.LockType.WRITE;
        case LOW_PRIORITY_WRITE:
            return SqlLockTable.LockType.LOW_PRIORITY_WRITE;
        }
        throw new AssertionError();
    }

    @Override
    public boolean visit(MySqlUnlockTablesStatement x) {
        sqlNode = new SqlUnlockTable(SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(DrdsInspectRuleVersionStatement x) {
        sqlNode = new SqlInspectRuleVersion(SqlParserPos.ZERO, x.isIgnoreManager());
        return false;
    }

    @Override
    public boolean visit(DrdsChangeRuleVersionStatement x) {
        throw new AssertionError();
    }

    @Override
    public boolean visit(DrdsRefreshLocalRulesStatement x) {
        sqlNode = new SqlRefreshLocalRules(SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(DrdsClearSeqCacheStatement x) {
        SqlIdentifier name;
        if (x.isAll()) {
            name = new SqlIdentifier("ALL", SqlParserPos.ZERO);
        } else {
            String sequence = SQLUtils.normalizeNoTrim(x.getName().getSimpleName());
            if (x.getName() instanceof SQLPropertyExpr) {
                String schema = SQLUtils.normalizeNoTrim(((SQLPropertyExpr) x.getName()).getOwnernName());
                name = new SqlIdentifier(ImmutableList.of(schema, sequence), SqlParserPos.ZERO);
            } else {
                name = new SqlIdentifier(sequence, SqlParserPos.ZERO);
            }
        }
        sqlNode = new SqlClearSeqCache(SqlParserPos.ZERO, name);
        return false;
    }

    @Override
    public boolean visit(DrdsInspectGroupSeqRangeStatement x) {
        SqlIdentifier name =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getName().getSimpleName()), SqlParserPos.ZERO);
        sqlNode = new SqlInspectGroupSeqRange(SqlParserPos.ZERO, name);
        return false;
    }

    @Override
    public boolean visit(DrdsConvertAllSequencesStatement x) {
        String fromTypeValue = x.getFromType().getSimpleName();
        String toTypeValue = x.getToType().getSimpleName();
        String schemaName = null;
        boolean allSchemata = true;

        SequenceAttribute.Type fromType;
        try {
            fromType = SequenceAttribute.Type.valueOf(fromTypeValue.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER, "Invalid FROM sequence type '" + fromTypeValue + "'");
        }

        SequenceAttribute.Type toType;
        try {
            toType = SequenceAttribute.Type.valueOf(toTypeValue.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER, "Invalid TO sequence type '" + toTypeValue + "'");
        }

        if (fromType == SequenceAttribute.Type.SIMPLE || toType == SequenceAttribute.Type.SIMPLE) {
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                "Don't support Simple Sequence related conversion in PolarDB-X 2.0");
        }

        if (fromType == toType) {
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                "Don't allow conversion between two same sequence types");
        }

        if (!x.isAllSchemata()) {
            schemaName = x.getSchemaName().getSimpleName().toLowerCase();
            allSchemata = false;
        }

        sqlNode = new SqlConvertAllSequences(SqlParserPos.ZERO, fromType, toType, schemaName, allSchemata);

        return false;
    }

    @Override
    public boolean visit(SQLSavePointStatement x) {
        SqlIdentifier name =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getName().getSimpleName()), SqlParserPos.ZERO);
        sqlNode = new SqlSavepoint(SqlParserPos.ZERO, name, SqlSavepoint.Action.SET_SAVEPOINT);
        return false;
    }

    @Override
    public boolean visit(SQLRollbackStatement x) {
        if (x.getTo() == null) {
            throw new UnsupportedOperationException("ROLLBACK should be handled in ServerQueryHandler");
        }
        SqlIdentifier name = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTo().getSimpleName()), SqlParserPos.ZERO);
        sqlNode = new SqlSavepoint(SqlParserPos.ZERO, name, SqlSavepoint.Action.ROLLBACK_TO);
        return false;
    }

    @Override
    public boolean visit(SQLReleaseSavePointStatement x) {
        SqlIdentifier name =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getName().getSimpleName()), SqlParserPos.ZERO);
        sqlNode = new SqlSavepoint(SqlParserPos.ZERO, name, SqlSavepoint.Action.RELEASE);
        return false;
    }

    @Override
    public boolean visit(MySqlPartitionByKey x) {
        SqlPartitionByHash sqlPartitionByHash = new SqlPartitionByHash(true, false, SqlParserPos.ZERO);
        sqlPartitionByHash.setPartitionsCount(convertToSqlNode(x.getPartitionsCount()));
        for (SQLExpr sqlExpr : x.getColumns()) {
            sqlPartitionByHash.getColumns().add(convertToSqlNode(sqlExpr));
        }
        for (SQLPartition sqlPartiton : x.getPartitions()) {
            sqlPartitionByHash.getPartitions().add((SqlPartition) convertToSqlNode(sqlPartiton));
        }
        sqlPartitionByHash.setSubPartitionBy((SqlSubPartitionBy) convertToSqlNode(x.getSubPartitionBy()));
        sqlNode = sqlPartitionByHash;
        sqlPartitionByHash.setSourceSql(x.toString());
        return false;
    }

    @Override
    public boolean visit(SQLPartitionByHash x) {
        if (false) {
            String method = "HASH";
            if (x.isUnique()) {
                method = "UNI_HASH";
            }
            SqlUnresolvedFunction functionOperator =
                new SqlUnresolvedFunction(new SqlIdentifier(method, SqlParserPos.ZERO),
                    null,
                    null,
                    null,
                    null,
                    SqlFunctionCategory.SYSTEM);

            List<SqlNode> argNodes = new ArrayList<>();
            for (SQLExpr column : x.getColumns()) {
                argNodes.add(convertToSqlNode(column));
            }
            this.sqlNode = functionOperator.createCall(SqlParserPos.ZERO, argNodes);
            return false;
        } else {
            SqlPartitionByHash sqlPartitionByHash = new SqlPartitionByHash(x.isKey(), x.isUnique(), SqlParserPos.ZERO);
            sqlPartitionByHash.setPartitionsCount(convertToSqlNode(x.getPartitionsCount()));
            for (SQLExpr sqlExpr : x.getColumns()) {
                sqlPartitionByHash.getColumns().add(convertToSqlNode(sqlExpr));
            }
            for (SQLPartition sqlPartiton : x.getPartitions()) {
                sqlPartitionByHash.getPartitions().add((SqlPartition) convertToSqlNode(sqlPartiton));
            }
            sqlPartitionByHash.setSubPartitionBy((SqlSubPartitionBy) convertToSqlNode(x.getSubPartitionBy()));
            sqlNode = sqlPartitionByHash;
            sqlPartitionByHash.setSourceSql(x.toString());
            return false;
        }
    }

    @Override
    public boolean visit(SQLPartitionByRange x) {
        SqlPartitionByRange part = new SqlPartitionByRange(SqlParserPos.ZERO);
        part.setColumns(x.isColumns());
        part.setInterval((SqlBasicCall) convertToSqlNode(x.getInterval()));
        part.setStartWith((SqlCharStringLiteral) convertToSqlNode(x.getStartWith()));
        part.setLifeCycleNum((SqlNumericLiteral) convertToSqlNode(x.getLifecycle()));
        part.setExpireAfter((SqlNumericLiteral) convertToSqlNode(x.getExpireAfter()));
        part.setPreAllocate((SqlNumericLiteral) convertToSqlNode(x.getPreAllocate()));
        if (x.getPivotDateExpr() != null) {
            LocalPartitionDefinitionInfo.assertValidPivotDateExpr(x.getPivotDateExpr());
        }
        part.setPivotDateExpr(convertToSqlNode(x.getPivotDateExpr()));
        part.setDisableSchedule(x.isDisableSchedule());
        for (SQLExpr sqlExpr : x.getColumns()) {
            part.getColumns().add(convertToSqlNode(sqlExpr));
        }
        for (SQLPartition sqlPartiton : x.getPartitions()) {
            part.getPartitions().add(convertToSqlNode(sqlPartiton));
        }
        part.setSubPartitionBy((SqlSubPartitionBy) convertToSqlNode(x.getSubPartitionBy()));
        part.setSourceSql(x.toString());
        sqlNode = part;
        return false;
    }

    @Override
    public boolean visit(DrdsAlterTableAllocateLocalPartition x) {
        SqlAlterTableAllocateLocalPartition result =
            new SqlAlterTableAllocateLocalPartition(SqlParserPos.ZERO);
        sqlNode = result;
        return false;
    }

    @Override
    public boolean visit(DrdsAlterTableExpireLocalPartition x) {

        List<SqlIdentifier> sqlNodes = new ArrayList<>();
        for (SQLExpr sqlExpr : x.getPartitions()) {
            SqlIdentifier partition = (SqlIdentifier) convertToSqlNode(sqlExpr);
            sqlNodes.add(partition);
        }

        SqlAlterTableExpireLocalPartition result =
            new SqlAlterTableExpireLocalPartition(SqlParserPos.ZERO, sqlNodes);

        sqlNode = result;
        return false;
    }

    @Override
    public boolean visit(SQLPartitionByList x) {
        SqlPartitionByList sqlPartitionByList = new SqlPartitionByList(SqlParserPos.ZERO);
        sqlPartitionByList.setColumns(x.isColumns());
        sqlPartitionByList.setPartitionsCount(convertToSqlNode(x.getPartitionsCount()));
        for (SQLExpr sqlExpr : x.getColumns()) {
            sqlPartitionByList.getColumns().add(convertToSqlNode(sqlExpr));
        }
        for (SQLPartition sqlPartiton : x.getPartitions()) {
            sqlPartitionByList.getPartitions().add((SqlPartition) convertToSqlNode(sqlPartiton));
        }
        sqlPartitionByList.setSubPartitionBy((SqlSubPartitionBy) convertToSqlNode(x.getSubPartitionBy()));
        sqlNode = sqlPartitionByList;
        sqlPartitionByList.setSourceSql(x.toString());
        return false;
    }

    @Override
    public boolean visit(SQLPartition x) {
        SqlNode name = convertToSqlNode(x.getName());
        SqlNode values = convertToSqlNode(x.getValues());
        SqlPartition sqlPartition = new SqlPartition(name, (SqlPartitionValue) values, SqlParserPos.ZERO);
        if (x.getLocality() != null) {
            SqlNode locality = convertToSqlNode(x.getLocality());
            sqlPartition.setLocality(RelUtils.stringValue(locality));
        }
        for (SQLSubPartition subPartition : x.getSubPartitions()) {
            sqlPartition.getSubPartitions().add((SqlSubPartition) convertToSqlNode(subPartition));
        }
        sqlNode = sqlPartition;
        return false;
    }

    @Override
    public boolean visit(SQLPartitionValue x) {
        SqlPartitionValue.Operator operator = null;
        final String maxValue = "maxValue";
        switch (x.getOperator()) {
        case In:
            operator = SqlPartitionValue.Operator.In;
            break;
        case LessThan:
            operator = SqlPartitionValue.Operator.LessThan;
            break;
        case List:
            operator = SqlPartitionValue.Operator.List;
            break;
        default:
            break;
        }
        SqlPartitionValue sqlPartitionValue = new SqlPartitionValue(operator, SqlParserPos.ZERO);
        for (SQLExpr sqlExpr : x.getItems()) {
            SqlNode item = convertToSqlNode(sqlExpr);
            SqlPartitionValueItem valueItem = new SqlPartitionValueItem(item);
            if (operator == SqlPartitionValue.Operator.LessThan && sqlExpr instanceof SQLIdentifierExpr) {
                if (maxValue.equalsIgnoreCase(((SQLIdentifierExpr) sqlExpr).getSimpleName())) {
                    valueItem.setMaxValue(true);
                }
            }
            sqlPartitionValue.getItems().add(valueItem);
        }
        sqlNode = sqlPartitionValue;
        return false;
    }

    @Override
    public boolean visit(SQLSubPartitionByHash x) {
        SqlNode expr = convertToSqlNode(x.getExpr());
        SqlNode subPartitionCount = convertToSqlNode(x.getSubPartitionsCount());
        SqlSubPartitionByHash sqlSubPartitionByHash =
            new SqlSubPartitionByHash(expr, x.isKey(), subPartitionCount, SqlParserPos.ZERO);
        for (SQLSubPartition sqlSubPartition : x.getSubPartitionTemplate()) {
            sqlSubPartitionByHash.getSubPartitionTemplate().add((SqlSubPartition) convertToSqlNode(sqlSubPartition));
        }
        sqlNode = sqlSubPartitionByHash;
        return false;
    }

    @Override
    public boolean visit(SQLSubPartitionByRange x) {
        return false;
    }

    @Override
    public boolean visit(SQLSubPartitionByList x) {
        return false;
    }

    @Override
    public boolean visit(SQLSubPartition x) {
        SqlNode name = convertToSqlNode(x.getName());
        SqlPartitionValue values = (SqlPartitionValue) convertToSqlNode(x.getValues());
        sqlNode = new SqlSubPartition(SqlParserPos.ZERO, name, values);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableExchangePartition x) {
        assert x.getPartitions().size() >= 1;
        List<SqlNode> partitions = new ArrayList<>();
        for (SQLObject sqlObject : x.getPartitions()) {
            SqlNode partition = convertToSqlNode(sqlObject);
            partitions.add(partition);
        }
        SqlNode tableName = convertToSqlNode(x.getTable());
        boolean validation = x.getValidation() == null ? false : x.getValidation().booleanValue();
        sqlNode =
            new SqlAlterTableExchangePartition(SqlParserPos.ZERO, validation, tableName, partitions);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAddPartition x) {
        assert x.getPartitions().size() >= 1;
        List<SqlNode> sqlNodes = new ArrayList<>();
        for (SQLObject sqlObject : x.getPartitions()) {
            SqlNode partition = convertToSqlNode(sqlObject);
            sqlNodes.add(partition);
        }
        SqlAlterTableAddPartition sqlAddPartition =
            new SqlAlterTableAddPartition(SqlParserPos.ZERO, sqlNodes);
        sqlNode = sqlAddPartition;

        return false;
    }

    @Override
    public boolean visit(SQLAlterTableDropPartition x) {
        // one SQLAlterTableDropPartition only contain one partition
        assert x.getPartitions().size() >= 1;
        List<SqlNode> sqlNodes = new ArrayList<>();
        for (SQLExpr sqlExpr : x.getPartitions()) {
            SqlNode partition = convertToSqlNode(sqlExpr);
            sqlNodes.add(partition);
        }
        SqlAlterTableDropPartition sqlDropPartition = new SqlAlterTableDropPartition(SqlParserPos.ZERO, sqlNodes);
        sqlNode = sqlDropPartition;
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableTruncatePartition x) {
        // one SQLAlterTableDropPartition only contain one partition
        assert x.getPartitions().size() == 1;
        SqlIdentifier name =
            (SqlIdentifier) (convertToSqlNode(x.getPartitions().get(0)));
        SqlAlterTableTruncatePartition sqlTruncatePartition =
            new SqlAlterTableTruncatePartition(SqlParserPos.ZERO, name);
        sqlNode = sqlTruncatePartition;
        return false;
    }

    @Override
    public boolean visit(DrdsCreateCclRuleStatement x) {
        SqlIdentifier ruleName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getName().getSimpleName()), SqlParserPos.ZERO);
        SqlIdentifier dbName = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTable().getSchema()), SqlParserPos.ZERO);
        SqlIdentifier tableName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getTable().getTableName()), SqlParserPos.ZERO);
        SqlUserName userName = (SqlUserName) convertToSqlNode(x.getUser());
        SqlIdentifier FOR = new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getFOR().getSimpleName()), SqlParserPos.ZERO);
        SQLListExpr fastSqlKeywords = x.getKeywords();
        SqlNodeList keywords = null;
        if (fastSqlKeywords != null) {
            List<SqlNode> keywordList =
                fastSqlKeywords.getItems().stream().map((e) -> convertToSqlNode((SQLCharExpr) e)).collect(
                    Collectors.toList());
            keywords = new SqlNodeList(keywordList, SqlParserPos.ZERO);
        }
        SQLListExpr sqlCharTemplateId = x.getTemplate();
        SqlNodeList templdateId = null;
        if (sqlCharTemplateId != null) {
            List<SqlNode> templateList =
                sqlCharTemplateId.getItems().stream().map((e) -> convertToSqlNode((SQLCharExpr) e)).collect(
                    Collectors.toList());
            templdateId = new SqlNodeList(templateList, SqlParserPos.ZERO);
        }
        List<Pair<SqlNode, SqlNode>> withList = new ArrayList<>(x.getWith().size());
        for (SQLAssignItem sqlAssignItem : x.getWith()) {
            SqlIdentifier target =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLVariantRefExpr) sqlAssignItem.getTarget()).getName()),
                    SqlParserPos.ZERO);
            SqlNumericLiteral value =
                (SqlNumericLiteral) convertToSqlNode((SQLIntegerExpr) (sqlAssignItem.getValue()));
            withList.add(Pair.of(target, value));
        }
        SqlNode query = null;
        SQLCharExpr queryCharExpr = x.getQuery();
        if (queryCharExpr != null) {
            query = (SqlNode) convertToSqlNode(queryCharExpr);
        }
        sqlNode = new SqlCreateCclRule(SqlParserPos.ZERO, x.isIfNotExists(), ruleName, dbName, tableName, userName, FOR,
            keywords, templdateId, withList, query);
        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(DrdsDropCclRuleStatement x) {
        List<SqlIdentifier> ruleNames = x.getRuleNames().stream()
            .map((e) -> new SqlIdentifier(SQLUtils.normalizeNoTrim(e.getSimpleName()), SqlParserPos.ZERO)).collect(
                Collectors.toList());
        sqlNode = new SqlDropCclRule(SqlParserPos.ZERO, ruleNames, x.isIfExist());

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(DrdsShowCclRuleStatement x) {
        List<SqlIdentifier> ruleNames = null;
        if (x.getRuleNames() != null) {
            ruleNames = x.getRuleNames().stream()
                .map((e) -> new SqlIdentifier(SQLUtils.normalizeNoTrim(e.getSimpleName()), SqlParserPos.ZERO)).collect(
                    Collectors.toList());
        }
        sqlNode =
            new SqlShowCclRule(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.CCL_RULE), x.isAllRules(),
                ruleNames);

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(DrdsClearCclRulesStatement x) {
        sqlNode = new SqlClearCclRules(SqlParserPos.ZERO);

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(CreateFileStorageStatement x) {
        SqlIdentifier engineName = (SqlIdentifier) convertToSqlNode(x.getEngineName());

        List<SQLAssignItem> withValue = x.getWithValue();
        List<Pair<SqlIdentifier, SqlIdentifier>> with = new ArrayList<>(withValue.size());
        for (SQLAssignItem sqlAssignItem : withValue) {
            SqlIdentifier target =
                (SqlIdentifier) convertToSqlNode(sqlAssignItem.getTarget());
            SqlIdentifier value =
                (SqlIdentifier) convertToSqlNode(sqlAssignItem.getValue());
            with.add(Pair.of(target, value));
        }
        SqlCreateFileStorage sqlCreateFileStorage = new SqlCreateFileStorage(SqlParserPos.ZERO, engineName, with);
        sqlNode = sqlCreateFileStorage;
        return false;
    }

    @Override
    public boolean visit(DrdsCreateScheduleStatement x) {

        SqlIdentifier sqlIdentifier = (SqlIdentifier) convertToSqlNode(x.getName());
        ImmutableList<String> names = sqlIdentifier.names;
        String schemaName = getDefaultSchema();
        String tableName = sqlIdentifier.getLastName();
        if (CollectionUtils.size(names) > 1) {
            schemaName = names.get(0);
        }
        SqlIdentifier fullTableName = new SqlIdentifier(Lists.newArrayList(schemaName, tableName), SqlParserPos.ZERO);

        SqlCharStringLiteral paramsExpr = (SqlCharStringLiteral) convertToSqlNode(x.getParamsExpr());
        SqlCharStringLiteral cronExpr = (SqlCharStringLiteral) convertToSqlNode(x.getCronExpr());
        String timeZone = null;
        if (x.getTimeZone() != null) {
            timeZone = ((SqlCharStringLiteral) convertToSqlNode(x.getTimeZone())).toValue();
        }
        final String paramExprValue = paramsExpr == null ? null : paramsExpr.toValue();
        sqlNode = new SqlCreateSchedule(SqlParserPos.ZERO,
            false, schemaName, fullTableName, x.isForLocalPartition(), x.isForAutoSplitTableGroup(), paramExprValue,
            cronExpr.toValue(), timeZone);
        addPrivilegeVerifyItem(schemaName, tableName, PrivilegePoint.ALTER);
        addPrivilegeVerifyItem(schemaName, tableName, PrivilegePoint.DROP);
        return false;
    }

    @Override
    public boolean visit(DrdsDropScheduleStatement x) {
        sqlNode = new SqlDropSchedule(SqlParserPos.ZERO, x.isIfExist(), x.getScheduleId());
        return false;
    }

    @Override
    public boolean visit(DrdsPauseScheduleStatement x) {
        if (x.isForLocalPartition()) {
            sqlNode = new SqlPauseSchedule(SqlParserPos.ZERO, true);
        } else {
            sqlNode = new SqlPauseSchedule(SqlParserPos.ZERO, x.isIfExist(), x.getScheduleId());
        }
        return false;
    }

    @Override
    public boolean visit(DrdsContinueScheduleStatement x) {
        sqlNode = new SqlContinueSchedule(SqlParserPos.ZERO, x.getScheduleId());
        return false;
    }

    @Override
    public boolean visit(DrdsDropFileStorageStatement x) {
        SqlNode fileStorageName = convertToSqlNode(x.getName());
        SqlDropFileStorage
            sqlDropFileStorage =
            new SqlDropFileStorage(new SqlIdentifier(fileStorageName.toString(), SqlParserPos.ZERO));
        this.sqlNode = sqlDropFileStorage;
        return false;
    }

    @Override
    public boolean visit(DrdsAlterFileStorageStatement x) {
        SqlNode fileStorageName = convertToSqlNode(x.getName());
        SqlAlterFileStorage
            sqlAlterFileStorage;
        if (x.isAsOf()) {
            SqlNode timestamp = convertToSqlNode(x.getTimestamp());
            sqlAlterFileStorage = new SqlAlterFileStorage(SqlParserPos.ZERO, fileStorageName, timestamp, x.toString());
            sqlAlterFileStorage.setAsOf(true);
        } else if (x.isPurgeBefore()) {
            SqlNode timestamp = convertToSqlNode(x.getTimestamp());
            sqlAlterFileStorage = new SqlAlterFileStorage(SqlParserPos.ZERO, fileStorageName, timestamp, x.toString());
            sqlAlterFileStorage.setPurgeBefore(true);
        } else if (x.isBackup()) {
            sqlAlterFileStorage = new SqlAlterFileStorage(SqlParserPos.ZERO, fileStorageName, x.toString());
            sqlAlterFileStorage.setBackup(true);
        } else {
            throw new AssertionError("unknown alter fileStorage type");
        }
        this.sqlNode = sqlAlterFileStorage;
        return false;
    }

    @Override
    public boolean visit(DrdsFireScheduleStatement x) {
        sqlNode = new SqlFireSchedule(SqlParserPos.ZERO, x.getScheduleId());
        return false;
    }

    @Override
    public boolean visit(DrdsSplitPartition x) {

        SqlNode partitionName = convertToSqlNode(x.getSplitPartitionName());
        List<SqlPartition> partitions = null;
        SqlNode atValue = convertToSqlNode(x.getAtValue());

        if (GeneralUtil.isNotEmpty(x.getPartitions())) {
            partitions = new ArrayList<>();
            for (SQLObject sqlPartition : x.getPartitions()) {
                assert sqlPartition instanceof SQLPartition;
                partitions.add((SqlPartition) convertToSqlNode(sqlPartition));
            }
        }
        boolean isAlterTable = (x.getParent() != null && x.getParent() instanceof SQLAlterTableStatement);

        SqlAlterTableSplitPartition sqlAlterTableGroupSplitPartition =
            isAlterTable ? new SqlAlterTableSplitPartition(SqlParserPos.ZERO,
                partitionName,
                atValue,
                partitions) :
                new SqlAlterTableGroupSplitPartition(SqlParserPos.ZERO,
                    partitionName,
                    atValue,
                    partitions);
        this.sqlNode = sqlAlterTableGroupSplitPartition;
        return false;
    }

    @Override
    public boolean visit(DrdsMergePartition x) {

        SqlNode partitionName = convertToSqlNode(x.getTargetPartitionName());
        List<SqlNode> oldPartitions = new ArrayList<>();

        assert GeneralUtil.isNotEmpty(x.getPartitions());

        for (SQLName sqlPartition : x.getPartitions()) {
            oldPartitions.add(convertToSqlNode(sqlPartition));
        }
        boolean isAlterTable = (x.getParent() != null && x.getParent() instanceof SQLAlterTableStatement);

        SqlAlterTableMergePartition sqlAlterTableGroupMergePartition =
            isAlterTable ?
                new SqlAlterTableMergePartition(SqlParserPos.ZERO,
                    partitionName,
                    oldPartitions) :
                new SqlAlterTableGroupMergePartition(SqlParserPos.ZERO,
                    partitionName,
                    oldPartitions);
        this.sqlNode = sqlAlterTableGroupMergePartition;
        return false;
    }

    @Override
    public boolean visit(DrdsMovePartition x) {

        Map<SqlNode, List<SqlNode>> instPartitions = new HashMap<>();
        for (Map.Entry<SQLName, List<SQLName>> entry : x.getInstPartitions().entrySet()) {
            SqlNode storageId = convertToSqlNode(entry.getKey());
            List<SqlNode> oldPartitions = new ArrayList<>();
            assert GeneralUtil.isNotEmpty(entry.getValue());
            for (SQLName sqlPartition : entry.getValue()) {
                oldPartitions.add(convertToSqlNode(sqlPartition));
            }
            instPartitions.put(storageId, oldPartitions);
        }
        Map<String, Set<String>> targetPartitions = new HashMap<>();
        for (Map.Entry<SqlNode, List<SqlNode>> entry : instPartitions.entrySet()) {
            String instId =
                Util.last(((SqlIdentifier) (entry.getKey())).names);
            Set<String> partitionsToBeMoved = entry.getValue().stream()
                .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                    Collectors.toSet());
            targetPartitions.put(instId, partitionsToBeMoved);
        }

        boolean isAlterTable = (x.getParent() != null && x.getParent() instanceof SQLAlterTableStatement);

        SqlAlterTableMovePartition sqlAlterTableGroupMovePartition =
            isAlterTable ? new SqlAlterTableMovePartition(SqlParserPos.ZERO, instPartitions, targetPartitions) :
                new SqlAlterTableGroupMovePartition(SqlParserPos.ZERO, instPartitions, targetPartitions);

        this.sqlNode = sqlAlterTableGroupMovePartition;
        return false;
    }

    public boolean visit(DrdsCreateCclTriggerStatement x) {
        SqlIdentifier triggerName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getName().getSimpleName()), SqlParserPos.ZERO);
        SqlCreateCclTrigger sqlCreateCclTrigger = new SqlCreateCclTrigger(SqlParserPos.ZERO);
        sqlCreateCclTrigger.setTriggerName(triggerName);
        sqlCreateCclTrigger.setIfNotExits(x.isIfNotExists());
        SqlIdentifier schemaName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getSchema().getSimpleName()), SqlParserPos.ZERO);
        sqlCreateCclTrigger.setSchemaName(schemaName);
        List<SQLBinaryOpExpr> whens = x.getWhens();
        List<SqlNode> leftOperands = Lists.newArrayListWithCapacity(whens.size());
        List<SqlNode> operators = Lists.newArrayListWithCapacity(whens.size());
        List<SqlNode> rightOperands = Lists.newArrayListWithCapacity(whens.size());
        for (SQLBinaryOpExpr sqlBinaryOpExpr : whens) {
            SqlNode leftOperand = convertToSqlNode(sqlBinaryOpExpr.getLeft());
            leftOperands.add(leftOperand);
            SqlNode operator = new SqlIdentifier(sqlBinaryOpExpr.getOperator().name, SqlParserPos.ZERO);
            operators.add(operator);
            SqlNode rightOperand = convertToSqlNode(sqlBinaryOpExpr.getRight());
            rightOperands.add(rightOperand);
        }
        sqlCreateCclTrigger.setLeftOperands(leftOperands);
        sqlCreateCclTrigger.setOperators(operators);
        sqlCreateCclTrigger.setRightOperands(rightOperands);

        List<SQLAssignItem> limitSQLAssignItems = x.getLimitAssignItems();
        List<Pair<SqlNode, SqlNode>> limitList = new ArrayList<>(limitSQLAssignItems.size());
        for (SQLAssignItem sqlAssignItem : limitSQLAssignItems) {
            SqlIdentifier target =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLVariantRefExpr) sqlAssignItem.getTarget()).getName()),
                    SqlParserPos.ZERO);
            SqlNumericLiteral value =
                (SqlNumericLiteral) convertToSqlNode((SQLIntegerExpr) (sqlAssignItem.getValue()));
            limitList.add(Pair.of(target, value));
        }
        sqlCreateCclTrigger.setLimits(limitList);

        List<SQLAssignItem> withAssignItems = x.getWithAssignItems();
        List<Pair<SqlNode, SqlNode>> withList = new ArrayList<>(withAssignItems.size());
        for (SQLAssignItem sqlAssignItem : withAssignItems) {
            SqlIdentifier target =
                new SqlIdentifier(SQLUtils.normalizeNoTrim(((SQLVariantRefExpr) sqlAssignItem.getTarget()).getName()),
                    SqlParserPos.ZERO);
            SqlNumericLiteral value =
                (SqlNumericLiteral) convertToSqlNode((SQLIntegerExpr) (sqlAssignItem.getValue()));
            withList.add(Pair.of(target, value));
        }
        sqlCreateCclTrigger.setRuleWiths(withList);

        sqlNode = sqlCreateCclTrigger;

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(DrdsExtractHotKey x) {

        List<SqlNode> hotkeys =
            x.getHotKeys().stream().map((e) -> convertToSqlNode(e)).collect(
                Collectors.toList());
        boolean isAlterTable = (x.getParent() != null && x.getParent() instanceof SQLAlterTableStatement);

        SqlAlterTableExtractPartition sqlAlterTableGroupExtractPartition =
            isAlterTable ?
                new SqlAlterTableExtractPartition(SqlParserPos.ZERO, hotkeys, x.getHotKeyPartitionName()) :
                new SqlAlterTableGroupExtractPartition(SqlParserPos.ZERO, hotkeys, x.getHotKeyPartitionName());
        this.sqlNode = sqlAlterTableGroupExtractPartition;
        return false;
    }

    @Override
    public boolean visit(DrdsSplitHotKey x) {

        List<SqlNode> hotkeys =
            x.getHotKeys().stream().map((e) -> convertToSqlNode(e)).collect(
                Collectors.toList());
        SqlNode partitions = convertToSqlNode(x.getPartitions());

        boolean isAlterTable = (x.getParent() != null && x.getParent() instanceof SQLAlterTableStatement);
        SqlAlterTableSplitPartitionByHotValue sqlAlterTableGroupSplitPartitionByHotValue =
            isAlterTable ?
                new SqlAlterTableSplitPartitionByHotValue(SqlParserPos.ZERO, hotkeys, partitions,
                    x.getHotKeyPartitionName()) :
                new SqlAlterTableGroupSplitPartitionByHotValue(SqlParserPos.ZERO, hotkeys, partitions,
                    x.getHotKeyPartitionName());
        this.sqlNode = sqlAlterTableGroupSplitPartitionByHotValue;
        return false;
    }

    public boolean visit(DrdsDropCclTriggerStatement x) {
        SqlDropCclTrigger dropCclTrigger = new SqlDropCclTrigger(SqlParserPos.ZERO);
        dropCclTrigger.setIfExists(x.isIfExists());
        List<SQLName> cclTriggerNames = x.getNames();
        List<SqlIdentifier> triggerNames = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(cclTriggerNames)) {
            for (SQLName sqlName : cclTriggerNames) {
                SqlIdentifier triggerName =
                    new SqlIdentifier(SQLUtils.normalizeNoTrim(sqlName.getSimpleName()), SqlParserPos.ZERO);
                triggerNames.add(triggerName);
            }
        }
        dropCclTrigger.setNames(triggerNames);

        sqlNode = dropCclTrigger;

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(DrdsRenamePartition x) {

        List<com.alibaba.polardbx.druid.util.Pair<SQLName, SQLName>> changePartitionsPair = x.getChangePartitionsPair();
        List<com.alibaba.polardbx.common.utils.Pair<String, String>> partitionNamesPair = new ArrayList<>();
        for (com.alibaba.polardbx.druid.util.Pair<SQLName, SQLName> partitionPair : changePartitionsPair) {
            com.alibaba.polardbx.common.utils.Pair<String, String> pair =
                new com.alibaba.polardbx.common.utils.Pair<>(
                    SQLUtils.normalizeNoTrim(partitionPair.getKey().getSimpleName()),
                    SQLUtils.normalizeNoTrim(partitionPair.getValue().getSimpleName()));
            partitionNamesPair.add(pair);
        }
        boolean isAlterTable = (x.getParent() != null && x.getParent() instanceof SQLAlterTableStatement);
        SqlAlterTableRenamePartition sqlAlterTableRenamePartition =
            isAlterTable ? new SqlAlterTableRenamePartition(SqlParserPos.ZERO, partitionNamesPair) :
                new SqlAlterTableGroupRenamePartition(SqlParserPos.ZERO, partitionNamesPair);
        this.sqlNode = sqlAlterTableRenamePartition;
        return false;
    }

    @Override
    public void endVisit(DrdsAlterTableGroupSetLocality x) {

    }

    @Override
    public boolean visit(DrdsAlterTableGroupSetLocality x) {
        SqlNode targetLocality = convertToSqlNode(x.getTargetLocality());

        SqlAlterTableGroupSetLocality sqlAlterTableGroupSetLocality = new SqlAlterTableGroupSetLocality(
            SqlParserPos.ZERO, targetLocality);
        sqlAlterTableGroupSetLocality.setLogical(x.getLogicalDDL());
        this.sqlNode = sqlAlterTableGroupSetLocality;
        return false;
    }

    @Override
    public void endVisit(DrdsAlterTableGroupSetPartitionsLocality x) {

    }

    @Override
    public boolean visit(DrdsAlterTableGroupSetPartitionsLocality x) {
        SqlNode targetLocality = convertToSqlNode(x.getTargetLocality());
        SqlNode partition = convertToSqlNode(x.getPartition());

        SqlAlterTableGroupSetPartitionsLocality sqlAlterTableGroupSetPartitionsLocality =
            new SqlAlterTableGroupSetPartitionsLocality(
                SqlParserPos.ZERO, partition, targetLocality);
        sqlAlterTableGroupSetPartitionsLocality.setLogical(x.getLogicalDDL());
        this.sqlNode = sqlAlterTableGroupSetPartitionsLocality;
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableGroupStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        SqlNode tableGroupNode = convertToSqlNode(x.getName());
        //todo
        x.getOptions();
        List<SqlAlterSpecification> alters = null;
        if (x.getItem() != null) {
            alters = new ArrayList<>();
            alters.add((SqlAlterSpecification) convertToSqlNode(x.getItem()));
        }
        SqlAlterTableGroup sqlAlterTableGroup =
            new SqlAlterTableGroup(SqlParserPos.ZERO, tableGroupNode, alters, x.toString());
        sqlAlterTableGroup.setHints(hints);
        this.sqlNode = sqlAlterTableGroup;
        return false;
    }

    public boolean visit(DrdsClearCclTriggersStatement x) {
        SqlClearCclTriggers clearCclTriggers = new SqlClearCclTriggers(SqlParserPos.ZERO);

        sqlNode = clearCclTriggers;

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableModifyPartitionValues x) {
        SqlNode partition = convertToSqlNode(x.getSqlPartition());
        sqlNode = new SqlAlterTableModifyPartitionValues(SqlParserPos.ZERO, (SqlPartition) partition, x.isAdd());
        return false;
    }

    @Override
    public boolean visit(SQLCreateTableGroupStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        String tableGroupName = SQLUtils.normalizeNoTrim(x.getTableGroupName());
        String schemaName = x.getSchemaName();
        boolean isIfNotExists = x.isIfNotExists();
        String locality = x.getLocality();

        SqlCreateTableGroup sqlCreateTableGroup =
            new SqlCreateTableGroup(SqlParserPos.ZERO, isIfNotExists, schemaName, tableGroupName, locality);
        sqlCreateTableGroup.setHints(hints);
        this.sqlNode = sqlCreateTableGroup;
        return false;
    }

    @Override
    public boolean visit(SQLDropTableGroupStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        String tableGroupName = SQLUtils.normalizeNoTrim(x.getTableGroupName());
        boolean ifExists = x.isIfExists();

        SqlDropTableGroup sqlDropTableGroup =
            new SqlDropTableGroup(SqlParserPos.ZERO, ifExists, null, tableGroupName);
        sqlDropTableGroup.setHints(hints);
        this.sqlNode = sqlDropTableGroup;
        return false;
    }

    @Override
    public boolean visit(DrdsShowCclTriggerStatement x) {
        List<SqlIdentifier> triggerNames = null;
        if (x.getTriggerNames() != null) {
            triggerNames = x.getTriggerNames().stream()
                .map((e) -> new SqlIdentifier(SQLUtils.normalizeNoTrim(e.getSimpleName()), SqlParserPos.ZERO)).collect(
                    Collectors.toList());
        }
        sqlNode =
            new SqlShowCclTrigger(SqlParserPos.ZERO, ImmutableList.of(SqlSpecialIdentifier.CCL_RULE), x.isAll(),
                triggerNames);

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(DrdsShowTableGroup x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        List<String> names = new ArrayList<String>(2);
        names.add("");

        List<String> tableName = new ArrayList<>();
        tableName.add(InformationSchema.NAME);
        if (!x.isFull()) {
            tableName.add(GmsSystemTables.TABLE_GROUP);
        } else {
            tableName.add("full_" + GmsSystemTables.TABLE_GROUP);
        }

        SqlNode fromClause = new SqlIdentifier(tableName, null, SqlParserPos.ZERO, null);
        // where
        SqlNode whereCalcite = FastSqlConstructUtils.constructWhere(x.getWhere(), context, ec);

        // orderBy
        SqlNodeList orderBySqlNode = FastSqlConstructUtils.constructOrderBy(x.getOrderBy(), context, ec);

        // limit
        SqlNodeList limitNodes = FastSqlConstructUtils.constructLimit(x.getLimit(), context, ec);

        SqlNode offset = null;
        SqlNode limit = null;
        if (limitNodes != null) {
            offset = limitNodes.get(0);
            limit = limitNodes.get(1);
        }

        this.sqlNode = new TDDLSqlSelect(SqlParserPos.ZERO,
            null,
            new SqlNodeList(Lists.newArrayList(new SqlIdentifier(names, SqlParserPos.ZERO)),
                SqlParserPos.ZERO),
            fromClause,
            whereCalcite,
            null,
            null,
            null,
            orderBySqlNode,
            offset,
            limit,
            hints,
            null);
        return false;
    }

    public boolean visit(DrdsSlowSqlCclStatement x) {
        SqlIdentifier operationName =
            new SqlIdentifier(SQLUtils.normalizeNoTrim(x.getOperation().getSimpleName()), SqlParserPos.ZERO);
        List<SQLExpr> exprs = x.getSqlExprs();
        List<SqlNode> sqlNodes = Lists.newArrayList();
        if (exprs != null) {
            sqlNodes = exprs.stream().map((e) -> convertToSqlNode(e)).collect(Collectors.toList());
        }
        sqlNode = new SqlSlowSqlCcl(operationName, sqlNodes, SqlParserPos.ZERO);

        addPrivilegeVerifyItem("*", "*", PrivilegePoint.CREATE);
        return false;
    }

    @Override
    public boolean visit(MySqlChangeMasterStatement x) {
        List<Pair<SqlNode, SqlNode>> options = new LinkedList<>();
        for (SQLAssignItem option : x.getOptions()) {
            SqlNode key = convertToSqlNode(option.getTarget());
            SqlNode value = convertToSqlNode(option.getValue());
            options.add(Pair.of(key, value));
        }
        final SQLCharExpr channel = x.getChannel();
        if (channel == null) {
            sqlNode = new SqlChangeMaster(SqlParserPos.ZERO, options);
        } else {
            SqlNode ch = convertToSqlNode(channel);
            sqlNode = new SqlChangeMaster(SqlParserPos.ZERO, options, ch);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlChangeReplicationFilterStatement x) {
        List<Pair<SqlNode, SqlNode>> options = new LinkedList<>();
        for (SQLAssignItem option : x.getFilters()) {
            SqlNode key = convertToSqlNode(option.getTarget());
            SqlNode value = convertToSqlNode(option.getValue());
            options.add(Pair.of(key, value));
        }
        final SQLCharExpr channel = x.getChannel();
        if (channel == null) {
            sqlNode = new SqlChangeReplicationFilter(SqlParserPos.ZERO, options);
        } else {
            SqlNode ch = convertToSqlNode(channel);
            sqlNode = new SqlChangeReplicationFilter(SqlParserPos.ZERO, options, ch);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlStartSlaveStatement x) {
        List<Pair<SqlNode, SqlNode>> options = new LinkedList<>();
        final SQLCharExpr channel = x.getChannel();
        if (channel == null) {
            sqlNode = new SqlStartSlave(SqlParserPos.ZERO, options);
        } else {
            SqlNode ch = convertToSqlNode(channel);
            sqlNode = new SqlStartSlave(SqlParserPos.ZERO, options, ch);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlStopSlaveStatement x) {
        List<Pair<SqlNode, SqlNode>> options = new LinkedList<>();
        final SQLCharExpr channel = x.getChannel();
        if (channel == null) {
            sqlNode = new SqlStopSlave(SqlParserPos.ZERO, options);
        } else {
            SqlNode ch = convertToSqlNode(channel);
            sqlNode = new SqlStopSlave(SqlParserPos.ZERO, options, ch);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlResetSlaveStatement x) {
        List<Pair<SqlNode, SqlNode>> options = new LinkedList<>();
        final SQLCharExpr channel = x.getChannel();
        if (channel == null) {
            sqlNode = new SqlResetSlave(SqlParserPos.ZERO, options, x.isAll());
        } else {
            SqlNode ch = convertToSqlNode(channel);
            sqlNode = new SqlResetSlave(SqlParserPos.ZERO, options, ch, x.isAll());
        }
        return false;
    }

    @Override
    public boolean visit(MySqlShowSlaveStatusStatement x) {
        List<Pair<SqlNode, SqlNode>> options = new LinkedList<>();
        final SQLCharExpr channel = x.getChannel();
        if (channel == null) {
            sqlNode = new SqlShowSlaveStatus(SqlParserPos.ZERO, options);
        } else {
            SqlNode ch = convertToSqlNode(channel);
            sqlNode = new SqlShowSlaveStatus(SqlParserPos.ZERO, options, ch);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlSetDefaultRoleStatement x) {
        List<SqlUserName> users = x.getUsers()
            .stream()
            .map(SqlUserName::from)
            .collect(Collectors.toList());

        List<SqlUserName> roles = x.getRoles()
            .stream()
            .map(SqlUserName::from)
            .collect(Collectors.toList());

        sqlNode = new SqlSetDefaultRole(SqlParserPos.ZERO, x.getDefaultRoleSpec(), roles, users);
        return false;
    }

    @Override
    public boolean visit(MySqlSetRoleStatement x) {
        List<SqlUserName> roles = x.getRoles()
            .stream()
            .map(SqlUserName::from)
            .collect(Collectors.toList());

        sqlNode = new SqlSetRole(SqlParserPos.ZERO, x.getRoleSpec(), roles);
        return false;
    }

    @Override
    public boolean visit(SQLRebalanceStatement x) {
        SqlRebalance node = new SqlRebalance(SqlParserPos.ZERO);

        node.setLogicalDdl(x.isLogicalDdl());
        switch (x.getRebalanceTarget()) {
        case TABLE: {
            SQLExprTableSource tableSource = x.getTableSource();
            SqlNode targetTable = convertToSqlNode(tableSource.getExpr());
            node.setRebalanceTable(targetTable);
            break;
        }
        case TABLEGROUP: {
            SQLExprTableSource tableSource = x.getTableSource();
            SqlNode targetTableGroup = convertToSqlNode(tableSource.getExpr());
            node.setRebalanceTableGroup(targetTableGroup);
            break;
        }
        case DATABASE: {
            node.setRebalanceDatabase();
            break;
        }
        case CLUSTER: {
            node.setRebalanceCluster();
            break;
        }
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER, x.toString());
        }

        for (SQLAssignItem item : x.getOptions()) {
            SqlNode target = convertToSqlNode(item.getTarget());
            SqlNode value = convertToSqlNode(item.getValue());
            String targetName = RelUtils.stringValue(target);

            node.addOption(targetName, value);
        }
        this.sqlNode = node;
        return false;
    }

    @Override
    public boolean visit(DrdsUnArchiveStatement x) {
        SqlUnArchive node = new SqlUnArchive(SqlParserPos.ZERO);
        switch (x.getTarget()) {
        case TABLE: {
            SqlNode targetTable = convertToSqlNode(x.getTableSource().getExpr());
            node.setTable(targetTable);
            break;
        }
        case TABLE_GROUP: {
            SqlNode tableGroup = convertToSqlNode(x.getTableGroup());
            node.setTableGroup(tableGroup);
            break;
        }
        case DATABASE: {
            SqlNode database = convertToSqlNode(x.getDatabase());
            node.setDatabase(database);
            break;
        }
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER, "only UNARCHIVE DATABASE/TABLEGROUP/TABLE supported");
        }

        this.sqlNode = node;
        return false;
    }

    @Override
    public boolean visit(DrdsPushDownUdfStatement x) {
        SqlPushDownUdf node = new SqlPushDownUdf(SqlParserPos.ZERO);
        this.sqlNode = node;
        return false;
    }

    @Override
    public boolean visit(DrdsRefreshTopology x) {

        SqlRefreshTopology sqlRefreshTopology = new SqlRefreshTopology(SqlParserPos.ZERO);

        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        sqlRefreshTopology.setHints(hints);
        this.sqlNode = sqlRefreshTopology;
        return false;
    }

    @Override
    public boolean visit(SQLCreateFunctionStatement x) {
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_UDF)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_NOT_SUPPORT, "udf is not support yet!");
        }
        if (x.getBlock() instanceof SQLBlockStatement) {
            recursiveSetAfterSemi(x.getBlock());
        }

        sqlNode = new SqlCreateFunction(x.toString(),
            SqlFunction.replaceUdfName(SQLUtils.normalize(x.getName().getSimpleName())),
            (x.getSqlDataAccess() == SqlDataAccess.NO_SQL));
        return false;
    }

    @Override
    public boolean visit(SQLDropFunctionStatement x) {
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_UDF)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_NOT_SUPPORT, "udf is not support yet!");
        }

        sqlNode = new SqlDropFunction(x.toString(),
            SqlFunction.replaceUdfName(SQLUtils.normalize(x.getName().getSimpleName())), x.isIfExists());
        return false;
    }

    @Override
    public boolean visit(SQLAlterFunctionStatement x) {
        sqlNode = new SqlAlterFunction(x.toString(),
            SqlFunction.replaceUdfName(SQLUtils.normalize(x.getName().getSimpleName())));
        return false;
    }

    @Override
    public boolean visit(SQLCreateProcedureStatement x) {
        if (x.getBlock() instanceof SQLBlockStatement) {
            recursiveSetAfterSemi(x.getBlock());
        }

        sqlNode = new SqlCreateProcedure(x.toString(), SQLUtils.normalizeSqlName(x.getName()));
        return false;
    }

    @Override
    public boolean visit(SQLDropProcedureStatement x) {
        sqlNode = new SqlDropProcedure(x.toString(), SQLUtils.normalizeSqlName(x.getName()), x.isIfExists());
        return false;
    }

    @Override
    public boolean visit(SQLAlterProcedureStatement x) {
        sqlNode = new SqlAlterProcedure(x.toString(), SQLUtils.normalizeSqlName(x.getName()));
        return false;
    }

    @Override
    public boolean visit(SQLCreateTriggerStatement x) {
        try {
            Field name = SQLCreateTriggerStatement.class.getDeclaredField("name");
            name.setAccessible(true);
            name.set(x, new SQLIdentifierExpr(x.getName().getSimpleName()));

            Field on = SQLCreateTriggerStatement.class.getDeclaredField("on");
            on.setAccessible(true);
            on.set(x, new SQLExprTableSource(x.getOn().getTableName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        sqlNode = new SqlCreateTrigger(x);
        boolean value = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_STORAGE_TRIGGER);
        if (value) {
            // valid
        } else {
            throw new TddlNestableRuntimeException("do not support create trigger");
        }
        return false;
    }

    @Override
    public boolean visit(SQLDropTriggerStatement x) {
        try {
            Field name = SQLDropTriggerStatement.class.getDeclaredField("name");
            name.setAccessible(true);
            name.set(x, new SQLIdentifierExpr(x.getName().getSimpleName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        sqlNode = new SqlDropTrigger(x);
        return false;
    }

    // TODO check semi
    private void recursiveSetAfterSemi(SQLStatement stmt) {
        if (stmt instanceof SQLBlockStatement) {
            for (SQLStatement s : ((SQLBlockStatement) stmt).getStatementList()) {
                recursiveSetAfterSemi(s);
            }
        } else if (stmt instanceof SQLIfStatement) {
            for (SQLStatement s : ((SQLIfStatement) stmt).getStatements()) {
                recursiveSetAfterSemi(s);
            }
            for (SQLIfStatement.ElseIf elseIf : ((SQLIfStatement) stmt).getElseIfList()) {
                for (SQLStatement s : elseIf.getStatements()) {
                    recursiveSetAfterSemi(s);
                }
            }
            if (((SQLIfStatement) stmt).getElseItem() != null) {
                for (SQLStatement s : ((SQLIfStatement) stmt).getElseItem().getStatements()) {
                    recursiveSetAfterSemi(s);
                }
            }
        } else if (stmt instanceof SQLLoopStatement) {
            for (SQLStatement s : ((SQLLoopStatement) stmt).getStatements()) {
                recursiveSetAfterSemi(s);
            }
        } else if (stmt instanceof SQLWhileStatement) {
            for (SQLStatement s : ((SQLWhileStatement) stmt).getStatements()) {
                recursiveSetAfterSemi(s);
            }
        } else if (stmt instanceof MySqlRepeatStatement) {
            for (SQLStatement s : ((MySqlRepeatStatement) stmt).getStatements()) {
                recursiveSetAfterSemi(s);
            }
        } else if (stmt instanceof MySqlCaseStatement) {
            for (MySqlCaseStatement.MySqlWhenStatement whenStatement : ((MySqlCaseStatement) stmt).getWhenList()) {
                for (SQLStatement s : whenStatement.getStatements()) {
                    recursiveSetAfterSemi(s);
                }
            }
            if (((MySqlCaseStatement) stmt).getElseItem() != null) {
                for (SQLStatement s : ((MySqlCaseStatement) stmt).getElseItem().getStatements()) {
                    recursiveSetAfterSemi(s);
                }
            }
        }
        stmt.setAfterSemi(true);
    }

    @Override
    public boolean visit(SQLCreateJoinGroupStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        String joinGroupName = SQLUtils.normalizeNoTrim(x.getJoinGroupName());
        String schemaName = x.getSchemaName();
        boolean isIfNotExists = x.isIfNotExists();
        String locality = x.getLocality();

        SqlCreateJoinGroup sqlCreateJoinGroup =
            new SqlCreateJoinGroup(SqlParserPos.ZERO, isIfNotExists, schemaName, joinGroupName, locality);
        sqlCreateJoinGroup.setHints(hints);
        this.sqlNode = sqlCreateJoinGroup;
        return false;
    }

    @Override
    public boolean visit(SQLDropJoinGroupStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        String joinGroupName = SQLUtils.normalizeNoTrim(x.getJoinGroupName());
        boolean ifExists = x.isIfExists();

        SqlDropJoinGroup sqlDropJoinGroup =
            new SqlDropJoinGroup(SqlParserPos.ZERO, ifExists, null, joinGroupName);
        sqlDropJoinGroup.setHints(hints);
        this.sqlNode = sqlDropJoinGroup;
        return false;
    }

    @Override
    public boolean visit(SQLAlterJoinGroupStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        String joinGroupName = SQLUtils.normalizeNoTrim(x.getJoinGroupName());

        List<String> tableNames = new ArrayList<>();
        for (SQLName tableName : x.getTableNames()) {
            final String normalize = SQLUtils.normalizeNoTrim(tableName.getSimpleName());
            tableNames.add(normalize);
        }
        SqlAlterJoinGroup sqlAlterJoinGroup =
            new SqlAlterJoinGroup(SqlParserPos.ZERO, joinGroupName, tableNames, x.isAdd(), x.toString());
        sqlAlterJoinGroup.setHints(hints);
        this.sqlNode = sqlAlterJoinGroup;
        return false;
    }

    @Override
    public boolean visit(SQLMergeTableGroupStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        SqlNodeList hints = new SqlNodeList(SqlParserPos.ZERO);
        if (headHints != null) {
            hints = FastSqlConstructUtils.convertHints(headHints, context, ec);
        }
        String targetTableGroup = SQLUtils.normalizeNoTrim(x.getTargetTableGroup().getSimpleName());

        List<String> sourceTableGroups = new ArrayList<>();
        for (SQLName tableGroupName : x.getSourceTableGroup()) {
            final String normalize = SQLUtils.normalizeNoTrim(tableGroupName.getSimpleName());
            sourceTableGroups.add(normalize);
        }
        SqlMergeTableGroup sqlMergeTableGroup =
            new SqlMergeTableGroup(SqlParserPos.ZERO, targetTableGroup, sourceTableGroups, x.toString(), x.isForce());
        sqlMergeTableGroup.setHints(hints);
        this.sqlNode = sqlMergeTableGroup;
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableGroupAddTable x) {
        List<SqlNode> tables = new ArrayList<>();
        for (SQLName sqlName : x.getTables()) {
            tables.add(convertToSqlNode(sqlName));
        }
        SqlAlterTableGroupAddTable sqlAlterTableGroupAddTable =
            new SqlAlterTableGroupAddTable(SqlParserPos.ZERO, tables, x.isForce());
        this.sqlNode = sqlAlterTableGroupAddTable;
        return false;
    }

    private static boolean hasSubquery(SqlNode sqlNode) {
        if (sqlNode == null) {
            return false;
        }
        if (sqlNode instanceof SqlNodeList) {
            for (SqlNode s : ((SqlNodeList) sqlNode).getList()) {
                if (hasSubquery(s)) {
                    return true;
                }
            }
        } else if (sqlNode instanceof SqlSelect) {
            return true;
        } else if (sqlNode instanceof SqlCall) {
            for (SqlNode s : ((SqlCall) sqlNode).getOperandList()) {
                if (hasSubquery(s)) {
                    return true;
                }
            }
        }
        return false;
    }

    private String getDefaultSchema() {
        return DefaultSchema.getSchemaName();
    }
}
