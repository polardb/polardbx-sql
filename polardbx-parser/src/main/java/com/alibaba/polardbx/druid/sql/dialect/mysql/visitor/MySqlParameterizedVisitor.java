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
package com.alibaba.polardbx.druid.sql.dialect.mysql.visitor;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableAllocateLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableExpireLocalPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsInspectIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement.MySqlWhenStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCursorDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareConditionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareHandlerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlIterateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlLeaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlSelectIntoStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCancelRebalanceJob;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectRuleVersionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectSeqRangeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsMoveDataBase;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseRebalanceJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseScheduleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPurgeTransStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPushDownUdfStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRecoverDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRefreshLocalRulesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRemoveDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRollbackDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsTerminateRebalanceJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowChangeSet;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowCreateTableGroup;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowStorage;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTableGroup;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTableReplicate;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTransStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTransStatsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsSkipRebalanceSubjob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsSlowSqlCclStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsUnArchiveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySql8ShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterDatabaseKillJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterDatabaseSetOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterEventStatement;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCheckTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChecksumTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlClearPartitionsHeatmapCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlClearPlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateAddLogFileGroupStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateEventStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateServerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableSpaceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement.TableSpaceOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDisabledPlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlEventSchedule;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExecuteForAdsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainPlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExtPartition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlFlashbackStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlGrantRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHelpStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlManageInstanceGroupStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlMigrateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlOptimizeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRaftLeaderTransferStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRaftMemberChangeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameSequenceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlResetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRevokeRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetDefaultRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryStreamsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBroadcastsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowClusterNameStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowConfigStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowContributorsStatement;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowHMSMetaStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowHelpStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowJobStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowMigrateTaskStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowNodeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPartitionsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPhysicalProcesslistStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPlanCacheStatusStatement;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByList;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByValue;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdatePlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlAlterFullTextStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlCreateFullTextAnalyzerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlCreateFullTextCharFilterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlCreateFullTextDictionaryStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlCreateFullTextTokenFilterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlCreateFullTextTokenizerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlDropFullTextStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowCreateFullTextStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowDbLockStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowFullTextStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowHtcStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowRouteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlShowStcStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.SQLShowPartitionsHeatmapStatement;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTParameterizedVisitor;

import java.util.List;

public class MySqlParameterizedVisitor extends SQLASTParameterizedVisitor implements MySqlASTVisitor {

    public MySqlParameterizedVisitor() {
        super(DbType.mysql);
    }

    public MySqlParameterizedVisitor(List<Object> outParameters) {
        super(DbType.mysql, outParameters);
    }

    @Override
    public boolean visit(MySqlTableIndex x) {
        return true;
    }

    @Override
    public void endVisit(MySqlTableIndex x) {

    }

    @Override
    public boolean visit(MySqlKey x) {
        return true;
    }

    @Override
    public void endVisit(MySqlKey x) {

    }

    @Override
    public boolean visit(MySqlPrimaryKey x) {
        return true;
    }

    @Override
    public void endVisit(MySqlPrimaryKey x) {

    }

    @Override
    public boolean visit(MySqlUnique x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUnique x) {

    }

    @Override
    public boolean visit(MysqlForeignKey x) {
        return true;
    }

    @Override
    public void endVisit(MysqlForeignKey x) {

    }

    @Override
    public void endVisit(MySqlPrepareStatement x) {

    }

    @Override
    public boolean visit(MySqlPrepareStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlExecuteStatement x) {

    }

    @Override
    public boolean visit(MysqlDeallocatePrepareStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlDeallocatePrepareStatement x) {

    }

    @Override
    public boolean visit(MySqlExecuteStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDeleteStatement x) {

    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlInsertStatement x) {

    }

    @Override
    public boolean visit(MySqlInsertStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlLoadDataInFileStatement x) {

    }

    @Override
    public boolean visit(MySqlLoadDataInFileStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlLoadXmlStatement x) {

    }

    @Override
    public boolean visit(MySqlLoadXmlStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowWarningsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowWarningsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowAuthorsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowAuthorsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlShowHtcStatement x) {

    }

    @Override
    public boolean visit(MysqlShowHtcStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlShowStcStatement x) {

    }

    @Override
    public boolean visit(MysqlShowStcStatement x) {
        return true;
    }

    @Override
    public void endVisit(CobarShowStatus x) {

    }

    @Override
    public boolean visit(CobarShowStatus x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowDDLJobs x) {

    }

    @Override
    public boolean visit(DrdsShowDDLJobs x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowDDLResults x) {

    }

    @Override
    public boolean visit(DrdsShowDDLResults x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowRebalanceBackFill x) {

    }

    @Override
    public boolean visit(DrdsShowRebalanceBackFill x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowScheduleResultStatement x) {

    }

    @Override
    public boolean visit(DrdsShowScheduleResultStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlShowRouteStatement x) {

    }

    @Override
    public boolean visit(MysqlShowRouteStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsCancelDDLJob x) {

    }

    @Override
    public boolean visit(DrdsCancelDDLJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsUnArchiveStatement x) {

    }

    @Override
    public boolean visit(DrdsUnArchiveStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsPushDownUdfStatement x) {

    }

    @Override
    public boolean visit(DrdsPushDownUdfStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsRecoverDDLJob x) {

    }

    @Override
    public boolean visit(DrdsRecoverDDLJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsContinueDDLJob x) {

    }

    @Override
    public boolean visit(DrdsContinueDDLJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsPauseDDLJob x) {

    }

    @Override
    public boolean visit(DrdsPauseDDLJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsPauseRebalanceJob x) {

    }

    @Override
    public boolean visit(DrdsPauseRebalanceJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsRollbackDDLJob x) {

    }

    @Override
    public boolean visit(DrdsRollbackDDLJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsTerminateRebalanceJob x) {

    }

    @Override
    public boolean visit(DrdsTerminateRebalanceJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsSkipRebalanceSubjob x) {

    }

    @Override
    public boolean visit(DrdsSkipRebalanceSubjob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsCancelRebalanceJob x) {

    }

    @Override
    public boolean visit(DrdsCancelRebalanceJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsRemoveDDLJob x) {

    }

    @Override
    public boolean visit(DrdsRemoveDDLJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsInspectDDLJobCache x) {

    }

    @Override
    public boolean visit(DrdsInspectDDLJobCache x) {
        return true;
    }

    @Override
    public void endVisit(DrdsClearDDLJobCache x) {

    }

    @Override
    public boolean visit(DrdsClearDDLJobCache x) {
        return true;
    }

    @Override
    public void endVisit(DrdsChangeDDLJob x) {

    }

    @Override
    public boolean visit(DrdsChangeDDLJob x) {
        return true;
    }

    @Override
    public void endVisit(DrdsBaselineStatement x) {

    }

    @Override
    public boolean visit(DrdsBaselineStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowGlobalIndex x) {

    }

    @Override
    public boolean visit(DrdsShowGlobalIndex x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowGlobalDeadlocks x) {

    }

    @Override
    public boolean visit(DrdsShowGlobalDeadlocks x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowLocalDeadlocks x) {

    }

    @Override
    public boolean visit(DrdsShowLocalDeadlocks x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowMetadataLock x) {

    }

    @Override
    public boolean visit(SQLShowPartitionsHeatmapStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLShowPartitionsHeatmapStatement x) {

    }

    @Override
    public boolean visit(DrdsShowMetadataLock x) {
        return true;
    }

    @Override
    public void endVisit(DrdsCheckGlobalIndex x) {

    }

    @Override
    public boolean visit(DrdsCheckGlobalIndex x) {
        return true;
    }

    @Override
    public void endVisit(DrdsCreateCclRuleStatement x) {

    }

    @Override
    public boolean visit(DrdsCreateCclRuleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsCreateScheduleStatement x) {

    }

    @Override
    public boolean visit(DrdsCreateScheduleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsDropScheduleStatement x) {

    }

    @Override
    public boolean visit(DrdsDropScheduleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsPauseScheduleStatement x) {

    }

    @Override
    public boolean visit(DrdsPauseScheduleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsContinueScheduleStatement x) {

    }

    @Override
    public boolean visit(DrdsContinueScheduleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsFireScheduleStatement x) {

    }

    @Override
    public boolean visit(DrdsFireScheduleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsDropCclRuleStatement x) {

    }

    @Override
    public boolean visit(CreateFileStorageStatement x) {
        return false;
    }

    @Override
    public void endVisit(CreateFileStorageStatement x) {

    }

    @Override
    public boolean visit(DrdsDropCclRuleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsShowCclRuleStatement x) {

    }

    @Override
    public boolean visit(DrdsShowCclRuleStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsClearCclRulesStatement x) {

    }

    @Override
    public boolean visit(DrdsClearCclRulesStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsCreateCclTriggerStatement x) {

    }

    @Override
    public boolean visit(DrdsCreateCclTriggerStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsDropCclTriggerStatement x) {

    }

    @Override
    public boolean visit(DrdsDropCclTriggerStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsClearCclTriggersStatement x) {

    }

    @Override
    public boolean visit(DrdsClearCclTriggersStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsShowCclTriggerStatement x) {

    }

    @Override
    public boolean visit(DrdsShowCclTriggerStatement x) {
        return false;
    }

    @Override
    public void endVisit(DrdsSlowSqlCclStatement x) {

    }

    @Override
    public boolean visit(DrdsSlowSqlCclStatement x) {
        return false;
    }

    @Override
    public void endVisit(MySqlBinlogStatement x) {

    }

    @Override
    public boolean visit(MySqlBinlogStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlResetStatement x) {

    }

    @Override
    public boolean visit(MySqlResetStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateUserStatement x) {

    }

    @Override
    public boolean visit(MySqlCreateUserStatement x) {
        return true;
    }

    @Override
    public void endVisit(UserSpecification x) {

    }

    @Override
    public boolean visit(UserSpecification x) {
        return true;
    }

    @Override
    public boolean visit(MySqlCreateRoleStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateRoleStatement x) {

    }

    @Override
    public boolean visit(MySqlCreateRoleStatement.RoleSpec x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateRoleStatement.RoleSpec x) {

    }

    @Override
    public boolean visit(MySqlDropRoleStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDropRoleStatement x) {

    }

    @Override
    public boolean visit(MySqlGrantRoleStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlGrantRoleStatement x) {

    }

    @Override
    public boolean visit(MySqlRevokeRoleStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRevokeRoleStatement x) {

    }

    @Override
    public void endVisit(MySqlPartitionByKey x) {

    }

    @Override
    public boolean visit(MySqlPartitionByKey x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUpdatePlanCacheStatement x) {

    }

    @Override
    public boolean visit(MySqlUpdatePlanCacheStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPlanCacheStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPlanCacheStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlClearPlanCacheStatement x) {

    }

    @Override
    public boolean visit(MySqlClearPlanCacheStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlClearPartitionsHeatmapCacheStatement x) {

    }

    @Override
    public boolean visit(MySqlClearPartitionsHeatmapCacheStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDisabledPlanCacheStatement x) {

    }

    @Override
    public boolean visit(MySqlDisabledPlanCacheStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlExplainPlanCacheStatement x) {

    }

    @Override
    public boolean visit(MySqlExplainPlanCacheStatement x) {
        return true;
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {

    }

    @Override
    public boolean visit(MySqlOutFileExpr x) {
        return true;
    }

    @Override
    public void endVisit(MySqlOutFileExpr x) {

    }

    @Override
    public boolean visit(MySqlExplainStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlExplainStatement x) {

    }

    @Override
    public boolean visit(MySqlUpdateStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUpdateStatement x) {

    }

    @Override
    public boolean visit(MySqlSetTransactionStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetTransactionStatement x) {

    }

    @Override
    public boolean visit(MySqlShowHMSMetaStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowHMSMetaStatement x) {

    }

    @Override
    public boolean visit(MySqlShowBinaryLogsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowBinaryLogsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowMasterLogsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowMasterLogsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCharacterSetStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCharacterSetStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCollationStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCollationStatement x) {

    }

    @Override
    public boolean visit(MySqlShowBinLogEventsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowBinLogEventsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowContributorsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowContributorsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateDatabaseStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateDatabaseStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateEventStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateEventStatement x) {

    }

    @Override
    public void endVisit(SQLAlterDatabaseStatement x) {
    }

    @Override
    public boolean visit(MySqlShowCreateFunctionStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateFunctionStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateProcedureStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateProcedureStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateTriggerStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateTriggerStatement x) {

    }

    @Override
    public boolean visit(MySqlShowEngineStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowEngineStatement x) {

    }

    @Override
    public boolean visit(MySqlShowEnginesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowEnginesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowErrorsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowErrorsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowEventsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowEventsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowFunctionCodeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowFunctionCodeStatement x) {

    }

    @Override
    public boolean visit(MySqlShowFunctionStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowFunctionStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowGrantsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowGrantsStatement x) {

    }

    @Override
    public boolean visit(MySql8ShowGrantsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySql8ShowGrantsStatement x) {

    }

    @Override
    public boolean visit(MySqlUserName x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUserName x) {

    }

    @Override
    public boolean visit(MySqlAlterDatabaseSetOption x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterDatabaseSetOption x) {

    }

    @Override
    public boolean visit(MySqlAlterDatabaseKillJob x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterDatabaseKillJob x) {

    }

    @Override
    public boolean visit(MySqlShowMasterStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowMasterStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowOpenTablesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowOpenTablesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPluginsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPluginsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPartitionsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPartitionsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPrivilegesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPrivilegesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProcedureCodeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcedureCodeStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProcedureStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcedureStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProcessListStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcessListStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProfileStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProfileStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProfilesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProfilesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowRelayLogEventsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowRelayLogEventsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowSlaveHostsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowSlaveHostsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowSequencesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowSequencesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowSlaveStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowSlaveStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowSlowStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowSlowStatement x) {

    }

    @Override
    public boolean visit(MySqlShowTableStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTableStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowTableInfoStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTableInfoStatement x) {

    }

    @Override
    public boolean visit(MySqlShowTriggersStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTriggersStatement x) {

    }

    @Override
    public boolean visit(MySqlShowVariantsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowVariantsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowTraceStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTraceStatement x) {

    }

    @Override
    public boolean visit(MySqlShowBroadcastsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowBroadcastsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowRuleStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowRuleStatement x) {

    }

    @Override
    public boolean visit(MySqlShowRuleStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowRuleStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowDsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowDsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowDdlStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowDdlStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowTopologyStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTopologyStatement x) {

    }

    @Override
    public boolean visit(MySqlShowFilesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowFilesStatement x) {

    }

    @Override
    public boolean visit(MySqlRenameTableStatement.Item x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRenameTableStatement.Item x) {

    }

    @Override
    public boolean visit(MySqlRenameTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRenameTableStatement x) {

    }

    @Override
    public boolean visit(MysqlShowDbLockStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlShowDbLockStatement x) {

    }

    @Override
    public boolean visit(MySqlShowDatabaseStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowDatabaseStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlUseIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUseIndexHint x) {

    }

    @Override
    public boolean visit(MySqlIgnoreIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlIgnoreIndexHint x) {

    }

    @Override
    public boolean visit(MySqlLockTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlLockTableStatement x) {

    }

    @Override
    public boolean visit(MySqlLockTableStatement.Item x) {
        return true;
    }

    @Override
    public void endVisit(MySqlLockTableStatement.Item x) {

    }

    @Override
    public boolean visit(MySqlUnlockTablesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUnlockTablesStatement x) {

    }

    @Override
    public boolean visit(MySqlForceIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlForceIndexHint x) {

    }

    @Override
    public boolean visit(MySqlAlterTableChangeColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableChangeColumn x) {

    }

    @Override
    public boolean visit(MySqlAlterTableOption x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableOption x) {

    }

    @Override
    public boolean visit(MySqlCreateTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateTableStatement x) {

    }

    @Override
    public boolean visit(MySqlHelpStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlHelpStatement x) {

    }

    @Override
    public boolean visit(MySqlCharExpr x) {
        parameterizeAndExportPara(x);
        return false;
    }

    @Override
    public void endVisit(MySqlCharExpr x) {

    }

    @Override
    public boolean visit(MySqlAlterTableModifyColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableModifyColumn x) {

    }

    @Override
    public boolean visit(MySqlAlterTableDiscardTablespace x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableDiscardTablespace x) {

    }

    @Override
    public boolean visit(MySqlAlterTableImportTablespace x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableImportTablespace x) {

    }

    @Override
    public boolean visit(TableSpaceOption x) {
        return true;
    }

    @Override
    public void endVisit(TableSpaceOption x) {

    }

    @Override
    public boolean visit(MySqlAnalyzeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAnalyzeStatement x) {

    }

    @Override
    public boolean visit(MySqlCreateExternalCatalogStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateExternalCatalogStatement x) {

    }

    @Override
    public boolean visit(MySqlAlterUserStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterUserStatement x) {

    }

    @Override
    public boolean visit(MySqlOptimizeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlOptimizeStatement x) {

    }

    @Override
    public boolean visit(MySqlHintStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlHintStatement x) {

    }

    @Override
    public boolean visit(MySqlOrderingExpr x) {
        return true;
    }

    @Override
    public void endVisit(MySqlOrderingExpr x) {

    }

    @Override
    public boolean visit(MySqlCaseStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCaseStatement x) {

    }

    @Override
    public boolean visit(MySqlDeclareStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDeclareStatement x) {

    }

    @Override
    public boolean visit(MySqlSelectIntoStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSelectIntoStatement x) {

    }

    @Override
    public boolean visit(MySqlWhenStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlWhenStatement x) {

    }

    @Override
    public boolean visit(MySqlLeaveStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlLeaveStatement x) {

    }

    @Override
    public boolean visit(MySqlIterateStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlIterateStatement x) {

    }

    @Override
    public boolean visit(MySqlRepeatStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRepeatStatement x) {

    }

    @Override
    public boolean visit(MySqlCursorDeclareStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCursorDeclareStatement x) {

    }

    @Override
    public boolean visit(MySqlUpdateTableSource x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUpdateTableSource x) {

    }

    @Override
    public boolean visit(MySqlAlterTableAlterColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableAlterColumn x) {

    }

    @Override
    public boolean visit(MySqlAlterTableForce x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableForce x) {

    }

    @Override
    public boolean visit(MySqlAlterTableCheckConstraint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableCheckConstraint x) {

    }

    @Override
    public boolean visit(MySqlAlterTableLock x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableLock x) {

    }

    @Override
    public boolean visit(MySqlAlterTableOrderBy x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableOrderBy x) {

    }

    @Override
    public boolean visit(MySqlAlterTableValidation x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableValidation x) {

    }

    @Override
    public boolean visit(MySqlSubPartitionByKey x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSubPartitionByKey x) {

    }

    @Override
    public boolean visit(MySqlSubPartitionByList x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSubPartitionByList x) {

    }

    @Override
    public boolean visit(MySqlDeclareHandlerStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDeclareHandlerStatement x) {

    }

    @Override
    public boolean visit(MySqlDeclareConditionStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDeclareConditionStatement x) {

    }

    @Override
    public boolean visit(MySqlFlushStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlFlushStatement x) {

    }

    @Override
    public boolean visit(MySqlEventSchedule x) {
        return true;
    }

    @Override
    public void endVisit(MySqlEventSchedule x) {

    }

    @Override
    public boolean visit(MySqlCreateEventStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateEventStatement x) {

    }

    @Override
    public boolean visit(MySqlCreateAddLogFileGroupStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateAddLogFileGroupStatement x) {

    }

    @Override
    public boolean visit(MySqlCreateServerStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateServerStatement x) {

    }

    @Override
    public boolean visit(MySqlCreateTableSpaceStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateTableSpaceStatement x) {

    }

    @Override
    public boolean visit(MySqlAlterEventStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterEventStatement x) {

    }

    @Override
    public boolean visit(MySqlAlterLogFileGroupStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterLogFileGroupStatement x) {

    }

    @Override
    public boolean visit(MySqlAlterServerStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterServerStatement x) {

    }

    @Override
    public boolean visit(MySqlAlterTablespaceStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTablespaceStatement x) {

    }

    @Override
    public boolean visit(MySqlChecksumTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlChecksumTableStatement x) {

    }

    @Override
    public boolean visit(MySqlShowDatasourcesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowDatasourcesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowNodeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowNodeStatement x) {

    }

    @Override
    public boolean visit(MySqlShowHelpStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowHelpStatement x) {

    }

    @Override
    public boolean visit(MySqlFlashbackStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlFlashbackStatement x) {

    }

    @Override
    public boolean visit(MySqlShowConfigStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowConfigStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPlanCacheStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPlanCacheStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPhysicalProcesslistStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPhysicalProcesslistStatement x) {

    }

    @Override
    public boolean visit(MySqlRenameSequenceStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRenameSequenceStatement x) {

    }

    @Override
    public boolean visit(MySqlCheckTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCheckTableStatement x) {

    }

    @Override
    public boolean visit(MysqlCreateFullTextCharFilterStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlCreateFullTextCharFilterStatement x) {

    }

    @Override
    public boolean visit(MysqlShowFullTextStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlShowFullTextStatement x) {

    }

    @Override
    public boolean visit(MysqlShowCreateFullTextStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlShowCreateFullTextStatement x) {

    }

    @Override
    public boolean visit(MysqlAlterFullTextStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlAlterFullTextStatement x) {

    }

    @Override
    public boolean visit(MysqlDropFullTextStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlDropFullTextStatement x) {

    }

    @Override
    public boolean visit(MysqlCreateFullTextTokenizerStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlCreateFullTextTokenizerStatement x) {

    }

    @Override
    public boolean visit(MysqlCreateFullTextTokenFilterStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlCreateFullTextTokenFilterStatement x) {

    }

    @Override
    public boolean visit(MysqlCreateFullTextAnalyzerStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlCreateFullTextAnalyzerStatement x) {

    }

    @Override
    public boolean visit(MysqlCreateFullTextDictionaryStatement x) {
        return true;
    }

    @Override
    public void endVisit(MysqlCreateFullTextDictionaryStatement x) {

    }

    @Override
    public boolean visit(MySqlAlterTableAlterFullTextIndex x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableAlterFullTextIndex x) {

    }

    @Override
    public boolean visit(MySqlExecuteForAdsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlExecuteForAdsStatement x) {

    }

    @Override
    public boolean visit(MySqlManageInstanceGroupStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlManageInstanceGroupStatement x) {

    }

    @Override
    public boolean visit(MySqlRaftMemberChangeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRaftMemberChangeStatement x) {

    }

    @Override
    public boolean visit(MySqlRaftLeaderTransferStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRaftLeaderTransferStatement x) {

    }

    @Override
    public boolean visit(MySqlMigrateStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlMigrateStatement x) {

    }

    @Override
    public boolean visit(MySqlShowClusterNameStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowClusterNameStatement x) {

    }

    @Override
    public boolean visit(MySqlShowJobStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowJobStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowMigrateTaskStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowMigrateTaskStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlSubPartitionByValue x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSubPartitionByValue x) {

    }

    @Override
    public boolean visit(MySqlExtPartition x) {
        return true;
    }

    @Override
    public void endVisit(MySqlExtPartition x) {

    }

    @Override
    public boolean visit(MySqlExtPartition.Item x) {
        return true;
    }

    @Override
    public void endVisit(MySqlExtPartition.Item x) {

    }

    @Override
    public boolean visit(DrdsInspectRuleVersionStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsInspectRuleVersionStatement x) {

    }

    @Override
    public boolean visit(DrdsInspectIndexStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsInspectIndexStatement x) {

    }

    @Override
    public boolean visit(DrdsChangeRuleVersionStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsChangeRuleVersionStatement x) {

    }

    @Override
    public boolean visit(DrdsRefreshLocalRulesStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsRefreshLocalRulesStatement x) {

    }

    @Override
    public boolean visit(DrdsClearSeqCacheStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsClearSeqCacheStatement x) {

    }

    @Override
    public boolean visit(DrdsInspectSeqRangeStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsInspectSeqRangeStatement x) {

    }

    @Override
    public boolean visit(DrdsConvertAllSequencesStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsConvertAllSequencesStatement x) {

    }

    @Override
    public void endVisit(DrdsShowTransStatement x) {

    }

    @Override
    public boolean visit(DrdsShowTransStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowTransStatsStatement x) {

    }

    @Override
    public boolean visit(DrdsShowTransStatsStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsPurgeTransStatement x) {

    }

    @Override
    public boolean visit(DrdsPurgeTransStatement x) {
        return true;
    }

    @Override
    public boolean visit(DrdsMoveDataBase x) {
        return true;
    }

    @Override
    public void endVisit(DrdsMoveDataBase x) {

    }

    @Override
    public boolean visit(DrdsShowMoveDatabaseStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowMoveDatabaseStatement x) {

    }

    @Override
    public boolean visit(DrdsShowStorage x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowStorage x) {

    }

    @Override
    public boolean visit(DrdsShowTableGroup x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowTableGroup x) {

    }

    @Override
    public boolean visit(DrdsShowCreateTableGroup x) {
        return true;
    }

    @Override
    public void endVisit(DrdsShowCreateTableGroup x) {

    }

    @Override
    public boolean visit(DrdsAlterTableSingle x) {
        return true;
    }

    @Override
    public void endVisit(DrdsAlterTableSingle x) {

    }

    @Override
    public boolean visit(DrdsAlterTableBroadcast x) {
        return true;
    }

    @Override
    public void endVisit(DrdsAlterTableBroadcast x) {

    }

    @Override
    public boolean visit(DrdsAlterTableAsOfTimeStamp x) {
        return true;
    }

    @Override
    public void endVisit(DrdsAlterTableAsOfTimeStamp x) {

    }

    @Override
    public boolean visit(DrdsAlterTablePurgeBeforeTimeStamp x) {
        return true;
    }

    @Override
    public void endVisit(DrdsAlterTablePurgeBeforeTimeStamp x) {

    }

    @Override
    public boolean visit(DrdsAlterFileStorageStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsAlterFileStorageStatement x) {

    }

    @Override
    public boolean visit(DrdsAlterTablePartition x) {
        return true;
    }

    @Override
    public void endVisit(DrdsAlterTablePartition x) {

    }

    @Override
    public boolean visit(MySqlSetRoleStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetRoleStatement x) {

    }

    @Override
    public boolean visit(MySqlSetDefaultRoleStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetDefaultRoleStatement x) {

    }

    @Override
    public boolean visit(DrdsSplitPartition x) {
        return false;
    }

    @Override
    public void endVisit(DrdsSplitPartition x) {

    }

    @Override
    public boolean visit(DrdsAlterTableAllocateLocalPartition x) {
        return false;
    }

    @Override
    public void endVisit(DrdsAlterTableAllocateLocalPartition x) {

    }

    @Override
    public boolean visit(DrdsAlterTableExpireLocalPartition x) {
        return false;
    }

    @Override
    public void endVisit(DrdsAlterTableExpireLocalPartition x) {

    }

    @Override
    public boolean visit(DrdsDropFileStorageStatement x) {
        return true;
    }

    @Override
    public void endVisit(DrdsDropFileStorageStatement x) {
    }

    @Override
    public boolean visit(DrdsShowLocality x) {
        return false;
    }

    @Override
    public void endVisit(DrdsShowLocality x) {

    }

    @Override
    public boolean visit(MySqlShowBinaryStreamsStatement mySqlShowBinaryStreamsStatement) {
        return false;
    }

    @Override
    public void endVisit(MySqlShowBinaryStreamsStatement mySqlShowBinaryStreamsStatement) {

    }

    @Override
    public boolean visit(DrdsShowChangeSet x) {
        return false;
    }

    @Override
    public void endVisit(DrdsShowChangeSet x) {

    }

    @Override
    public boolean visit(DrdsShowTableReplicate x) {
        return false;
    }

    @Override
    public void endVisit(DrdsShowTableReplicate x) {

    }

    @Override
    public boolean visit(DrdsAlignToTableGroup x) {
        return false;
    }

    @Override
    public void endVisit(DrdsAlignToTableGroup x) {

    }
}
