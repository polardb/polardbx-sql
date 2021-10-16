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

import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupExtractHotKey;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupMergePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupMovePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupRenamePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupReorgPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableGroupSplitPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateTableStatement;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTableBroadcast;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTablePartition;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsCreateCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsDropCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsDropCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectDDLJobCache;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectGroupSeqRangeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsInspectRuleVersionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsMoveDataBase;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPauseDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsPurgeTransStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRecoverDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRefreshLocalRulesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRefreshTopology;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRemoveDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsRollbackDDLJob;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowCclRuleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowCclTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowDDLJobs;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowDDLResults;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowGlobalIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowMetadataLock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowMoveDatabaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTableGroup;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsShowTransStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsSlowSqlCclStatement;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlClearPlanCacheStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateAddLogFileGroupStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateEventStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateExternalCatalogStatement;
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
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public interface MySqlASTVisitor extends SQLASTVisitor {
    boolean visit(MySqlTableIndex x);

    void endVisit(MySqlTableIndex x);

    boolean visit(MySqlKey x);

    void endVisit(MySqlKey x);

    boolean visit(MySqlPrimaryKey x);

    void endVisit(MySqlPrimaryKey x);

    boolean visit(MySqlUnique x);

    void endVisit(MySqlUnique x);

    boolean visit(MysqlForeignKey x);

    void endVisit(MysqlForeignKey x);

    void endVisit(MySqlPrepareStatement x);

    boolean visit(MySqlPrepareStatement x);

    void endVisit(MySqlExecuteStatement x);

    boolean visit(MysqlDeallocatePrepareStatement x);

    void endVisit(MysqlDeallocatePrepareStatement x);

    boolean visit(MySqlExecuteStatement x);

    void endVisit(MySqlDeleteStatement x);

    boolean visit(MySqlDeleteStatement x);

    void endVisit(MySqlInsertStatement x);

    boolean visit(MySqlInsertStatement x);

    void endVisit(MySqlLoadDataInFileStatement x);

    boolean visit(MySqlLoadDataInFileStatement x);

    void endVisit(MySqlLoadXmlStatement x);

    boolean visit(MySqlLoadXmlStatement x);

    void endVisit(SQLShowColumnsStatement x);

    boolean visit(SQLShowColumnsStatement x);

    void endVisit(MySqlShowWarningsStatement x);

    boolean visit(MySqlShowWarningsStatement x);

    void endVisit(MySqlShowStatusStatement x);

    boolean visit(MySqlShowStatusStatement x);

    void endVisit(MySqlShowAuthorsStatement x);

    boolean visit(MySqlShowAuthorsStatement x);

    void endVisit(MysqlShowHtcStatement x);

    boolean visit(MysqlShowHtcStatement x);

    void endVisit(MysqlShowStcStatement x);

    boolean visit(MysqlShowStcStatement x);

    void endVisit(CobarShowStatus x);

    boolean visit(CobarShowStatus x);

    void endVisit(DrdsShowDDLJobs x);

    boolean visit(DrdsShowDDLJobs x);

    void endVisit(DrdsShowDDLResults x);

    boolean visit(DrdsShowDDLResults x);

    void endVisit(MysqlShowRouteStatement x);

    boolean visit(MysqlShowRouteStatement x);

    void endVisit(DrdsCancelDDLJob x);

    boolean visit(DrdsCancelDDLJob x);

    void endVisit(DrdsRecoverDDLJob x);

    boolean visit(DrdsRecoverDDLJob x);

    void endVisit(DrdsContinueDDLJob x);

    boolean visit(DrdsContinueDDLJob x);

    void endVisit(DrdsPauseDDLJob x);

    boolean visit(DrdsPauseDDLJob x);

    void endVisit(DrdsRollbackDDLJob x);

    boolean visit(DrdsRollbackDDLJob x);

    void endVisit(DrdsRemoveDDLJob x);

    boolean visit(DrdsRemoveDDLJob x);

    void endVisit(DrdsInspectDDLJobCache x);

    boolean visit(DrdsInspectDDLJobCache x);

    void endVisit(DrdsClearDDLJobCache x);

    boolean visit(DrdsClearDDLJobCache x);

    void endVisit(DrdsChangeDDLJob x);

    boolean visit(DrdsChangeDDLJob x);

    void endVisit(DrdsBaselineStatement x);

    boolean visit(DrdsBaselineStatement x);

    void endVisit(DrdsShowGlobalIndex x);

    boolean visit(DrdsShowGlobalIndex x);

    void endVisit(DrdsShowMetadataLock x);

    boolean visit(DrdsShowMetadataLock x);

    void endVisit(DrdsCheckGlobalIndex x);

    boolean visit(DrdsCheckGlobalIndex x);

    void endVisit(DrdsCreateCclRuleStatement x);

    boolean visit(DrdsCreateCclRuleStatement x);

    void endVisit(DrdsDropCclRuleStatement x);

    boolean visit(DrdsDropCclRuleStatement x);

    void endVisit(DrdsShowCclRuleStatement x);

    boolean visit(DrdsShowCclRuleStatement x);

    void endVisit(DrdsClearCclRulesStatement x);

    boolean visit(DrdsClearCclRulesStatement x);

    void endVisit(DrdsCreateCclTriggerStatement x);

    boolean visit(DrdsCreateCclTriggerStatement x);

    void endVisit(DrdsDropCclTriggerStatement x);

    boolean visit(DrdsDropCclTriggerStatement x);

    void endVisit(DrdsClearCclTriggersStatement x);

    boolean visit(DrdsClearCclTriggersStatement x);

    void endVisit(DrdsShowCclTriggerStatement x);

    boolean visit(DrdsShowCclTriggerStatement x);

    void endVisit(DrdsSlowSqlCclStatement x);

    boolean visit(DrdsSlowSqlCclStatement x);

    void endVisit(MySqlBinlogStatement x);

    boolean visit(MySqlBinlogStatement x);

    void endVisit(MySqlResetStatement x);

    boolean visit(MySqlResetStatement x);

    void endVisit(MySqlCreateUserStatement x);

    boolean visit(MySqlCreateUserStatement x);

    void endVisit(MySqlCreateUserStatement.UserSpecification x);

    boolean visit(MySqlCreateUserStatement.UserSpecification x);

    boolean visit(MySqlCreateRoleStatement x);

    void endVisit(MySqlCreateRoleStatement x);

    boolean visit(MySqlCreateRoleStatement.RoleSpec x);

    void endVisit(MySqlCreateRoleStatement.RoleSpec x);

    boolean visit(MySqlDropRoleStatement x);

    void endVisit(MySqlDropRoleStatement x);

    boolean visit(MySqlGrantRoleStatement x);

    void endVisit(MySqlGrantRoleStatement x);

    boolean visit(MySqlRevokeRoleStatement x);

    void endVisit(MySqlRevokeRoleStatement x);

    void endVisit(MySqlPartitionByKey x);

    boolean visit(MySqlPartitionByKey x);

    void endVisit(MySqlUpdatePlanCacheStatement x);

    boolean visit(MySqlUpdatePlanCacheStatement x);

    void endVisit(MySqlShowPlanCacheStatusStatement x);

    boolean visit(MySqlShowPlanCacheStatusStatement x);

    void endVisit(MySqlClearPlanCacheStatement x);

    boolean visit(MySqlClearPlanCacheStatement x);

    void endVisit(MySqlDisabledPlanCacheStatement x);

    boolean visit(MySqlDisabledPlanCacheStatement x);

    void endVisit(MySqlExplainPlanCacheStatement x);

    boolean visit(MySqlExplainPlanCacheStatement x);

    boolean visit(MySqlSelectQueryBlock x);

    void endVisit(MySqlSelectQueryBlock x);

    boolean visit(MySqlOutFileExpr x);

    void endVisit(MySqlOutFileExpr x);

    boolean visit(MySqlExplainStatement x);

    void endVisit(MySqlExplainStatement x);

    boolean visit(MySqlUpdateStatement x);

    void endVisit(MySqlUpdateStatement x);

    boolean visit(MySqlSetTransactionStatement x);

    void endVisit(MySqlSetTransactionStatement x);

    boolean visit(MySqlShowHMSMetaStatement x);

    void endVisit(MySqlShowHMSMetaStatement x);

    boolean visit(MySqlShowBinaryLogsStatement x);

    void endVisit(MySqlShowBinaryLogsStatement x);

    boolean visit(MySqlShowMasterLogsStatement x);

    void endVisit(MySqlShowMasterLogsStatement x);

    boolean visit(MySqlShowCharacterSetStatement x);

    void endVisit(MySqlShowCharacterSetStatement x);

    boolean visit(MySqlShowCollationStatement x);

    void endVisit(MySqlShowCollationStatement x);

    boolean visit(MySqlShowBinLogEventsStatement x);

    void endVisit(MySqlShowBinLogEventsStatement x);

    boolean visit(MySqlShowContributorsStatement x);

    void endVisit(MySqlShowContributorsStatement x);

    boolean visit(MySqlShowCreateDatabaseStatement x);

    void endVisit(MySqlShowCreateDatabaseStatement x);

    boolean visit(MySqlShowCreateEventStatement x);

    void endVisit(MySqlShowCreateEventStatement x);

    boolean visit(MySqlShowCreateFunctionStatement x);

    void endVisit(MySqlShowCreateFunctionStatement x);

    boolean visit(MySqlShowCreateProcedureStatement x);

    void endVisit(MySqlShowCreateProcedureStatement x);

    boolean visit(SQLShowCreateDatabaseStatement x);

    void endVisit(SQLShowCreateDatabaseStatement x);

    boolean visit(SQLShowCreateTableStatement x);

    void endVisit(SQLShowCreateTableStatement x);

    boolean visit(MySqlShowCreateTriggerStatement x);

    void endVisit(MySqlShowCreateTriggerStatement x);

    boolean visit(MySqlShowEngineStatement x);

    void endVisit(MySqlShowEngineStatement x);

    boolean visit(MySqlShowEnginesStatement x);

    void endVisit(MySqlShowEnginesStatement x);

    boolean visit(MySqlShowErrorsStatement x);

    void endVisit(MySqlShowErrorsStatement x);

    boolean visit(MySqlShowEventsStatement x);

    void endVisit(MySqlShowEventsStatement x);

    boolean visit(MySqlShowFunctionCodeStatement x);

    void endVisit(MySqlShowFunctionCodeStatement x);

    boolean visit(MySqlShowFunctionStatusStatement x);

    void endVisit(MySqlShowFunctionStatusStatement x);

    boolean visit(MySqlShowGrantsStatement x);

    void endVisit(MySqlShowGrantsStatement x);

    boolean visit(MySql8ShowGrantsStatement x);

    void endVisit(MySql8ShowGrantsStatement x);

    boolean visit(MySqlUserName x);

    void endVisit(MySqlUserName x);

    boolean visit(MySqlAlterDatabaseSetOption x);

    void endVisit(MySqlAlterDatabaseSetOption x);

    boolean visit(MySqlAlterDatabaseKillJob x);

    void endVisit(MySqlAlterDatabaseKillJob x);

    boolean visit(MySqlShowMasterStatusStatement x);

    void endVisit(MySqlShowMasterStatusStatement x);

    boolean visit(MySqlShowOpenTablesStatement x);

    void endVisit(MySqlShowOpenTablesStatement x);

    boolean visit(MySqlShowPluginsStatement x);

    void endVisit(MySqlShowPluginsStatement x);

    boolean visit(MySqlShowPartitionsStatement x);

    void endVisit(MySqlShowPartitionsStatement x);

    boolean visit(MySqlShowPrivilegesStatement x);

    void endVisit(MySqlShowPrivilegesStatement x);

    boolean visit(MySqlShowProcedureCodeStatement x);

    void endVisit(MySqlShowProcedureCodeStatement x);

    boolean visit(MySqlShowProcedureStatusStatement x);

    void endVisit(MySqlShowProcedureStatusStatement x);

    boolean visit(MySqlShowProcessListStatement x);

    void endVisit(MySqlShowProcessListStatement x);

    boolean visit(MySqlShowProfileStatement x);

    void endVisit(MySqlShowProfileStatement x);

    boolean visit(MySqlShowProfilesStatement x);

    void endVisit(MySqlShowProfilesStatement x);

    boolean visit(MySqlShowRelayLogEventsStatement x);

    void endVisit(MySqlShowRelayLogEventsStatement x);

    boolean visit(MySqlShowSlaveHostsStatement x);

    void endVisit(MySqlShowSlaveHostsStatement x);

    boolean visit(MySqlShowSequencesStatement x);

    void endVisit(MySqlShowSequencesStatement x);

    boolean visit(MySqlShowSlaveStatusStatement x);

    void endVisit(MySqlShowSlaveStatusStatement x);

    boolean visit(MySqlShowSlowStatement x);

    void endVisit(MySqlShowSlowStatement x);

    boolean visit(MySqlShowTableStatusStatement x);

    void endVisit(MySqlShowTableStatusStatement x);

    boolean visit(MySqlShowTableInfoStatement x);

    void endVisit(MySqlShowTableInfoStatement x);

    boolean visit(MySqlShowTriggersStatement x);

    void endVisit(MySqlShowTriggersStatement x);

    boolean visit(MySqlShowVariantsStatement x);

    void endVisit(MySqlShowVariantsStatement x);

    boolean visit(MySqlShowTraceStatement x);

    void endVisit(MySqlShowTraceStatement x);

    boolean visit(MySqlShowBroadcastsStatement x);

    void endVisit(MySqlShowBroadcastsStatement x);

    boolean visit(MySqlShowRuleStatement x);

    void endVisit(MySqlShowRuleStatement x);

    boolean visit(MySqlShowRuleStatusStatement x);

    void endVisit(MySqlShowRuleStatusStatement x);

    boolean visit(MySqlShowDsStatement x);

    void endVisit(MySqlShowDsStatement x);

    boolean visit(MySqlShowDdlStatusStatement x);

    void endVisit(MySqlShowDdlStatusStatement x);

    boolean visit(MySqlShowTopologyStatement x);

    void endVisit(MySqlShowTopologyStatement x);

    boolean visit(MySqlRenameTableStatement.Item x);

    void endVisit(MySqlRenameTableStatement.Item x);

    boolean visit(MySqlRenameTableStatement x);

    void endVisit(MySqlRenameTableStatement x);

    boolean visit(MysqlShowDbLockStatement x);

    void endVisit(MysqlShowDbLockStatement x);

    boolean visit(MySqlShowDatabaseStatusStatement x);

    void endVisit(MySqlShowDatabaseStatusStatement x);

    boolean visit(MySqlUseIndexHint x);

    void endVisit(MySqlUseIndexHint x);

    boolean visit(MySqlIgnoreIndexHint x);

    void endVisit(MySqlIgnoreIndexHint x);

    boolean visit(MySqlLockTableStatement x);

    void endVisit(MySqlLockTableStatement x);

    boolean visit(MySqlLockTableStatement.Item x);

    void endVisit(MySqlLockTableStatement.Item x);

    boolean visit(MySqlUnlockTablesStatement x);

    void endVisit(MySqlUnlockTablesStatement x);

    boolean visit(MySqlForceIndexHint x);

    void endVisit(MySqlForceIndexHint x);

    boolean visit(MySqlAlterTableChangeColumn x);

    void endVisit(MySqlAlterTableChangeColumn x);

    boolean visit(MySqlAlterTableOption x);

    void endVisit(MySqlAlterTableOption x);

    boolean visit(MySqlCreateTableStatement x);

    void endVisit(MySqlCreateTableStatement x);

    boolean visit(MySqlHelpStatement x);

    void endVisit(MySqlHelpStatement x);

    boolean visit(MySqlCharExpr x);

    void endVisit(MySqlCharExpr x);

    boolean visit(MySqlAlterTableModifyColumn x);

    void endVisit(MySqlAlterTableModifyColumn x);

    boolean visit(MySqlAlterTableDiscardTablespace x);

    void endVisit(MySqlAlterTableDiscardTablespace x);

    boolean visit(MySqlAlterTableImportTablespace x);

    void endVisit(MySqlAlterTableImportTablespace x);

    boolean visit(MySqlCreateTableStatement.TableSpaceOption x);

    void endVisit(MySqlCreateTableStatement.TableSpaceOption x);

    boolean visit(MySqlAnalyzeStatement x);

    void endVisit(MySqlAnalyzeStatement x);

    boolean visit(MySqlCreateExternalCatalogStatement x);

    void endVisit(MySqlCreateExternalCatalogStatement x);

    boolean visit(MySqlAlterUserStatement x);

    void endVisit(MySqlAlterUserStatement x);

    boolean visit(MySqlOptimizeStatement x);

    void endVisit(MySqlOptimizeStatement x);

    boolean visit(MySqlHintStatement x);

    void endVisit(MySqlHintStatement x);

    boolean visit(MySqlOrderingExpr x);

    void endVisit(MySqlOrderingExpr x);

    boolean visit(MySqlCaseStatement x);

    void endVisit(MySqlCaseStatement x);

    boolean visit(MySqlDeclareStatement x);

    void endVisit(MySqlDeclareStatement x);

    boolean visit(MySqlSelectIntoStatement x);

    void endVisit(MySqlSelectIntoStatement x);

    boolean visit(MySqlWhenStatement x);

    void endVisit(MySqlWhenStatement x);

    boolean visit(MySqlLeaveStatement x);

    void endVisit(MySqlLeaveStatement x);

    boolean visit(MySqlIterateStatement x);

    void endVisit(MySqlIterateStatement x);

    boolean visit(MySqlRepeatStatement x);

    void endVisit(MySqlRepeatStatement x);

    boolean visit(MySqlCursorDeclareStatement x);

    void endVisit(MySqlCursorDeclareStatement x);

    boolean visit(MySqlUpdateTableSource x);

    void endVisit(MySqlUpdateTableSource x);

    boolean visit(MySqlAlterTableAlterColumn x);

    void endVisit(MySqlAlterTableAlterColumn x);

    boolean visit(MySqlAlterTableForce x);

    void endVisit(MySqlAlterTableForce x);

    boolean visit(MySqlAlterTableCheckConstraint x);

    void endVisit(MySqlAlterTableCheckConstraint x);

    boolean visit(MySqlAlterTableLock x);

    void endVisit(MySqlAlterTableLock x);

    boolean visit(MySqlAlterTableOrderBy x);

    void endVisit(MySqlAlterTableOrderBy x);

    boolean visit(MySqlAlterTableValidation x);

    void endVisit(MySqlAlterTableValidation x);

    boolean visit(MySqlSubPartitionByKey x);

    void endVisit(MySqlSubPartitionByKey x);

    boolean visit(MySqlSubPartitionByList x);

    void endVisit(MySqlSubPartitionByList x);

    boolean visit(MySqlDeclareHandlerStatement x);

    void endVisit(MySqlDeclareHandlerStatement x);

    boolean visit(MySqlDeclareConditionStatement x);

    void endVisit(MySqlDeclareConditionStatement x);

    boolean visit(MySqlFlushStatement x);

    void endVisit(MySqlFlushStatement x);

    boolean visit(MySqlEventSchedule x);
    void endVisit(MySqlEventSchedule x);

    boolean visit(MySqlCreateEventStatement x);
    void endVisit(MySqlCreateEventStatement x);

    boolean visit(MySqlCreateAddLogFileGroupStatement x);
    void endVisit(MySqlCreateAddLogFileGroupStatement x);

    boolean visit(MySqlCreateServerStatement x);
    void endVisit(MySqlCreateServerStatement x);

    boolean visit(MySqlCreateTableSpaceStatement x);
    void endVisit(MySqlCreateTableSpaceStatement x);

    boolean visit(MySqlAlterEventStatement x);
    void endVisit(MySqlAlterEventStatement x);

    boolean visit(MySqlAlterLogFileGroupStatement x);
    void endVisit(MySqlAlterLogFileGroupStatement x);

    boolean visit(MySqlAlterServerStatement x);
    void endVisit(MySqlAlterServerStatement x);

    boolean visit(MySqlAlterTablespaceStatement x);
    void endVisit(MySqlAlterTablespaceStatement x);

    boolean visit(MySqlChecksumTableStatement x);
    void endVisit(MySqlChecksumTableStatement x);

    boolean visit(MySqlShowDatasourcesStatement x);
    void endVisit(MySqlShowDatasourcesStatement x);

    boolean visit(MySqlShowNodeStatement x);
    void endVisit(MySqlShowNodeStatement x);

    boolean visit(MySqlShowHelpStatement x);
    void endVisit(MySqlShowHelpStatement x);

    boolean visit(MySqlFlashbackStatement x);
    void endVisit(MySqlFlashbackStatement x);

    boolean visit(MySqlShowConfigStatement x);
    void endVisit(MySqlShowConfigStatement x);

    boolean visit(MySqlShowPlanCacheStatement x);
    void endVisit(MySqlShowPlanCacheStatement x);

    boolean visit(MySqlShowPhysicalProcesslistStatement x);
    void endVisit(MySqlShowPhysicalProcesslistStatement x);

    boolean visit(MySqlRenameSequenceStatement x);
    void endVisit(MySqlRenameSequenceStatement x);

    boolean visit(MySqlCheckTableStatement x);
    void endVisit(MySqlCheckTableStatement x);

    boolean visit(MysqlCreateFullTextCharFilterStatement x);
    void endVisit(MysqlCreateFullTextCharFilterStatement x);

    boolean visit(MysqlShowFullTextStatement x);
    void endVisit(MysqlShowFullTextStatement x);

    boolean visit(MysqlShowCreateFullTextStatement x);
    void endVisit(MysqlShowCreateFullTextStatement x);

    boolean visit(MysqlAlterFullTextStatement x);
    void endVisit(MysqlAlterFullTextStatement x);

    boolean visit(MysqlDropFullTextStatement x);
    void endVisit(MysqlDropFullTextStatement x);

    boolean visit(MysqlCreateFullTextTokenizerStatement x);
    void endVisit(MysqlCreateFullTextTokenizerStatement x);

    boolean visit(MysqlCreateFullTextTokenFilterStatement x);
    void endVisit(MysqlCreateFullTextTokenFilterStatement x);

    boolean visit(MysqlCreateFullTextAnalyzerStatement x);
    void endVisit(MysqlCreateFullTextAnalyzerStatement x);

    boolean visit(MysqlCreateFullTextDictionaryStatement x);
    void endVisit(MysqlCreateFullTextDictionaryStatement x);

    boolean visit(MySqlAlterTableAlterFullTextIndex x);
    void endVisit(MySqlAlterTableAlterFullTextIndex x);

    boolean visit(MySqlExecuteForAdsStatement x);
    void endVisit(MySqlExecuteForAdsStatement x);

    boolean visit(MySqlManageInstanceGroupStatement x);
    void endVisit(MySqlManageInstanceGroupStatement x);

    boolean visit(MySqlRaftMemberChangeStatement x);
    void endVisit(MySqlRaftMemberChangeStatement x);

    boolean visit(MySqlRaftLeaderTransferStatement x);
    void endVisit(MySqlRaftLeaderTransferStatement x);

    boolean visit(MySqlMigrateStatement x);
    void endVisit(MySqlMigrateStatement x);

    boolean visit(MySqlShowClusterNameStatement x);
    void endVisit(MySqlShowClusterNameStatement x);

    boolean visit(MySqlShowJobStatusStatement x);
    void endVisit(MySqlShowJobStatusStatement x);

    boolean visit(MySqlShowMigrateTaskStatusStatement x);
    void endVisit(MySqlShowMigrateTaskStatusStatement x);

    boolean visit(MySqlSubPartitionByValue x);
    void endVisit(MySqlSubPartitionByValue x);

    boolean visit(MySqlExtPartition x);

    void endVisit(MySqlExtPartition x);

    boolean visit(MySqlExtPartition.Item x);

    void endVisit(MySqlExtPartition.Item x);


    boolean visit(DrdsInspectRuleVersionStatement x);

    void endVisit(DrdsInspectRuleVersionStatement x);

    boolean visit(DrdsChangeRuleVersionStatement x);

    void endVisit(DrdsChangeRuleVersionStatement x);

    boolean visit(DrdsRefreshLocalRulesStatement x);

    void endVisit(DrdsRefreshLocalRulesStatement x);

    boolean visit(DrdsClearSeqCacheStatement x);

    void endVisit(DrdsClearSeqCacheStatement x);

    boolean visit(DrdsInspectGroupSeqRangeStatement x);

    void endVisit(DrdsInspectGroupSeqRangeStatement x);

    void endVisit(DrdsShowTransStatement x);

    boolean visit(DrdsShowTransStatement x);

    void endVisit(DrdsPurgeTransStatement x);

    boolean visit(DrdsPurgeTransStatement x);

    boolean visit(DrdsMoveDataBase x);

    void endVisit(DrdsMoveDataBase x);

    boolean visit(SQLAlterTableDropCheck x);

    void endVisit(SQLAlterTableDropCheck x);

    boolean visit(DrdsShowMoveDatabaseStatement x);

    void endVisit(DrdsShowMoveDatabaseStatement x);

    boolean visit(DrdsShowTableGroup x);

    void endVisit(DrdsShowTableGroup x);

    boolean visit(DrdsAlterTableSingle x);

    void endVisit(DrdsAlterTableSingle x);

    boolean visit(DrdsAlterTableBroadcast x);

    void endVisit(DrdsAlterTableBroadcast x);

    boolean visit(DrdsAlterTablePartition x);

    void endVisit(DrdsAlterTablePartition x);

    boolean visit(MySqlSetRoleStatement x);
    void endVisit(MySqlSetRoleStatement x);

    boolean visit(MySqlSetDefaultRoleStatement x);
    void endVisit(MySqlSetDefaultRoleStatement x);

    boolean visit(DrdsAlterTableGroupSplitPartition x);

    void endVisit(DrdsAlterTableGroupSplitPartition x);

    boolean visit(DrdsAlterTableGroupMergePartition x);

    void endVisit(DrdsAlterTableGroupMergePartition x);

    boolean visit(DrdsAlterTableGroupMovePartition x);

    void endVisit(DrdsAlterTableGroupMovePartition x);

    boolean visit(DrdsAlterTableGroupExtractHotKey x);

    void endVisit(DrdsAlterTableGroupExtractHotKey x);

    boolean visit(DrdsAlterTableGroupReorgPartition x);

    void endVisit(DrdsAlterTableGroupReorgPartition x);

    boolean visit(DrdsAlterTableGroupRenamePartition x);

    void endVisit(DrdsAlterTableGroupRenamePartition x);

    boolean visit(DrdsRefreshTopology x);

    void endVisit(DrdsRefreshTopology x);
} //
