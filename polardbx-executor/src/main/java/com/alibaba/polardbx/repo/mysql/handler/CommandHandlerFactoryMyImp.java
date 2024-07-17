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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.druid.support.logging.Log;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.handler.LogicalAlterDatabaseHandler;
import com.alibaba.polardbx.executor.handler.LogicalAlterInstanceHandler;
import com.alibaba.polardbx.executor.handler.LogicalCancelReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalChangeMasterHandler;
import com.alibaba.polardbx.executor.handler.LogicalChangeReplicationFilterHandler;
import com.alibaba.polardbx.executor.handler.LogicalClearCclRulesHandler;
import com.alibaba.polardbx.executor.handler.LogicalClearCclTriggersHandler;
import com.alibaba.polardbx.executor.handler.LogicalContinueReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalContinueScheduleHandler;
import com.alibaba.polardbx.executor.handler.LogicalCreateCclRuleHandler;
import com.alibaba.polardbx.executor.handler.LogicalCreateCclTriggerHandler;
import com.alibaba.polardbx.executor.handler.LogicalStartReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalPauseReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalReplicaHashcheckHandler;
import com.alibaba.polardbx.executor.handler.LogicalCreateSecurityEntityHandler;
import com.alibaba.polardbx.executor.handler.LogicalDropSecurityEntityHandler;
import com.alibaba.polardbx.executor.handler.LogicalImportSequenceHandler;
import com.alibaba.polardbx.executor.handler.LogicalResetReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowReplicaCheckDiffHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowReplicaCheckProgressHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterStoragePoolHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalClearFileStorageHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateJavaFunctionHandler;
import com.alibaba.polardbx.executor.handler.LogicalFlushLogsHandler;
import com.alibaba.polardbx.executor.handler.LogicalSetCdcGlobalHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateJavaFunctionHandler;
import com.alibaba.polardbx.executor.handler.LogicalCreateSecurityLabelComponentHandler;
import com.alibaba.polardbx.executor.handler.LogicalCreateSecurityLabelHandler;
import com.alibaba.polardbx.executor.handler.LogicalCreateSecurityPolicyHandler;
import com.alibaba.polardbx.executor.handler.LogicalDropSecurityLabelComponentHandler;
import com.alibaba.polardbx.executor.handler.LogicalDropSecurityLabelHandler;
import com.alibaba.polardbx.executor.handler.LogicalDropSecurityPolicyHandler;
import com.alibaba.polardbx.executor.handler.LogicalGrantSecurityLabelHandler;
import com.alibaba.polardbx.executor.handler.LogicalRevokeSecurityLabelHandler;
import com.alibaba.polardbx.executor.handler.LogicalSetCdcGlobalHandler;
import com.alibaba.polardbx.executor.handler.LogicalCreateScheduleHandler;
import com.alibaba.polardbx.executor.handler.LogicalDropCclRuleHandler;
import com.alibaba.polardbx.executor.handler.LogicalDropCclTriggerHandler;
import com.alibaba.polardbx.executor.handler.LogicalDropScheduleHandler;
import com.alibaba.polardbx.executor.handler.LogicalFireScheduleHandler;
import com.alibaba.polardbx.executor.handler.LogicalFlushLogsHandler;
import com.alibaba.polardbx.executor.handler.LogicalFlushLogsHandler;
import com.alibaba.polardbx.executor.handler.LogicalImportSequenceHandler;
import com.alibaba.polardbx.executor.handler.LogicalPauseReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalPauseScheduleHandler;
import com.alibaba.polardbx.executor.handler.LogicalRebalanceHandler;
import com.alibaba.polardbx.executor.handler.LogicalRebalanceMasterHandler;
import com.alibaba.polardbx.executor.handler.LogicalReplicaHashcheckHandler;
import com.alibaba.polardbx.executor.handler.LogicalResetMasterHandler;
import com.alibaba.polardbx.executor.handler.LogicalResetReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalResetSlaveHandler;
import com.alibaba.polardbx.executor.handler.LogicalRestartMasterHandler;
import com.alibaba.polardbx.executor.handler.LogicalSetCdcGlobalHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowBinaryLogsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowBinaryStreamsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowBinlogEventsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowBroadcastsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowCclRuleHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowCclTriggerHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowCdcStorageHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowChangesetStatsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableGroupHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowDatasourcesHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowDsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowFilesHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowGlobalDeadlocksHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowHtcHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowIndexHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowLocalDeadlocksHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowMasterStatusHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowPartitionsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowPartitionsHeatmapHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowProfileHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowPruneTraceHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowReplicaCheckDiffHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowReplicaCheckProgressHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowRuleHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowRuleStatusHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowSequencesHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowSlaveStatusHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowSlowHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowStatsHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowStcHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowTableAccessHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowTableReplicateHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowTopologyHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowTraceHandler;
import com.alibaba.polardbx.executor.handler.LogicalSlowSqlCclHandler;
import com.alibaba.polardbx.executor.handler.LogicalStartMasterHandler;
import com.alibaba.polardbx.executor.handler.LogicalStartReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalStartSlaveHandler;
import com.alibaba.polardbx.executor.handler.LogicalStopMasterHandler;
import com.alibaba.polardbx.executor.handler.LogicalStopSlaveHandler;
import com.alibaba.polardbx.executor.handler.PolarShowGrantsHandler;
import com.alibaba.polardbx.executor.handler.ShowTransHandler;
import com.alibaba.polardbx.executor.handler.ShowTransStatsHandler;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterFileStoragHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterFunctionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterJoinGroupHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterProcedureHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterRuleHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterStoragePoolHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableAddPartitionProxyHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableDropPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableExtractPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableExtractPartitionProxyHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupAddPartitionProxyHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupAddTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupDropPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupExtractPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupExtractPartitionProxyHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupMergePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupModifyPartitionProxyHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupMovePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupRenamePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupReorgPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupSetLocalityHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupSetPartitionsLocalityHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupSplitPartitionByHotValueHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupSplitPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableGroupTruncatePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableMergePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableModifyPartitionProxyHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableMovePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTablePartitionCountHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableRemovePartitioningHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableRenamePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableReorgPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableRepartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableSetTableGroupHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableSplitPartitionByHotValueHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableSplitPartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableTruncatePartitionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCheckCciHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCheckGsiHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateDatabaseHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateDatabaseLikeAsHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateFileStorageHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateFunctionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateIndexHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateJavaFunctionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateMaterializedViewHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateProcedureHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateStoragePoolHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateTableGroupHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateViewHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropDatabaseHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropFileStorageHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropFunctionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropIndexHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropJavaFunctionHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropJoinGroupHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropMaterializedViewHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropProcedureHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropStoragePoolHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropTableGroupHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropViewHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalGenericDdlHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalImportDatabaseHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalInsertOverwriteHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalMergeTableGroupHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalMoveDatabaseHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalOptimizeTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalPushDownUdfHanlder;
import com.alibaba.polardbx.executor.handler.ddl.LogicalRefreshTopologyHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalRenameTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalRenameTablesHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalSequenceDdlHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalTruncateTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalUnArchiveHandler;
import com.alibaba.polardbx.executor.spi.ICommandHandlerFactory;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.spi.PlanHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.AffectedRowsSum;
import com.alibaba.polardbx.optimizer.core.rel.AlterTableGroupBackfill;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.ColumnBackFill;
import com.alibaba.polardbx.optimizer.core.rel.EmptyOperation;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.GsiBackfill;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsertIgnore;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalReplace;
import com.alibaba.polardbx.optimizer.core.rel.LogicalTableDataMigrationBackfill;
import com.alibaba.polardbx.optimizer.core.rel.LogicalUpsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.MoveTableBackfill;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyViewUnion;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalBackfill;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemLeader;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemRefreshStorage;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemReloadStorage;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRebalance;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterInstance;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterJoinGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterProcedure;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterRule;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterStoragePool;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterSystemSetConfig;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExtractPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupExtractPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupRenamePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupReorgPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSetLocality;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSetPartitionsLocality;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupTruncatePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTablePartitionCount;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRemovePartitioning;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRenamePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableReorgPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRepartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSetTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSplitPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableTruncatePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAnalyzeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalChangeConsensusLeader;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckCci;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckGsi;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalConvertAllSequences;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalClearFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJavaFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJoinGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateMaterializedView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateProcedure;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateStoragePool;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropJavaFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropJoinGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropMaterializedView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropProcedure;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropStoragePool;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalGenericDdl;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalImportDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalImportSequence;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalInsertOverwrite;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalInspectIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMergeTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMoveDatabases;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalPushDownUdf;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRefreshTopology;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTables;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSequenceDdl;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalUnArchive;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineCancelJobsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineContinueJobsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineInspectCacheHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEnginePauseJobsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEnginePauseRebalanceHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineRecoverJobsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineRollbackJobsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowDdlStatsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowJobsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowRebalanceBackFillHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowResultsHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineSkipRebalanceSubjobHandler;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineTerminateRebalanceHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalRecyclebin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.sql.SqlCreateDatabase;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:18:33
 * @since 5.0.0
 */
public class CommandHandlerFactoryMyImp implements ICommandHandlerFactory {

    public CommandHandlerFactoryMyImp(IRepository repo) {
        SINGLE_TABLE_SCAN_HANDLER = new MySingleTableScanHandler(repo);

        LOGICAL_GATHER_HANDLER = new GatherHandler();
        LOGICAL_MERGE_SORT_HANDLER = new MergeSortHandler();
        LOGICAL_VALUE_HANDLER = new LogicalValueHandler();
        LOGICAL_VIEW_HANDLER = new LogicalViewHandler();
        LOGICAL_MODIFY_VIEW_HANDLER = new LogicalModifyViewHandler(repo);
        LOGICAL_MODIFY_HANDLER = new LogicalModifyHandler(repo);
        SINGLE_TABLE_MODIFY_HANDLER = new MySingleTableModifyHandler(repo);
        LOGICAL_RELOCATE_HANDLER = new LogicalRelocateHandler(repo);
        LOGICAL_AFFECT_ROW_SUM_HANDLER = new AffectRowSumHandler();
        LOGICAL_INSERT_HANDLER = new LogicalInsertHandler(repo);
        LOGICAL_LOAD_DATA_HANDLER = new LogicalLoadDataHandler(repo);
        LOGICAL_INSERT_IGNORE_HANDLER = new LogicalInsertIgnoreHandler(repo);
        LOGICAL_REPLACE_HANDLER = new LogicalReplaceHandler(repo);
        LOGICAL_UPSERT_HANDLER = new LogicalUpsertHandler(repo);
        BROADCAST_TABLE_MODIFY_HANDLER = new BroadcastTableModifyHandler(repo);

        LOGICAL_RECYCLEBIN_HANDLER = new LogicalRecyclebinHandler(repo);
        LOGICAL_SEQUENCE_DDL_HANDLER = new LogicalSequenceDdlHandler(repo);

        LOGICAL_CREATE_TABLE_HANDLER = new LogicalCreateTableHandler(repo);
        LOGICAL_CONVERT_TABLE_MODE_HANDLER = new LogicalConvertTableModeHandler(repo);
        LOGICAL_ALTER_TABLE_HANDLER = new LogicalAlterTableHandler(repo);
        LOGICAL_ALTER_FILESTORAGE_HANDLER = new LogicalAlterFileStoragHandler(repo);
        LOGICAL_DROP_FILESTORAGE_HANDLER = new LogicalDropFileStorageHandler(repo);
        LOGICAL_CREATE_FILESTORAGE_HANDLER = new LogicalCreateFileStorageHandler(repo);
        LOGICAL_CLEAR_FILESTORAGE_HANDLER = new LogicalClearFileStorageHandler(repo);
        LOGICAL_RENAME_TABLE_HANDLER = new LogicalRenameTableHandler(repo);
        LOGICAL_RENAME_TABLES_HANDLER = new LogicalRenameTablesHandler(repo);
        LOGICAL_INSERT_OVERWRITE_HANDLER = new LogicalInsertOverwriteHandler(repo);
        LOGICAL_TRUNCATE_TABLE_HANDLER = new LogicalTruncateTableHandler(repo);
        LOGICAL_DROP_TABLE_HANDLER = new LogicalDropTableHandler(repo);
        LOGICAL_CREATE_INDEX_HANDLER = new LogicalCreateIndexHandler(repo);
        LOGICAL_DROP_LOGICAL_HANDLER = new LogicalDropIndexHandler(repo);
        LOGICAL_ALTER_RULE_HANDLER = new LogicalAlterRuleHandler(repo);
        LOGICAL_GENERIC_DDL_HANDLER = new LogicalGenericDdlHandler(repo);

        PHY_QUERY_HANDLER = new MyPhyQueryHandler(repo);
        BASE_DAL_HANDLER = new MyBaseDalHandler(repo);
        LOGICAL_SHOW_DATASOURCES_HANDLER = new LogicalShowDatasourcesHandler(repo);
        LOGICAL_SHOW_TABLES_HANDLER = new LogicalShowTablesMyHandler(repo);
        LOGICAL_SHOW_CREATE_DATABASE_HANDLER = new LogicalShowCreateDatabaseMyHandler(repo);
        LOGICAL_SHOW_CREATE_TABLES_HANDLER = new LogicalShowCreateTableHandler(repo);
        LOGICAL_SHOW_CREATE_VIEW_HANDLER = new LogicalShowCreateViewMyHandler(repo);
        LOGICAL_SHOW_PROCEDURE_STATUS_HANDLER = new LogicalShowProcedureStatusMyHandler(repo);
        LOGICAL_SHOW_FUNCTION_STATUS_HANDLER = new LogicalShowFunctionStatusMyHandler(repo);
        LOGICAL_SHOW_CREATE_PROCEDURE_HANDLER = new LogicalShowCreateProcedureHandler(repo);
        LOGICAL_SHOW_CREATE_FUNCTION_HANDLER = new LogicalShowCreateFunctionHandler(repo);
        LOGICAL_SHOW_VARIABLES_HANDLER = new LogicalShowVariablesMyHandler(repo);
        LOGICAL_SHOW_PROCESSLIST_HANDLER = new LogicalShowProcesslistHandler(repo);
        LOGICAL_SHOW_TABLE_STATUS_HANDLER = new LogicalShowTableStatusHandler(repo);
        LOGICAL_SHOW_SLOW_HANDLER = new LogicalShowSlowHandler(repo);
        LOGICAL_SHOW_STC_HANDLER = new LogicalShowStcHandler(repo);
        LOGICAL_SHOW_HTC_HANDLER = new LogicalShowHtcHandler(repo);
        LOGICAL_SHOW_PARTITIONS_HANDLER = new LogicalShowPartitionsHandler(repo);
        LOGICAL_SHOW_TOPOLOGY_HANDLER = new LogicalShowTopologyHandler(repo);
        LOGICAL_SHOW_FILES_HANDLER = new LogicalShowFilesHandler(repo);
        LOGICAL_SHOW_BROADCASTS_HANDLER = new LogicalShowBroadcastsHandler(repo);
        LOGICAL_SHOW_DS_HANDLER = new LogicalShowDsHandler(repo);
        LOGICAL_SHOW_DB_STATUS_HANDLER = new LogicalShowDbStatusHandler(repo);
        LOGICAL_SHOW_TRACE_HANDLER = new LogicalShowTraceHandler(repo);
        LOGICAL_SHOW_PRUNE_TRACE_HANDLER = new LogicalShowPruneTraceHandler(repo);
        LOGICAL_SHOW_SEQUENCES_HANDLER = new LogicalShowSequencesHandler(repo);
        LOGICAL_SHOW_RULE_HANDLER = new LogicalShowRuleHandler(repo);
        LOGICAL_SHOW_GRANTS_HANDLER = new PolarShowGrantsHandler(repo);
        LOGICAL_SHOW_STATS_HANDLER = new LogicalShowStatsHandler(repo);
        LOGICAL_SHOW_CHANGESET_STATS_HANDLER = new LogicalShowChangesetStatsHandler(repo);
        LOGICAL_SHOW_TABLE_REPLICATE_HANDLER = new LogicalShowTableReplicateHandler(repo);
        LOGICAL_SHOW_TABLE_ACCESS_HANDLER = new LogicalShowTableAccessHandler(repo);
        LOGICAL_SHOW_RULE_STATUS_HANDLER = new LogicalShowRuleStatusHandler(repo);
        LOGICAL_SHOW_INDEX_HANDLER = new LogicalShowIndexHandler(repo);
        LOGICAL_SHOW_PROFILE_HANDLER = new LogicalShowProfileHandler(repo);
        LOGICAL_SHOW_TABLE_INFO_HANDLER = new LogicalShowTableInfoHandler(repo);
        LOGICAL_SHOW_LOCALITY_INFO_HANDLER = new LogicalShowLocalityInfoHandler(repo);
        LOGICAL_SHOW_PHYSICAL_DDL_HANDLER = new LogicalShowPhysicalDdlHandler(repo);
        LOGICAL_SHOW_HOTKEY_HANDLER = new LogicalShowHotkeyHandler(repo);
        LOGICAL_DESC_HANDLER = new LogicalDescHandler(repo);
        LOGICAL_EXPLAIN_HANDLER = new LogicalExplainHandler(repo);
        LOGICAL_BASELINE_HANDLER = new LogicalBaselineHandler(repo);

        LOGICAL_CHECK_TABLE_HANDLER = new LogicalCheckTableHandler(repo);

        LOGICAL_CHECK_COLUMNAR_PARTITION_HANDLER = new LogicalCheckColumnarPartitionHandler(repo);

        LOGICAL_KILL_HANDLER = new LogicalKillHandler(repo);
        LOGICAL_ANALYZE_TABLE_HANDLER = new LogicalAnalyzeTableDdlHandler(repo);
        LOGICAL_SHOW_RECYCLEBIN_HANDLER = new LogicalShowRecyclebinHandler(repo);

        LOGICAL_CREATE_DATABASE_HANDLER = new LogicalCreateDatabaseHandler(repo);
        LOGICAL_CREATE_DATABASE_LIKE_AS_HANDLER = new LogicalCreateDatabaseLikeAsHandler(repo);
        LOGICAL_ALTER_DATABASE_HANDLER = new LogicalAlterDatabaseHandler(repo);
        LOGICAL_ALTER_INSTANCE_HANDLER = new LogicalAlterInstanceHandler(repo);
        LOGICAL_SHOW_CONVERT_TABLE_HANDLER = new LogicalShowConvertTableHandler(repo);
        LOGICAL_DROP_DATABASE_HANDLER = new LogicalDropDatabaseHandler(repo);

        LOGICAL_CREATE_JAVA_FUNCTION_HANDLER = new LogicalCreateJavaFunctionHandler(repo);
        LOGICAL_DROP_JAVA_FUNCTION_HANDLER = new LogicalDropJavaFunctionHandler(repo);

        LOGICAL_IMPORT_DATABASE = new LogicalImportDatabaseHandler(repo);

        LOGICAL_IMPORT_SEQUENCE = new LogicalImportSequenceHandler(repo);

        SHOW_DDL_JOBS_HANDLER = new DdlEngineShowJobsHandler(repo);
        RECOVER_DDL_JOBS_HANDLER = new DdlEngineRecoverJobsHandler(repo);
        CANCEL_DDL_JOBS_HANDLER = new DdlEngineCancelJobsHandler(repo);
        ROLLBACK_DDL_JOBS_HANDLER = new DdlEngineRollbackJobsHandler(repo);
        INSPECT_DDL_JOBS_CACHE_HANDLER = new DdlEngineInspectCacheHandler(repo);
        LOGICAL_CHECK_GSI_HANDLER = new LogicalCheckGsiHandler(repo);
        LOGICAL_CHECK_CCI_HANDLER = new LogicalCheckCciHandler(repo);

        PAUSE_DDL_JOBS_HANDLER = new DdlEnginePauseJobsHandler(repo);
        PAUSE_REBALANCE_JOBS_HANDLER = new DdlEnginePauseRebalanceHandler(repo);
        TERMINATE_REBALANCE_JOBS_HANDLER = new DdlEngineTerminateRebalanceHandler(repo);
        SKIP_REBALANCE_SUBJOB_HANDLER = new DdlEngineSkipRebalanceSubjobHandler(repo);
        CONTINUE_DDL_JOBS_HANDLER = new DdlEngineContinueJobsHandler(repo);
        SHOW_DDL_RESULTS_HANDLER = new DdlEngineShowResultsHandler(repo);
        SHOW_DDL_STATS_HANDLER = new DdlEngineShowDdlStatsHandler(repo);
        SHOW_REBALANCE_BACKFILL = new DdlEngineShowRebalanceBackFillHandler(repo);
        SHOW_SCHEDULE_RESULTS_HANDLER = new LogicalShowScheduleResultHandler(repo);

        LOGICAL_REBALANCE_HANDLER = new LogicalRebalanceHandler(repo);

        LOGICAL_CHANGE_CONSENSUS_LEADER_HANDLER = new LogicalChangeConsensusRoleHandler(repo);
        LOGICAL_ALTER_SYSTEM_SET_CONFIG_HANDLER = new LogicalAlterSystemSetConfigHandler(repo);

        LOGICAL_ALTER_SYSTEM_REFRESH_STORAGE_HANDLER = new LogicalAlterSystemRefreshStorageHandler(repo);
        LOGICAL_ALTER_SYSTEM_RELOAD_STORAGE_HANDLER = new LogicalAlterSystemReloadStorageHandler(repo);
        LOGICAL_ALTER_SYSTEM_LEADER_HANDLER = new LogicalAlterSystemLeaderHandler(repo);

        INSPECT_RULE_VERSION_HANDLER = new InspectRuleVersionHandler(repo);
        CLEAR_SEQ_CACHE_HANDLER = new ClearSeqCacheHandler(repo);
        INSPECT_GROUP_SEQ_RANGE_HANDLER = new InspectSeqRangeHandler(repo);
        CONVERT_ALL_SEQUENCES_HANDLER = new ConvertAllSequencesHandler(repo);
        SAVEPOINT_HANDLER = new SavepointHandler(repo);

        GSI_BACKFILL_HANDLER = new GsiBackfillHandler(repo);
        LOGICAL_MOVE_DATABASES_HANDLER = new LogicalMoveDatabaseHandler(repo);
        COLUMN_BACKFILL_HANDLER = new ColumnBackfillHandler(repo);
        SHOW_GLOBAL_INDEX_HANDLER = new ShowGlobalIndexHandler(repo);
        SHOW_COLUMNAR_INDEX_HANDLER = new ShowColumnarIndexHandler(repo);
        SHOW_METADATA_LOCK_HANDLER = new ShowMetadataLockHandler(repo);

        SHOW_TRANS_HANDLER = new ShowTransHandler(repo);
        SHOW_TRANS_STATS_HANDLER = new ShowTransStatsHandler(repo);

        LOGICAL_CREATE_VIEW_HANDLER = new LogicalCreateViewHandler(repo);
        LOGICAL_DROP_VIEW_HANDLER = new LogicalDropViewHandler(repo);
        VIRTUAL_VIEW_HANDLER = new VirtualViewHandler(repo);

        EMPTY_OPERATION_HANDLER = new EmptyOperationHandler(repo);
        SHOW_MOVE_DATABASE_HANDLER = new ShowMoveDatabaseHandler(repo);
        LOGICAL_SHOW_BINARY_LOGS_HANDLER = new LogicalShowBinaryLogsHandler(repo);
        LOGICAL_SHOW_MASTER_STATUS_HANDLER = new LogicalShowMasterStatusHandler(repo);
        LOGICAL_SHOW_BINLOG_EVENTS_HANDLER = new LogicalShowBinlogEventsHandler(repo);
        LOGICAL_SHOW_BINARY_STREAMS_HANDLER = new LogicalShowBinaryStreamsHandler(repo);
        LOGICAL_SHOW_CDC_STORAGE_HANDLER = new LogicalShowCdcStorageHandler(repo);
        LOGICAL_SET_CDC_GLOBAL_HANDLER = new LogicalSetCdcGlobalHandler(repo);
        LOGICAL_FLUSH_LOGS_HANDLER = new LogicalFlushLogsHandler(repo);
        LOGICAL_CHANGE_MASTER_HANDLER = new LogicalChangeMasterHandler(repo);
        LOGICAL_CHANGE_REPLICATION_FILTER_HANDLER = new LogicalChangeReplicationFilterHandler(repo);
        LOGICAL_SHOW_SLAVE_STATUS_HANDLER = new LogicalShowSlaveStatusHandler(repo);
        LOGICAL_START_SLAVE_HANDLER = new LogicalStartSlaveHandler(repo);
        LOGICAL_STOP_SLAVE_HANDLER = new LogicalStopSlaveHandler(repo);
        LOGICAL_RESET_SLAVE_HANDLER = new LogicalResetSlaveHandler(repo);
        LOGICAL_START_MASTER_HANDLER = new LogicalStartMasterHandler(repo);
        LOGICAL_STOP_MASTER_HANDLER = new LogicalStopMasterHandler(repo);
        LOGICAL_RESTART_MASTER_HANDLER = new LogicalRestartMasterHandler(repo);
        LOGICAL_REBALANCE_MASTER_HANDLER = new LogicalRebalanceMasterHandler(repo);
        LOGICAL_RESET_MASTER_HANDLER = new LogicalResetMasterHandler(repo);
        LOGICAL_REPLICA_HASHCHECK_HANDLER = new LogicalReplicaHashcheckHandler(repo);
        LOGICAL_START_REPLICA_CHECK_HANDLER = new LogicalStartReplicaCheckTableHandler(repo);
        LOGICAL_PAUSE_REPLICA_CHECK_HANDLER = new LogicalPauseReplicaCheckTableHandler(repo);
        LOGICAL_CANCEL_REPLICA_CHECK_HANDLER = new LogicalCancelReplicaCheckTableHandler(repo);
        LOGICAL_RESET_REPLICA_CHECK_HANDLER = new LogicalResetReplicaCheckTableHandler(repo);
        LOGICAL_CONTINUE_REPLICA_CHECK_HANDLER = new LogicalContinueReplicaCheckTableHandler(repo);
        LOGICAL_SHOW_REPLICA_CHECK_PROGRESS_HANDLER = new LogicalShowReplicaCheckProgressHandler(repo);
        LOGICAL_SHOW_REPLICA_CHECK_DIFF_HANDLER = new LogicalShowReplicaCheckDiffHandler(repo);

        CREATE_CCL_RULE_HANDLER = new LogicalCreateCclRuleHandler(repo);
        DROP_CCL_RULE_HANDLER = new LogicalDropCclRuleHandler(repo);
        SHOW_CCL_RULE_HANDLER = new LogicalShowCclRuleHandler(repo);
        CLEAR_CCL_RULES_HANDLER = new LogicalClearCclRulesHandler(repo);
        REBALANCE_HANDLER = new LogicalRebalanceHandler(repo);
        UNARCHIVE_HANDLER = new LogicalUnArchiveHandler(repo);

        CREATE_CCL_TRIGGER_HANDLER = new LogicalCreateCclTriggerHandler(repo);
        DROP_CCL_TRIGGER_HANDLER = new LogicalDropCclTriggerHandler(repo);
        SHOW_CCL_TRIGGER_HANDLER = new LogicalShowCclTriggerHandler(repo);
        CLEAR_CCL_TRIGGERS_HANDLER = new LogicalClearCclTriggersHandler(repo);
        SLOW_SQL_CCL_HANDLER = new LogicalSlowSqlCclHandler(repo);
        CREATE_SCHEDULE_HANDLER = new LogicalCreateScheduleHandler(repo);
        DROP_SCHEDULE_HANDLER = new LogicalDropScheduleHandler(repo);
        PAUSE_SCHEDULE_HANDLER = new LogicalPauseScheduleHandler(repo);
        CONTINUE_SCHEDULE_HANDLER = new LogicalContinueScheduleHandler(repo);
        FIRE_SCHEDULE_HANDLER = new LogicalFireScheduleHandler(repo);

        CREATE_SECURITY_LABEL_COMPONENT_HANDLER = new LogicalCreateSecurityLabelComponentHandler(repo);
        DROP_SECURITY_LABEL_COMPONENT_HANDLER = new LogicalDropSecurityLabelComponentHandler(repo);
        CREATE_SECURITY_LABEL_HANDLER = new LogicalCreateSecurityLabelHandler(repo);
        DROP_SECURITY_LABEL_HANDLER = new LogicalDropSecurityLabelHandler(repo);
        CREATE_SECURITY_POLICY_HANDLER = new LogicalCreateSecurityPolicyHandler(repo);
        DROP_SECURITY_POLICY_HANDLER = new LogicalDropSecurityPolicyHandler(repo);
        CREATE_SECURITY_ENTITY_HANDLER = new LogicalCreateSecurityEntityHandler(repo);
        DROP_SECURITY_ENTITY_HANDLER = new LogicalDropSecurityEntityHandler(repo);
        GRANT_SECURITY_LABEL_HANDLER = new LogicalGrantSecurityLabelHandler(repo);
        REVOKE_SECURITY_LABEL_HANDLER = new LogicalRevokeSecurityLabelHandler(repo);

        LOGICAL_SET_DEFAULT_ROLE_HANDLER = new LogicalSetDefaultRoleHandler(repo);
        ALTER_TABLEGROUP_BACKFILL_HANDLER = new AlterTableGroupBackfillHandler(repo);
        CREATE_TABLEGROUP_HANDLER = new LogicalCreateTableGroupHandler(repo);
        DROP_TABLEGROUP_HANDLER = new LogicalDropTableGroupHandler(repo);
        PHYSICAL_BACKFILL_HANDLER = new PhysicalBackfillHandler(repo);
        LOGICAL_OUT_FILE_HANDLER = new LogicalOutFileHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_SPLIT_PARTITION_HANDLER = new LogicalAlterTableGroupSplitPartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_MERGE_PARTITION_HANDLER = new LogicalAlterTableGroupMergePartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_MOVE_PARTITION_HANDLER = new LogicalAlterTableGroupMovePartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_EXTRACT_PARTITION_HANDLER = new LogicalAlterTableGroupExtractPartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_EXTRACT_PARTITION_PROXY_HANDLER =
            new LogicalAlterTableGroupExtractPartitionProxyHandler(repo);
        LOGICAL_ALTER_TABLE_EXTRACT_PARTITION_HANDLER = new LogicalAlterTableExtractPartitionHandler(repo);
        LOGICAL_ALTER_TABLE_EXTRACT_PARTITION_PROXY_HANDLER = new LogicalAlterTableExtractPartitionProxyHandler(repo);
        LOGICAL_ALTER_TABLE_SET_TABLEGROUP_HANDLER = new LogicalAlterTableSetTableGroupHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_RENAME_PARTITION_HANDLER = new LogicalAlterTableGroupRenamePartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_SET_LOCALITY_HANDLER = new LogicalAlterTableGroupSetLocalityHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_SET_PARTITIONS_LOCALITY_HANDLER =
            new LogicalAlterTableGroupSetPartitionsLocalityHandler(repo);
        LOGICAL_REPLICATE_BROADCAST_TABLE_HANDLER = new LogicalRefreshTopologyHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_ADD_PARTITION_PROXY_HANDLER = new LogicalAlterTableGroupAddPartitionProxyHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_DROP_PARTITION_HANDLER = new LogicalAlterTableGroupDropPartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_TRUNCATE_PARTITION_HANDLER = new LogicalAlterTableGroupTruncatePartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_REORGANIZE_PARTITION_HANDLER =
            new LogicalAlterTableGroupReorgPartitionHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_MODIFY_PARTITION_PROXY_HANDLER =
            new LogicalAlterTableGroupModifyPartitionProxyHandler(repo);
        MOVE_TABLE_BACKFILL_HANDLER = new MoveTableBackfillHandler(repo);
        LOGICAL_TABLE_DATA_MIGRATION_BACKFILL_HANDLER = new LogicalTableDataMigrationBackfillHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_SPLIT_PARTITION_BY_HOT_AVLUE_HANDLER =
            new LogicalAlterTableGroupSplitPartitionByHotValueHandler(repo);
        LOGICAL_ALTER_TABLE_SPLIT_PARTITION_BY_HOT_AVLUE_HANDLER =
            new LogicalAlterTableSplitPartitionByHotValueHandler(repo);

        LOGICAL_SHOW_GLOBAL_DEADLOCKS_HANDLER = new LogicalShowGlobalDeadlocksHandler(repo);
        LOGICAL_SHOW_LOCAL_DEADLOCKS_HANDLER = new LogicalShowLocalDeadlocksHandler(repo);
        LOGICAL_ALTER_TABLE_REPARTITION_HANDLER = new LogicalAlterTableRepartitionHandler(repo);

        LOGICAL_CREATE_STORAGE_POOL_HANDLER = new LogicalCreateStoragePoolHandler(repo);
        LOGICAL_ALTER_STORAGE_POOL_HANDLER = new LogicalAlterStoragePoolHandler(repo);
        LOGICAL_DROP_STORAGE_POOL_HANDLER = new LogicalDropStoragePoolHandler(repo);

        LOGICAL_SHOW_PARTITIONS_HEATMAP_HANDLER = new LogicalShowPartitionsHeatmapHandler(repo);
        CREATE_JOINGROUP_HANDLER = new LogicalCreateJoinGroupHandler(repo);
        LOGICAL_DROP_JOINGROUP_HANDLER = new LogicalDropJoinGroupHandler(repo);
        LOGICAL_ALTER_JOINGROUP_HANDLER = new LogicalAlterJoinGroupHandler(repo);
        LOGICAL_MERGE_TABLEGROUP_HANDLER = new LogicalMergeTableGroupHandler(repo);
        LOGICAL_ALTER_TABLEGROUP_ADD_TABLE_HANDLER = new LogicalAlterTableGroupAddTableHandler(repo);
        LOGICAL_OPTIMIZE_TABLE_HANDLER = new LogicalOptimizeTableHandler(repo);
        LOGICAL_ALTER_TABLE_SPLIT_PARTITION_HANDLER = new LogicalAlterTableSplitPartitionHandler(repo);
        LOGICAL_ALTER_TABLE_MERGE_PARTITION_HANDLER = new LogicalAlterTableMergePartitionHandler(repo);
        LOGICAL_ALTER_TABLE_MOVE_PARTITION_HANDLER = new LogicalAlterTableMovePartitionHandler(repo);
        LOGICAL_ALTER_TABLE_ADD_PARTITION_PROXY_HANDLER = new LogicalAlterTableAddPartitionProxyHandler(repo);
        LOGICAL_ALTER_TABLE_DROP_PARTITION_HANDLER = new LogicalAlterTableDropPartitionHandler(repo);
        LOGICAL_ALTER_TABLE_TRUNCATE_PARTITION_HANDLER = new LogicalAlterTableTruncatePartitionHandler(repo);
        LOGICAL_ALTER_TABLE_REORGANIZE_PARTITION_HANDLER = new LogicalAlterTableReorgPartitionHandler(repo);
        LOGICAL_ALTER_TABLE_MODIFY_PARTITION_PROXY_HANDLER = new LogicalAlterTableModifyPartitionProxyHandler(repo);
        LOGICAL_ALTER_TABLE_RENAME_PARTITION_HANDLER = new LogicalAlterTableRenamePartitionHandler(repo);
        LOGICAL_ALTER_TABLE_PARTITION_COUNT_HANDLER = new LogicalAlterTablePartitionCountHandler(repo);
        LOGICAL_ALTER_TABLE_REMOVE_PARTITIONING_HANDLER = new LogicalAlterTableRemovePartitioningHandler(repo);
        LOGICAL_PUSH_DOWN_UDF_HANDLER = new LogicalPushDownUdfHanlder(repo);
        LOGICAL_ALTER_PROCEDURE_HANDLER = new LogicalAlterProcedureHandler(repo);
        LOGICAL_ALTER_FUNCTION_HANDLER = new LogicalAlterFunctionHandler(repo);
        LOGICAL_CREATE_FUNCTION_HANDLER = new LogicalCreateFunctionHandler(repo);
        LOGICAL_DROP_FUNCTION_HANDLER = new LogicalDropFunctionHandler(repo);
        LOGICAL_CREATE_PROCEDURE_HANDLER = new LogicalCreateProcedureHandler(repo);
        LOGICAL_DROP_PROCEDURE_HANDLER = new LogicalDropProcedureHandler(repo);
        LOGICAL_CREATE_MATERIALIZED_VIEW = new LogicalCreateMaterializedViewHandler(repo);
        LOGICAL_INSPECT_INDEX_HANDLER = new LogicalInspectIndexHandler(repo);

        LOGICAL_DROP_MATERIALIZED_VIEW = new LogicalDropMaterializedViewHandler(repo);
        LOGICAL_SHOW_CREATE_TABLEGROUP_HANDLER = new LogicalShowCreateTableGroupHandler(repo);
    }

    private final LogicalRecyclebinHandler LOGICAL_RECYCLEBIN_HANDLER;

    private final LogicalCommonDdlHandler LOGICAL_CREATE_TABLE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_ALTER_TABLE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_ALTER_FILESTORAGE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_DROP_FILESTORAGE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_CLEAR_FILESTORAGE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_CREATE_FILESTORAGE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_ALTER_TABLE_REPARTITION_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_ALTER_TABLE_PARTITION_COUNT_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_ALTER_TABLE_REMOVE_PARTITIONING_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_RENAME_TABLE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_RENAME_TABLES_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_INSERT_OVERWRITE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_TRUNCATE_TABLE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_DROP_TABLE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_CREATE_INDEX_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_DROP_LOGICAL_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_GENERIC_DDL_HANDLER;

    private final LogicalCommonDdlHandler LOGICAL_PUSH_DOWN_UDF_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_ALTER_PROCEDURE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_ALTER_FUNCTION_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_CREATE_FUNCTION_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_DROP_FUNCTION_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_CREATE_PROCEDURE_HANDLER;
    private final LogicalCommonDdlHandler LOGICAL_DROP_PROCEDURE_HANDLER;
    private final PlanHandler LOGICAL_SEQUENCE_DDL_HANDLER;
    private final PlanHandler LOGICAL_ALTER_RULE_HANDLER;

    private final PlanHandler LOGICAL_INSPECT_INDEX_HANDLER;

    private final PlanHandler LOGICAL_GATHER_HANDLER;
    private final PlanHandler SINGLE_TABLE_SCAN_HANDLER;
    private final PlanHandler LOGICAL_VALUE_HANDLER;
    private final PlanHandler LOGICAL_MERGE_SORT_HANDLER;
    private final PlanHandler LOGICAL_VIEW_HANDLER;
    private final PlanHandler LOGICAL_MODIFY_VIEW_HANDLER;

    private final PlanHandler LOGICAL_CREATE_STORAGE_POOL_HANDLER;

    private final PlanHandler LOGICAL_ALTER_STORAGE_POOL_HANDLER;
    private final PlanHandler LOGICAL_DROP_STORAGE_POOL_HANDLER;
    private final PlanHandler LOGICAL_AFFECT_ROW_SUM_HANDLER;
    private final PlanHandler LOGICAL_INSERT_HANDLER;
    private final PlanHandler LOGICAL_LOAD_DATA_HANDLER;
    private final PlanHandler LOGICAL_INSERT_IGNORE_HANDLER;
    private final PlanHandler LOGICAL_REPLACE_HANDLER;
    private final PlanHandler LOGICAL_UPSERT_HANDLER;
    private final PlanHandler LOGICAL_MODIFY_HANDLER;
    private final PlanHandler LOGICAL_RELOCATE_HANDLER;
    private final PlanHandler SINGLE_TABLE_MODIFY_HANDLER;
    private final PlanHandler BROADCAST_TABLE_MODIFY_HANDLER;
    private final PlanHandler LOGICAL_SHOW_DATASOURCES_HANDLER;
    private final PlanHandler LOGICAL_SHOW_TABLES_HANDLER;
    private final PlanHandler LOGICAL_SHOW_CREATE_DATABASE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_CREATE_TABLES_HANDLER;
    private final PlanHandler LOGICAL_SHOW_CREATE_VIEW_HANDLER;
    private final PlanHandler LOGICAL_SHOW_PROCEDURE_STATUS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_FUNCTION_STATUS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_CREATE_PROCEDURE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_CREATE_FUNCTION_HANDLER;
    private final PlanHandler LOGICAL_SHOW_VARIABLES_HANDLER;
    private final PlanHandler LOGICAL_SHOW_PROCESSLIST_HANDLER;
    private final PlanHandler LOGICAL_SHOW_TABLE_STATUS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_SLOW_HANDLER;
    private final PlanHandler LOGICAL_SHOW_STC_HANDLER;
    private final PlanHandler LOGICAL_SHOW_HTC_HANDLER;
    private final PlanHandler LOGICAL_SHOW_PARTITIONS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_TOPOLOGY_HANDLER;
    private final PlanHandler LOGICAL_SHOW_FILES_HANDLER;
    private final PlanHandler LOGICAL_SHOW_BROADCASTS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_DS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_DB_STATUS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_TRACE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_PRUNE_TRACE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_SEQUENCES_HANDLER;
    private final PlanHandler LOGICAL_SHOW_RULE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_GRANTS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_STATS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_CHANGESET_STATS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_TABLE_REPLICATE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_TABLE_ACCESS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_RULE_STATUS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_INDEX_HANDLER;
    private final PlanHandler LOGICAL_SHOW_PROFILE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_TABLE_INFO_HANDLER;
    private final PlanHandler LOGICAL_SHOW_LOCALITY_INFO_HANDLER;

    private final PlanHandler LOGICAL_SHOW_PHYSICAL_DDL_HANDLER;
    private final PlanHandler LOGICAL_SHOW_HOTKEY_HANDLER;

    private final PlanHandler LOGICAL_DESC_HANDLER;

    private final PlanHandler LOGICAL_BASELINE_HANDLER;
    private final PlanHandler BASE_DAL_HANDLER;
    private final PlanHandler PHY_QUERY_HANDLER;

    private final PlanHandler LOGICAL_CHECK_TABLE_HANDLER;

    private final PlanHandler LOGICAL_CHECK_COLUMNAR_PARTITION_HANDLER;

    private final PlanHandler LOGICAL_KILL_HANDLER;
    private final PlanHandler LOGICAL_ANALYZE_TABLE_HANDLER;

    private final PlanHandler LOGICAL_IMPORT_DATABASE;

    private final PlanHandler LOGICAL_IMPORT_SEQUENCE;
    private final PlanHandler LOGICAL_SHOW_RECYCLEBIN_HANDLER;
    private final PlanHandler LOGICAL_EXPLAIN_HANDLER;

    // database
    private final PlanHandler LOGICAL_CREATE_DATABASE_HANDLER;
    private final PlanHandler LOGICAL_CREATE_DATABASE_LIKE_AS_HANDLER;

    private final PlanHandler LOGICAL_ALTER_DATABASE_HANDLER;
    private final PlanHandler LOGICAL_ALTER_INSTANCE_HANDLER;

    private final PlanHandler LOGICAL_SHOW_CONVERT_TABLE_HANDLER;

    private final PlanHandler LOGICAL_CONVERT_TABLE_MODE_HANDLER;
    private final PlanHandler LOGICAL_DROP_DATABASE_HANDLER;

    private final PlanHandler LOGICAL_CREATE_JAVA_FUNCTION_HANDLER;
    private final PlanHandler LOGICAL_DROP_JAVA_FUNCTION_HANDLER;

    private final PlanHandler SHOW_DDL_JOBS_HANDLER;
    private final PlanHandler CANCEL_DDL_JOBS_HANDLER;
    private final PlanHandler ROLLBACK_DDL_JOBS_HANDLER;
    private final PlanHandler INSPECT_DDL_JOBS_CACHE_HANDLER;

    private final PlanHandler PAUSE_DDL_JOBS_HANDLER;
    private final PlanHandler PAUSE_REBALANCE_JOBS_HANDLER;
    private final PlanHandler TERMINATE_REBALANCE_JOBS_HANDLER;
    private final PlanHandler SKIP_REBALANCE_SUBJOB_HANDLER;
    private final PlanHandler CONTINUE_DDL_JOBS_HANDLER;
    private final PlanHandler SHOW_DDL_RESULTS_HANDLER;
    private final PlanHandler SHOW_DDL_STATS_HANDLER;
    private final PlanHandler SHOW_REBALANCE_BACKFILL;
    private final PlanHandler SHOW_SCHEDULE_RESULTS_HANDLER;

    private final PlanHandler RECOVER_DDL_JOBS_HANDLER;

    private final PlanHandler LOGICAL_REBALANCE_HANDLER;
    private final PlanHandler LOGICAL_CHANGE_CONSENSUS_LEADER_HANDLER;
    private final PlanHandler LOGICAL_ALTER_SYSTEM_SET_CONFIG_HANDLER;

    private final PlanHandler LOGICAL_ALTER_SYSTEM_REFRESH_STORAGE_HANDLER;
    private final PlanHandler LOGICAL_ALTER_SYSTEM_RELOAD_STORAGE_HANDLER;
    private final PlanHandler LOGICAL_ALTER_SYSTEM_LEADER_HANDLER;
    private final PlanHandler INSPECT_RULE_VERSION_HANDLER;
    private final PlanHandler CLEAR_SEQ_CACHE_HANDLER;
    private final PlanHandler INSPECT_GROUP_SEQ_RANGE_HANDLER;
    private final PlanHandler CONVERT_ALL_SEQUENCES_HANDLER;
    private final PlanHandler SAVEPOINT_HANDLER;

    private final PlanHandler GSI_BACKFILL_HANDLER;
    private final PlanHandler LOGICAL_CHECK_GSI_HANDLER;
    private final PlanHandler LOGICAL_CHECK_CCI_HANDLER;
    private final PlanHandler COLUMN_BACKFILL_HANDLER;
    private final PlanHandler SHOW_GLOBAL_INDEX_HANDLER;
    private final PlanHandler SHOW_COLUMNAR_INDEX_HANDLER;
    private final PlanHandler SHOW_METADATA_LOCK_HANDLER;
    private final PlanHandler SHOW_TRANS_HANDLER;

    private final PlanHandler SHOW_TRANS_STATS_HANDLER;

    private final PlanHandler LOGICAL_CREATE_VIEW_HANDLER;
    private final PlanHandler LOGICAL_DROP_VIEW_HANDLER;
    private final PlanHandler VIRTUAL_VIEW_HANDLER;

    private final PlanHandler EMPTY_OPERATION_HANDLER;
    private final PlanHandler SHOW_MOVE_DATABASE_HANDLER;
    private final PlanHandler LOGICAL_SHOW_BINARY_LOGS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_MASTER_STATUS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_BINLOG_EVENTS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_BINARY_STREAMS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_CDC_STORAGE_HANDLER;
    private final PlanHandler LOGICAL_SET_CDC_GLOBAL_HANDLER;
    private final PlanHandler LOGICAL_FLUSH_LOGS_HANDLER;
    private final PlanHandler LOGICAL_CHANGE_MASTER_HANDLER;
    private final PlanHandler LOGICAL_CHANGE_REPLICATION_FILTER_HANDLER;
    private final PlanHandler LOGICAL_SHOW_SLAVE_STATUS_HANDLER;
    private final PlanHandler LOGICAL_START_SLAVE_HANDLER;
    private final PlanHandler LOGICAL_STOP_SLAVE_HANDLER;
    private final PlanHandler LOGICAL_RESET_SLAVE_HANDLER;
    private final PlanHandler LOGICAL_START_MASTER_HANDLER;
    private final PlanHandler LOGICAL_STOP_MASTER_HANDLER;
    private final PlanHandler LOGICAL_RESTART_MASTER_HANDLER;
    private final PlanHandler LOGICAL_REBALANCE_MASTER_HANDLER;
    private final PlanHandler LOGICAL_RESET_MASTER_HANDLER;
    private final PlanHandler LOGICAL_REPLICA_HASHCHECK_HANDLER;
    private final PlanHandler LOGICAL_START_REPLICA_CHECK_HANDLER;
    private final PlanHandler LOGICAL_PAUSE_REPLICA_CHECK_HANDLER;
    private final PlanHandler LOGICAL_CONTINUE_REPLICA_CHECK_HANDLER;
    private final PlanHandler LOGICAL_CANCEL_REPLICA_CHECK_HANDLER;
    private final PlanHandler LOGICAL_RESET_REPLICA_CHECK_HANDLER;
    private final PlanHandler LOGICAL_SHOW_REPLICA_CHECK_PROGRESS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_REPLICA_CHECK_DIFF_HANDLER;

    private final PlanHandler CREATE_CCL_RULE_HANDLER;
    private final PlanHandler DROP_CCL_RULE_HANDLER;
    private final PlanHandler SHOW_CCL_RULE_HANDLER;
    private final PlanHandler CLEAR_CCL_RULES_HANDLER;
    private final PlanHandler ALTER_TABLEGROUP_BACKFILL_HANDLER;
    private final PlanHandler PHYSICAL_BACKFILL_HANDLER;
    private final PlanHandler CREATE_TABLEGROUP_HANDLER;
    private final PlanHandler DROP_TABLEGROUP_HANDLER;

    private final PlanHandler REBALANCE_HANDLER;
    private final PlanHandler UNARCHIVE_HANDLER;

    private final PlanHandler CREATE_CCL_TRIGGER_HANDLER;
    private final PlanHandler DROP_CCL_TRIGGER_HANDLER;
    private final PlanHandler SHOW_CCL_TRIGGER_HANDLER;
    private final PlanHandler CLEAR_CCL_TRIGGERS_HANDLER;
    private final PlanHandler SLOW_SQL_CCL_HANDLER;
    private final PlanHandler CREATE_SCHEDULE_HANDLER;
    private final PlanHandler DROP_SCHEDULE_HANDLER;
    private final PlanHandler PAUSE_SCHEDULE_HANDLER;
    private final PlanHandler CONTINUE_SCHEDULE_HANDLER;
    private final PlanHandler FIRE_SCHEDULE_HANDLER;
    private final PlanHandler CREATE_SECURITY_LABEL_COMPONENT_HANDLER;
    private final PlanHandler DROP_SECURITY_LABEL_COMPONENT_HANDLER;
    private final PlanHandler CREATE_SECURITY_LABEL_HANDLER;
    private final PlanHandler DROP_SECURITY_LABEL_HANDLER;
    private final PlanHandler CREATE_SECURITY_POLICY_HANDLER;
    private final PlanHandler DROP_SECURITY_POLICY_HANDLER;
    private final PlanHandler CREATE_SECURITY_ENTITY_HANDLER;
    private final PlanHandler DROP_SECURITY_ENTITY_HANDLER;
    private final PlanHandler GRANT_SECURITY_LABEL_HANDLER;
    private final PlanHandler REVOKE_SECURITY_LABEL_HANDLER;

    private final PlanHandler LOGICAL_SET_DEFAULT_ROLE_HANDLER;

    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_SPLIT_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_MERGE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_MOVE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_EXTRACT_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_EXTRACT_PARTITION_PROXY_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_SET_TABLEGROUP_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_RENAME_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_SET_LOCALITY_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_SET_PARTITIONS_LOCALITY_HANDLER;
    private final PlanHandler LOGICAL_REPLICATE_BROADCAST_TABLE_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_ADD_PARTITION_PROXY_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_DROP_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_TRUNCATE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_REORGANIZE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_MODIFY_PARTITION_PROXY_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_SPLIT_PARTITION_BY_HOT_AVLUE_HANDLER;

    private final PlanHandler LOGICAL_OUT_FILE_HANDLER;
    private final PlanHandler LOGICAL_MOVE_DATABASES_HANDLER;
    private final PlanHandler MOVE_TABLE_BACKFILL_HANDLER;
    private final PlanHandler LOGICAL_TABLE_DATA_MIGRATION_BACKFILL_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_SPLIT_PARTITION_BY_HOT_AVLUE_HANDLER;

    private final PlanHandler LOGICAL_SHOW_GLOBAL_DEADLOCKS_HANDLER;
    private final PlanHandler LOGICAL_SHOW_LOCAL_DEADLOCKS_HANDLER;

    private final PlanHandler LOGICAL_SHOW_PARTITIONS_HEATMAP_HANDLER;

    private final PlanHandler CREATE_JOINGROUP_HANDLER;
    private final PlanHandler LOGICAL_DROP_JOINGROUP_HANDLER;
    private final PlanHandler LOGICAL_ALTER_JOINGROUP_HANDLER;
    private final PlanHandler LOGICAL_MERGE_TABLEGROUP_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLEGROUP_ADD_TABLE_HANDLER;
    private final PlanHandler LOGICAL_OPTIMIZE_TABLE_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_EXTRACT_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_EXTRACT_PARTITION_PROXY_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_SPLIT_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_MERGE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_MOVE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_ADD_PARTITION_PROXY_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_DROP_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_TRUNCATE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_REORGANIZE_PARTITION_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_MODIFY_PARTITION_PROXY_HANDLER;
    private final PlanHandler LOGICAL_ALTER_TABLE_RENAME_PARTITION_HANDLER;

    private final PlanHandler LOGICAL_CREATE_MATERIALIZED_VIEW;
    private final PlanHandler LOGICAL_DROP_MATERIALIZED_VIEW;

    private final PlanHandler LOGICAL_SHOW_CREATE_TABLEGROUP_HANDLER;

    @Override
    public PlanHandler getCommandHandler(RelNode logicalPlan, ExecutionContext executionContext) {
        if (executionContext.getExplain() != null
            && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE) {
            if (logicalPlan instanceof LogicalView) {
                return LOGICAL_EXPLAIN_HANDLER;
            } else if (logicalPlan instanceof BaseTableOperation) {
                SqlKind kind = ((BaseTableOperation) logicalPlan).getKind();
                if (kind != SqlKind.SELECT) {
                    return LOGICAL_EXPLAIN_HANDLER;
                }
            }
        }

        if (logicalPlan instanceof BaseTableOperation) {
            SqlKind kind = ((BaseTableOperation) logicalPlan).getKind();
            if (kind == SqlKind.SELECT) {
                return SINGLE_TABLE_SCAN_HANDLER;
            } else if (SqlKind.SEQUENCE_DDL.contains(kind)) {
                return LOGICAL_SEQUENCE_DDL_HANDLER;
            } else if (SqlKind.SUPPORT_DDL.contains(kind)) {
                return SINGLE_TABLE_SCAN_HANDLER;
            } else {
                return SINGLE_TABLE_MODIFY_HANDLER;
            }
        } else if (logicalPlan instanceof LogicalValues) {
            return LOGICAL_VALUE_HANDLER;
        } else if (logicalPlan instanceof Gather) {
            return LOGICAL_GATHER_HANDLER;
        } else if (logicalPlan instanceof PhyViewUnion) {
            return LOGICAL_GATHER_HANDLER;
        } else if (logicalPlan instanceof MergeSort) {
            return LOGICAL_MERGE_SORT_HANDLER;
        } else if (logicalPlan instanceof LogicalModifyView) {
            return LOGICAL_MODIFY_VIEW_HANDLER;
        } else if (logicalPlan instanceof LogicalView) {
            return LOGICAL_VIEW_HANDLER;
        } else if (logicalPlan instanceof VirtualView) {
            return VIRTUAL_VIEW_HANDLER;
        } else if (logicalPlan instanceof AffectedRowsSum) {
            return LOGICAL_AFFECT_ROW_SUM_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateMaterializedView) {
            return LOGICAL_CREATE_MATERIALIZED_VIEW;
        } else if (logicalPlan instanceof LogicalDropMaterializedView) {
            return LOGICAL_DROP_MATERIALIZED_VIEW;
        } else if (logicalPlan instanceof LogicalReplace) {
            if (executionContext.getLoadDataContext() != null) {
                return LOGICAL_LOAD_DATA_HANDLER;
            } else {
                return LOGICAL_REPLACE_HANDLER;
            }
        } else if (logicalPlan instanceof LogicalUpsert) {
            return LOGICAL_UPSERT_HANDLER;
        } else if (logicalPlan instanceof LogicalInsertIgnore) {
            if (executionContext.getLoadDataContext() != null) {
                return LOGICAL_LOAD_DATA_HANDLER;
            } else {
                return LOGICAL_INSERT_IGNORE_HANDLER;
            }
        } else if (logicalPlan instanceof LogicalInsert) {
            if (executionContext.getLoadDataContext() != null) {
                return LOGICAL_LOAD_DATA_HANDLER;
            } else {
                return LOGICAL_INSERT_HANDLER;
            }
        } else if (logicalPlan instanceof LogicalModify) {
            return LOGICAL_MODIFY_HANDLER;
        } else if (logicalPlan instanceof LogicalRelocate) {
            return LOGICAL_RELOCATE_HANDLER;
        } else if (logicalPlan instanceof BroadcastTableModify) {
            return BROADCAST_TABLE_MODIFY_HANDLER;
        } else if (logicalPlan instanceof LogicalRecyclebin) {
            return LOGICAL_RECYCLEBIN_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateDatabase) {
            SqlNode sqlNode = ((LogicalCreateDatabase) logicalPlan).relDdl.sqlNode;
            if (sqlNode instanceof SqlCreateDatabase) {
                SqlCreateDatabase sqlCreateDatabase = (SqlCreateDatabase) sqlNode;
                //only need to show table conversion sql
                if (sqlCreateDatabase.isDryRun()) {
                    return LOGICAL_SHOW_CONVERT_TABLE_HANDLER;
                }
                if (sqlCreateDatabase.getLike() || sqlCreateDatabase.getAs()) {
                    return LOGICAL_CREATE_DATABASE_LIKE_AS_HANDLER;
                }
            }
            return LOGICAL_CREATE_DATABASE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterDatabase) {
            return LOGICAL_ALTER_DATABASE_HANDLER;
        } else if (logicalPlan instanceof LogicalDropDatabase) {
            return LOGICAL_DROP_DATABASE_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateJavaFunction) {
            return LOGICAL_CREATE_JAVA_FUNCTION_HANDLER;
        } else if (logicalPlan instanceof LogicalDropJavaFunction) {
            return LOGICAL_DROP_JAVA_FUNCTION_HANDLER;
        } else if (logicalPlan instanceof LogicalRebalance) {
            return LOGICAL_REBALANCE_HANDLER;
        } else if (logicalPlan instanceof LogicalChangeConsensusLeader) {
            return LOGICAL_CHANGE_CONSENSUS_LEADER_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterSystemSetConfig) {
            return LOGICAL_ALTER_SYSTEM_SET_CONFIG_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterSystemRefreshStorage) {
            return LOGICAL_ALTER_SYSTEM_REFRESH_STORAGE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterSystemReloadStorage) {
            return LOGICAL_ALTER_SYSTEM_RELOAD_STORAGE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterSystemLeader) {
            return LOGICAL_ALTER_SYSTEM_LEADER_HANDLER;
        } else if (logicalPlan instanceof GsiBackfill) {
            return GSI_BACKFILL_HANDLER;
        } else if (logicalPlan instanceof ColumnBackFill) {
            return COLUMN_BACKFILL_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateView) {
            return LOGICAL_CREATE_VIEW_HANDLER;
        } else if (logicalPlan instanceof LogicalDropView) {
            return LOGICAL_DROP_VIEW_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateTable) {
            LogicalCreateTable logicalCreateTable = (LogicalCreateTable) logicalPlan;
            if (logicalCreateTable.getSqlCreateTable() != null && logicalCreateTable.getSqlCreateTable()
                .isOnlyConvertTableMode()) {
                return LOGICAL_CONVERT_TABLE_MODE_HANDLER;

            } else {
                return LOGICAL_CREATE_TABLE_HANDLER;
            }
        } else if (logicalPlan instanceof LogicalAlterTable) {
            return LOGICAL_ALTER_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTablePartitionCount) {
            return LOGICAL_ALTER_TABLE_PARTITION_COUNT_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableRemovePartitioning) {
            return LOGICAL_ALTER_TABLE_REMOVE_PARTITIONING_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableRepartition) {
            return LOGICAL_ALTER_TABLE_REPARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalRenameTable) {
            return LOGICAL_RENAME_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalRenameTables) {
            return LOGICAL_RENAME_TABLES_HANDLER;
        } else if (logicalPlan instanceof LogicalInsertOverwrite) {
            return LOGICAL_INSERT_OVERWRITE_HANDLER;
        } else if (logicalPlan instanceof LogicalTruncateTable) {
            return LOGICAL_TRUNCATE_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalDropTable) {
            return LOGICAL_DROP_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateIndex) {
            return LOGICAL_CREATE_INDEX_HANDLER;
        } else if (logicalPlan instanceof LogicalDropIndex) {
            return LOGICAL_DROP_LOGICAL_HANDLER;
        } else if (logicalPlan instanceof LogicalSequenceDdl) {
            return LOGICAL_SEQUENCE_DDL_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterRule) {
            return LOGICAL_ALTER_RULE_HANDLER;
        } else if (logicalPlan instanceof LogicalGenericDdl) {
            return LOGICAL_GENERIC_DDL_HANDLER;
        } else if (logicalPlan instanceof LogicalCheckGsi) {
            return LOGICAL_CHECK_GSI_HANDLER;
        } else if (logicalPlan instanceof LogicalCheckCci) {
            return LOGICAL_CHECK_CCI_HANDLER;
        } else if (logicalPlan instanceof AlterTableGroupBackfill) {
            return ALTER_TABLEGROUP_BACKFILL_HANDLER;
        } else if (logicalPlan instanceof PhysicalBackfill) {
            return PHYSICAL_BACKFILL_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterFileStorage) {
            return LOGICAL_ALTER_FILESTORAGE_HANDLER;
        } else if (logicalPlan instanceof LogicalDropFileStorage) {
            return LOGICAL_DROP_FILESTORAGE_HANDLER;
        } else if (logicalPlan instanceof LogicalClearFileStorage) {
            return LOGICAL_CLEAR_FILESTORAGE_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateFileStorage) {
            return LOGICAL_CREATE_FILESTORAGE_HANDLER;
        } else if (logicalPlan instanceof PhyQueryOperation) {
            return PHY_QUERY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupSplitPartition) {
            return LOGICAL_ALTER_TABLEGROUP_SPLIT_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableSplitPartition
            && !(logicalPlan instanceof LogicalAlterTableGroupSplitPartition)) {
            return LOGICAL_ALTER_TABLE_SPLIT_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupMergePartition) {
            return LOGICAL_ALTER_TABLEGROUP_MERGE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableMergePartition
            && !(logicalPlan instanceof LogicalAlterTableGroupMergePartition)) {
            return LOGICAL_ALTER_TABLE_MERGE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupMovePartition) {
            return LOGICAL_ALTER_TABLEGROUP_MOVE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableMovePartition
            && !(logicalPlan instanceof LogicalAlterTableGroupMovePartition)) {
            return LOGICAL_ALTER_TABLE_MOVE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupExtractPartition) {
            return LOGICAL_ALTER_TABLEGROUP_EXTRACT_PARTITION_PROXY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableExtractPartition
            && !(logicalPlan instanceof LogicalAlterTableGroupExtractPartition)) {
            return LOGICAL_ALTER_TABLE_EXTRACT_PARTITION_PROXY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableSetTableGroup) {
            return LOGICAL_ALTER_TABLE_SET_TABLEGROUP_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupRenamePartition) {
            return LOGICAL_ALTER_TABLEGROUP_RENAME_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableRenamePartition
            && !(logicalPlan instanceof LogicalAlterTableGroupRenamePartition)) {
            return LOGICAL_ALTER_TABLE_RENAME_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupSetLocality) {
            return LOGICAL_ALTER_TABLEGROUP_SET_LOCALITY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupSetPartitionsLocality) {
            return LOGICAL_ALTER_TABLEGROUP_SET_PARTITIONS_LOCALITY_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateStoragePool) {
            return LOGICAL_CREATE_STORAGE_POOL_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterStoragePool) {
            return LOGICAL_ALTER_STORAGE_POOL_HANDLER;
        } else if (logicalPlan instanceof LogicalDropStoragePool) {
            return LOGICAL_DROP_STORAGE_POOL_HANDLER;
        } else if (logicalPlan instanceof LogicalRefreshTopology) {
            return LOGICAL_REPLICATE_BROADCAST_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupAddPartition) {
            return LOGICAL_ALTER_TABLEGROUP_ADD_PARTITION_PROXY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableAddPartition
            && !(logicalPlan instanceof LogicalAlterTableGroupAddPartition)) {
            return LOGICAL_ALTER_TABLE_ADD_PARTITION_PROXY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupDropPartition) {
            return LOGICAL_ALTER_TABLEGROUP_DROP_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableDropPartition
            && !(logicalPlan instanceof LogicalAlterTableGroupDropPartition)) {
            return LOGICAL_ALTER_TABLE_DROP_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupTruncatePartition) {
            return LOGICAL_ALTER_TABLEGROUP_TRUNCATE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableTruncatePartition
            && !(logicalPlan instanceof LogicalAlterTableGroupTruncatePartition)) {
            return LOGICAL_ALTER_TABLE_TRUNCATE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupReorgPartition) {
            return LOGICAL_ALTER_TABLEGROUP_REORGANIZE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableReorgPartition
            && !(logicalPlan instanceof LogicalAlterTableGroupReorgPartition)) {
            return LOGICAL_ALTER_TABLE_REORGANIZE_PARTITION_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupModifyPartition) {
            return LOGICAL_ALTER_TABLEGROUP_MODIFY_PARTITION_PROXY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableModifyPartition
            && !(logicalPlan instanceof LogicalAlterTableGroupModifyPartition)) {
            return LOGICAL_ALTER_TABLE_MODIFY_PARTITION_PROXY_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableSplitPartitionByHotValue
            && !(logicalPlan instanceof LogicalAlterTableGroupSplitPartitionByHotValue)) {
            return LOGICAL_ALTER_TABLE_SPLIT_PARTITION_BY_HOT_AVLUE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupSplitPartitionByHotValue) {
            return LOGICAL_ALTER_TABLEGROUP_SPLIT_PARTITION_BY_HOT_AVLUE_HANDLER;
        } else if (logicalPlan instanceof LogicalUnArchive) {
            return UNARCHIVE_HANDLER;
        } else if (logicalPlan instanceof LogicalPushDownUdf) {
            return LOGICAL_PUSH_DOWN_UDF_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateFunction) {
            return LOGICAL_CREATE_FUNCTION_HANDLER;
        } else if (logicalPlan instanceof LogicalDropFunction) {
            return LOGICAL_DROP_FUNCTION_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateProcedure) {
            return LOGICAL_CREATE_PROCEDURE_HANDLER;
        } else if (logicalPlan instanceof LogicalDropProcedure) {
            return LOGICAL_DROP_PROCEDURE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterProcedure) {
            return LOGICAL_ALTER_PROCEDURE_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterFunction) {
            return LOGICAL_ALTER_FUNCTION_HANDLER;
        } else if (logicalPlan instanceof LogicalConvertAllSequences) {
            return CONVERT_ALL_SEQUENCES_HANDLER;
        } else if (logicalPlan instanceof BaseDalOperation) {

            if (logicalPlan instanceof PhyShow) {
                return BASE_DAL_HANDLER;
            }

            switch (((BaseDalOperation) logicalPlan).kind()) {
            case SHOW_DATASOURCES:
                return LOGICAL_SHOW_DATASOURCES_HANDLER;
            case SHOW_TABLES:
                return LOGICAL_SHOW_TABLES_HANDLER;
            case SHOW_TABLE_INFO:
                return LOGICAL_SHOW_TABLE_INFO_HANDLER;
            case SHOW_LOCALITY_INFO:
                return LOGICAL_SHOW_LOCALITY_INFO_HANDLER;
            case SHOW_PHYSICAL_DDL:
                return LOGICAL_SHOW_PHYSICAL_DDL_HANDLER;
            case SHOW_HOTKEY:
                return LOGICAL_SHOW_HOTKEY_HANDLER;
            case SHOW_CREATE_DATABASE:
                return LOGICAL_SHOW_CREATE_DATABASE_HANDLER;
            case SHOW_CREATE_TABLE:
                return LOGICAL_SHOW_CREATE_TABLES_HANDLER;
            case SHOW_CREATE_VIEW:
                return LOGICAL_SHOW_CREATE_VIEW_HANDLER;
            case SHOW_PROCEDURE_STATUS:
                return LOGICAL_SHOW_PROCEDURE_STATUS_HANDLER;
            case SHOW_FUNCTION_STATUS:
                return LOGICAL_SHOW_FUNCTION_STATUS_HANDLER;
            case SHOW_CREATE_PROCEDURE:
                return LOGICAL_SHOW_CREATE_PROCEDURE_HANDLER;
            case SHOW_CREATE_FUNCTION:
                return LOGICAL_SHOW_CREATE_FUNCTION_HANDLER;
            case SHOW_VARIABLES:
                return LOGICAL_SHOW_VARIABLES_HANDLER;
            case SHOW_PROCESSLIST:
                return LOGICAL_SHOW_PROCESSLIST_HANDLER;
            case SHOW_TABLE_STATUS:
                return LOGICAL_SHOW_TABLE_STATUS_HANDLER;
            case SHOW_SLOW:
                return LOGICAL_SHOW_SLOW_HANDLER;
            case SHOW_STC:
                return LOGICAL_SHOW_STC_HANDLER;
            case SHOW_HTC:
                return LOGICAL_SHOW_HTC_HANDLER;
            case SHOW_PARTITIONS:
                return LOGICAL_SHOW_PARTITIONS_HANDLER;
            case SHOW_TOPOLOGY:
                return LOGICAL_SHOW_TOPOLOGY_HANDLER;
            case SHOW_FILES:
                return LOGICAL_SHOW_FILES_HANDLER;
            case SHOW_BROADCASTS:
                return LOGICAL_SHOW_BROADCASTS_HANDLER;
            case SHOW_DS:
                return LOGICAL_SHOW_DS_HANDLER;
            case SHOW_DB_STATUS:
                return LOGICAL_SHOW_DB_STATUS_HANDLER;
            case SHOW_STATS:
                return LOGICAL_SHOW_STATS_HANDLER;
            case SHOW_CHANGESET_STATS:
                return LOGICAL_SHOW_CHANGESET_STATS_HANDLER;
            case SHOW_TABLE_REPLICATE:
                return LOGICAL_SHOW_TABLE_REPLICATE_HANDLER;
            case SHOW_TABLE_ACCESS:
                return LOGICAL_SHOW_TABLE_ACCESS_HANDLER;
            case SHOW_TRACE:
                return LOGICAL_SHOW_TRACE_HANDLER;
            case SHOW_PRUNE_TRACE:
                return LOGICAL_SHOW_PRUNE_TRACE_HANDLER;
            case SHOW_SEQUENCES:
                return LOGICAL_SHOW_SEQUENCES_HANDLER;
            case SHOW_RULE:
                return LOGICAL_SHOW_RULE_HANDLER;
            case SHOW_GRANTS:
                return LOGICAL_SHOW_GRANTS_HANDLER;
            case SHOW_RULE_STATUS:
                return LOGICAL_SHOW_RULE_STATUS_HANDLER;
            case SHOW_PROFILE:
            case SHOW_PROFILES:
                return LOGICAL_SHOW_PROFILE_HANDLER;
            case DESCRIBE_COLUMNS:
                return LOGICAL_DESC_HANDLER;
            case CHECK_TABLE:
                return LOGICAL_CHECK_TABLE_HANDLER;
            case CHECK_COLUMNAR_PARTITION:
                return LOGICAL_CHECK_COLUMNAR_PARTITION_HANDLER;
            case KILL:
                return LOGICAL_KILL_HANDLER;
            case ANALYZE_TABLE:
                return LOGICAL_ANALYZE_TABLE_HANDLER;
            case SHOW_RECYCLEBIN:
                return LOGICAL_SHOW_RECYCLEBIN_HANDLER;
            case SHOW_INDEX:
                return LOGICAL_SHOW_INDEX_HANDLER;
            case SHOW_DDL_JOBS:
                return SHOW_DDL_JOBS_HANDLER;
            case SHOW_DDL_STATUS:
                return SHOW_DDL_STATS_HANDLER;
            case SHOW_REBALANCE_BACKFILL:
                return SHOW_REBALANCE_BACKFILL;
            case SHOW_DDL_RESULTS:
                return SHOW_DDL_RESULTS_HANDLER;
            case SHOW_SCHEDULE_RESULTS:
                return SHOW_SCHEDULE_RESULTS_HANDLER;
            case CANCEL_DDL_JOB:
                return CANCEL_DDL_JOBS_HANDLER;
            case RECOVER_DDL_JOB:
                return RECOVER_DDL_JOBS_HANDLER;
            case CONTINUE_DDL_JOB:
                return CONTINUE_DDL_JOBS_HANDLER;
            case PAUSE_DDL_JOB:
                return PAUSE_DDL_JOBS_HANDLER;
            case PAUSE_REBALANCE_JOB:
                return PAUSE_REBALANCE_JOBS_HANDLER;
            case TERMINATE_REBALANCE_JOB:
                return TERMINATE_REBALANCE_JOBS_HANDLER;
            case ROLLBACK_DDL_JOB:
                return ROLLBACK_DDL_JOBS_HANDLER;
            case SKIP_REBALANCE_SUBJOB:
                return SKIP_REBALANCE_SUBJOB_HANDLER;
            case INSPECT_DDL_JOB_CACHE:
                return INSPECT_DDL_JOBS_CACHE_HANDLER;
            case INSPECT_RULE_VERSION:
                return INSPECT_RULE_VERSION_HANDLER;
            case CLEAR_SEQ_CACHE:
                return CLEAR_SEQ_CACHE_HANDLER;
            case INSPECT_SEQ_RANGE:
                return INSPECT_GROUP_SEQ_RANGE_HANDLER;
            case BASELINE:
                return LOGICAL_BASELINE_HANDLER;
            case SHOW_GLOBAL_INDEX:
                return SHOW_GLOBAL_INDEX_HANDLER;
            case SHOW_COLUMNAR_INDEX:
                return SHOW_COLUMNAR_INDEX_HANDLER;
            case SHOW_METADATA_LOCK:
                return SHOW_METADATA_LOCK_HANDLER;
            case SHOW_TRANS:
                return SHOW_TRANS_HANDLER;
            case SHOW_TRANS_STATS:
                return SHOW_TRANS_STATS_HANDLER;
            case SAVEPOINT:
                return SAVEPOINT_HANDLER;
            case SHOW_MOVE_DATABASE:
                return SHOW_MOVE_DATABASE_HANDLER;
            case SHOW_BINARY_LOGS:
                return LOGICAL_SHOW_BINARY_LOGS_HANDLER;
            case SHOW_MASTER_STATUS:
                return LOGICAL_SHOW_MASTER_STATUS_HANDLER;
            case SHOW_BINLOG_EVENTS:
                return LOGICAL_SHOW_BINLOG_EVENTS_HANDLER;
            case SHOW_BINARY_STREAMS:
                return LOGICAL_SHOW_BINARY_STREAMS_HANDLER;
            case SHOW_CDC_STORAGE:
                return LOGICAL_SHOW_CDC_STORAGE_HANDLER;
            case CHANGE_MASTER:
                return LOGICAL_CHANGE_MASTER_HANDLER;
            case CHANGE_REPLICATION_FILTER:
                return LOGICAL_CHANGE_REPLICATION_FILTER_HANDLER;
            case SHOW_SLAVE_STATUS:
                return LOGICAL_SHOW_SLAVE_STATUS_HANDLER;
            case START_SLAVE:
                return LOGICAL_START_SLAVE_HANDLER;
            case STOP_SLAVE:
                return LOGICAL_STOP_SLAVE_HANDLER;
            case RESET_SLAVE:
                return LOGICAL_RESET_SLAVE_HANDLER;
            case START_MASTER:
                return LOGICAL_START_MASTER_HANDLER;
            case STOP_MASTER:
                return LOGICAL_STOP_MASTER_HANDLER;
            case RESTART_MASTER:
                return LOGICAL_RESTART_MASTER_HANDLER;
            case REBALANCE_MASTER:
                return LOGICAL_REBALANCE_MASTER_HANDLER;
            case RESET_MASTER:
                return LOGICAL_RESET_MASTER_HANDLER;
            case REPLICA_HASH_CHECK:
                return LOGICAL_REPLICA_HASHCHECK_HANDLER;
            case SET_CDC_GLOBAL:
                return LOGICAL_SET_CDC_GLOBAL_HANDLER;
            case FLUSH_LOGS:
                return LOGICAL_FLUSH_LOGS_HANDLER;
            case START_REPLICA_CHECK:
                return LOGICAL_START_REPLICA_CHECK_HANDLER;
            case PAUSE_REPLICA_CHECK:
                return LOGICAL_PAUSE_REPLICA_CHECK_HANDLER;
            case CONTINUE_REPLICA_CHECK:
                return LOGICAL_CONTINUE_REPLICA_CHECK_HANDLER;
            case CANCEL_REPLICA_CHECK:
                return LOGICAL_CANCEL_REPLICA_CHECK_HANDLER;
            case RESET_REPLICA_CHECK:
                return LOGICAL_RESET_REPLICA_CHECK_HANDLER;
            case SHOW_REPLICA_CHECK_PROGRESS:
                return LOGICAL_SHOW_REPLICA_CHECK_PROGRESS_HANDLER;
            case SHOW_REPLICA_CHECK_DIFF:
                return LOGICAL_SHOW_REPLICA_CHECK_DIFF_HANDLER;

            case SQL_SET_DEFAULT_ROLE:
                return LOGICAL_SET_DEFAULT_ROLE_HANDLER;
            case CREATE_CCL_RULE:
                return CREATE_CCL_RULE_HANDLER;
            case DROP_CCL_RULE:
                return DROP_CCL_RULE_HANDLER;
            case SHOW_CCL_RULE:
                return SHOW_CCL_RULE_HANDLER;
            case CLEAR_CCL_RULES:
                return CLEAR_CCL_RULES_HANDLER;
            case REBALANCE:
                return REBALANCE_HANDLER;
            case CREATE_CCL_TRIGGER:
                return CREATE_CCL_TRIGGER_HANDLER;
            case DROP_CCL_TRIGGER:
                return DROP_CCL_TRIGGER_HANDLER;
            case CLEAR_CCL_TRIGGERS:
                return CLEAR_CCL_TRIGGERS_HANDLER;
            case SHOW_CCL_TRIGGER:
                return SHOW_CCL_TRIGGER_HANDLER;
            case SLOW_SQL_CCL:
                return SLOW_SQL_CCL_HANDLER;
            case SHOW_GLOBAL_DEADLOCKS:
                return LOGICAL_SHOW_GLOBAL_DEADLOCKS_HANDLER;
            case SHOW_LOCAL_DEADLOCKS:
                return LOGICAL_SHOW_LOCAL_DEADLOCKS_HANDLER;
            case SHOW_PARTITONS_HEATMAP:
                return LOGICAL_SHOW_PARTITIONS_HEATMAP_HANDLER;
            case CREATE_SCHEDULE:
                return CREATE_SCHEDULE_HANDLER;
            case DROP_SCHEDULE:
                return DROP_SCHEDULE_HANDLER;
            case PAUSE_SCHEDULE:
                return PAUSE_SCHEDULE_HANDLER;
            case CONTINUE_SCHEDULE:
                return CONTINUE_SCHEDULE_HANDLER;
            case FIRE_SCHEDULE:
                return FIRE_SCHEDULE_HANDLER;
            case CREATE_SECURITY_LABEL_COMPONENT:
                return CREATE_SECURITY_LABEL_COMPONENT_HANDLER;
            case DROP_SECURITY_LABEL_COMPONENT:
                return DROP_SECURITY_LABEL_COMPONENT_HANDLER;
            case CREATE_SECURITY_LABEL:
                return CREATE_SECURITY_LABEL_HANDLER;
            case DROP_SECURITY_LABEL:
                return DROP_SECURITY_LABEL_HANDLER;
            case CREATE_SECURITY_POLICY:
                return CREATE_SECURITY_POLICY_HANDLER;
            case DROP_SECURITY_POLICY:
                return DROP_SECURITY_POLICY_HANDLER;
            case CREATE_SECURITY_ENTITY:
                return CREATE_SECURITY_ENTITY_HANDLER;
            case DROP_SECURITY_ENTITY:
                return DROP_SECURITY_ENTITY_HANDLER;
            case GRANT_SECURITY_LABEL:
                return GRANT_SECURITY_LABEL_HANDLER;
            case REVOKE_SECURITY_LABEL:
                return REVOKE_SECURITY_LABEL_HANDLER;
            case SHOW_CREATE_TABLEGROUP:
                return LOGICAL_SHOW_CREATE_TABLEGROUP_HANDLER;
            default:
                return BASE_DAL_HANDLER;
            }
        } else if (logicalPlan instanceof EmptyOperation) {
            return EMPTY_OPERATION_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateTableGroup) {
            return CREATE_TABLEGROUP_HANDLER;
        } else if (logicalPlan instanceof LogicalDropTableGroup) {
            return DROP_TABLEGROUP_HANDLER;
        } else if (logicalPlan instanceof LogicalOutFile) {
            return LOGICAL_OUT_FILE_HANDLER;
        } else if (logicalPlan instanceof LogicalMoveDatabases) {
            return LOGICAL_MOVE_DATABASES_HANDLER;
        } else if (logicalPlan instanceof MoveTableBackfill) {
            return MOVE_TABLE_BACKFILL_HANDLER;
        } else if (logicalPlan instanceof LogicalTableDataMigrationBackfill) {
            return LOGICAL_TABLE_DATA_MIGRATION_BACKFILL_HANDLER;
        } else if (logicalPlan instanceof LogicalCreateJoinGroup) {
            return CREATE_JOINGROUP_HANDLER;
        } else if (logicalPlan instanceof LogicalDropJoinGroup) {
            return LOGICAL_DROP_JOINGROUP_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterJoinGroup) {
            return LOGICAL_ALTER_JOINGROUP_HANDLER;
        } else if (logicalPlan instanceof LogicalMergeTableGroup) {
            return LOGICAL_MERGE_TABLEGROUP_HANDLER;
        } else if (logicalPlan instanceof LogicalAlterTableGroupAddTable) {
            return LOGICAL_ALTER_TABLEGROUP_ADD_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalOptimizeTable) {
            return LOGICAL_OPTIMIZE_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalInspectIndex) {
            return LOGICAL_INSPECT_INDEX_HANDLER;
        } else if (logicalPlan instanceof LogicalAnalyzeTable) {
            return LOGICAL_ANALYZE_TABLE_HANDLER;
        } else if (logicalPlan instanceof LogicalImportDatabase) {
            return LOGICAL_IMPORT_DATABASE;
        } else if (logicalPlan instanceof LogicalImportSequence) {
            return LOGICAL_IMPORT_SEQUENCE;
        } else if (logicalPlan instanceof LogicalAlterInstance) {
            return LOGICAL_ALTER_INSTANCE_HANDLER;
        }
        throw new AssertionError("Unsupported RelNode: " + logicalPlan.getClass().getSimpleName());
    }

}
