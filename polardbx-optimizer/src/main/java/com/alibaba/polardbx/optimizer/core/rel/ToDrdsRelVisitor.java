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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta.IndexType;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.AccessPathRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemLeader;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemRefreshStorage;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemReloadStorage;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalBaseline;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRebalance;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalReplicateDatabase;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalSet;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyDal;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalClearFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalConvertAllSequences;
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
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdIndex;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils.TableProperties;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.rel.ddl.AlterDatabase;
import org.apache.calcite.rel.ddl.AlterFileStorageAsOfTimestamp;
import org.apache.calcite.rel.ddl.AlterFileStorageBackup;
import org.apache.calcite.rel.ddl.AlterFileStoragePurgeBeforeTimestamp;
import org.apache.calcite.rel.ddl.AlterFunction;
import org.apache.calcite.rel.ddl.AlterInstance;
import org.apache.calcite.rel.ddl.AlterJoinGroup;
import org.apache.calcite.rel.ddl.AlterProcedure;
import org.apache.calcite.rel.ddl.AlterRule;
import org.apache.calcite.rel.ddl.AlterStoragePool;
import org.apache.calcite.rel.ddl.AlterSystemSetConfig;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupAddPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupAddTable;
import org.apache.calcite.rel.ddl.AlterTableGroupDropPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupExtractPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupModifyPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupRenamePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupReorgPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSetLocality;
import org.apache.calcite.rel.ddl.AlterTableGroupSetPartitionsLocality;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.rel.ddl.AlterTableGroupTruncatePartition;
import org.apache.calcite.rel.ddl.AlterTablePartitionCount;
import org.apache.calcite.rel.ddl.AlterTableRemovePartitioning;
import org.apache.calcite.rel.ddl.AlterTableRepartition;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;
import org.apache.calcite.rel.ddl.AnalyzeTable;
import org.apache.calcite.rel.ddl.ChangeConsensusRole;
import org.apache.calcite.rel.ddl.ClearFileStorage;
import org.apache.calcite.rel.ddl.ConvertAllSequences;
import org.apache.calcite.rel.ddl.CreateDatabase;
import org.apache.calcite.rel.ddl.CreateFileStorage;
import org.apache.calcite.rel.ddl.CreateFunction;
import org.apache.calcite.rel.ddl.CreateIndex;
import org.apache.calcite.rel.ddl.CreateJavaFunction;
import org.apache.calcite.rel.ddl.CreateJoinGroup;
import org.apache.calcite.rel.ddl.CreateMaterializedView;
import org.apache.calcite.rel.ddl.CreateProcedure;
import org.apache.calcite.rel.ddl.CreateStoragePool;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.rel.ddl.CreateTableGroup;
import org.apache.calcite.rel.ddl.CreateView;
import org.apache.calcite.rel.ddl.DropDatabase;
import org.apache.calcite.rel.ddl.DropFileStorage;
import org.apache.calcite.rel.ddl.DropFunction;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.rel.ddl.DropJavaFunction;
import org.apache.calcite.rel.ddl.DropJoinGroup;
import org.apache.calcite.rel.ddl.DropMaterializedView;
import org.apache.calcite.rel.ddl.DropProcedure;
import org.apache.calcite.rel.ddl.DropStoragePool;
import org.apache.calcite.rel.ddl.DropTable;
import org.apache.calcite.rel.ddl.DropTableGroup;
import org.apache.calcite.rel.ddl.DropView;
import org.apache.calcite.rel.ddl.GenericDdl;
import org.apache.calcite.rel.ddl.ImportDatabase;
import org.apache.calcite.rel.ddl.ImportSequence;
import org.apache.calcite.rel.ddl.InspectIndex;
import org.apache.calcite.rel.ddl.MergeTableGroup;
import org.apache.calcite.rel.ddl.MoveDatabase;
import org.apache.calcite.rel.ddl.OptimizeTable;
import org.apache.calcite.rel.ddl.PushDownUdf;
import org.apache.calcite.rel.ddl.RefreshTopology;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.rel.ddl.RenameTables;
import org.apache.calcite.rel.ddl.SequenceDdl;
import org.apache.calcite.rel.ddl.TruncateTable;
import org.apache.calcite.rel.ddl.UnArchive;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalRecyclebin;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableExtractPartition;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableModifySubPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTableRemoveLocalPartition;
import org.apache.calcite.sql.SqlAlterTableRenamePartition;
import org.apache.calcite.sql.SqlAlterTableReorgPartition;
import org.apache.calcite.sql.SqlAlterTableRepartitionLocalPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlCheckColumnarIndex;
import org.apache.calcite.sql.SqlCheckGlobalIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOptimizeTable;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowLocalityInfo;
import org.apache.calcite.sql.SqlShowPhysicalDdl;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DML_WITH_SUBQUERY;

/**
 * 对RelNode进行转换,将其底层TableScan转换为 LogicalView
 *
 * @author lingce.ldm
 */
public class ToDrdsRelVisitor extends RelShuttleImpl {

    private static final Logger logger = LoggerFactory.getLogger(ToDrdsRelVisitor.class);

    protected static Set<String> systemDbName = new TreeSet<String>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    static {
        // information_schema
        // performance_schema
        // sys
        // mysql
        systemDbName.add("information_schema");
        systemDbName.add("performance_schema");
        systemDbName.add("sys");
        systemDbName.add("mysql");
    }

    // Whether all tables are broadcast
    private boolean allTableBroadcast = true;
    // all tables are single table
    private boolean allTableSingle = true;
    // Whether all tables are single and in the same group
    private boolean allTableSingleWithSameGroup = true;
    private Long allTableSingleTgId = null;
    // Whether all tables are single and in the same group and no broadcast
    // table
    private boolean allTableSingleNoBroadcast = true;
    // If all tables are single, which group are they?
    private String singleDbIndex = null;
    private LogicalView baseLogicalView = null;
    // table names from original plan
    private List<String> tableNames = new ArrayList<>();
    private List<Map<Long, String>> tableStorages = new ArrayList<>();
    private SqlKind sqlKind = SqlKind.SELECT;
    private LockMode lockMode = LockMode.UNDEF;
    private boolean shouldRemoveSchemaName = false;
    private boolean modifyBroadcastTable = false;
    private boolean modifyGsiTable = false;
    private List<String> schemaNames = Lists.newArrayList();
    private PlannerContext plannerContext = null;
    private List<TableProperties> modifiedTables = new ArrayList<>();
    private boolean withIndexHint = false;
    private boolean modifyShardingColumn = false;
    private boolean containUncertainValue = false;
    private boolean containComplexExpression = false;
    private boolean containScaleOutWritableTable = false;
    private boolean containReplicateWriableTable = false;
    private boolean containOnlineModifyColumnTable = false;
    private boolean containGeneratedColumn = false;
    private boolean modifyForeignKey = false;

    private SqlNode ast;
    private boolean existsWindow = false;
    private boolean existsNonPushDownFunc = false;
    private boolean existsIntersect = false;
    private boolean existsMinus = false;
    private boolean existsGroupingSets;
    private boolean modifyWithLimitOffset = false;
    private boolean existsOSSTable;

    private boolean existsCheckSum = false;
    private boolean existsUnpushableAgg = false;
    private boolean existsCheckSumV2 = false;

    private boolean outFileStatistics = false;

    private boolean existsUnPushedDynamicValues = false;

    public ToDrdsRelVisitor() {
    }

    public ToDrdsRelVisitor(SqlNode ast, PlannerContext plannerContext) {
        this.sqlKind = ast.getKind();
        this.lockMode = LockMode.getLockMode(ast);
        this.plannerContext = plannerContext;
        this.ast = ast;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        this.existsGroupingSets = CBOUtil.isGroupSets(aggregate) || existsGroupingSets;
        this.existsCheckSum = CBOUtil.isCheckSum(aggregate) || this.existsCheckSum;
        this.existsUnpushableAgg = CBOUtil.containUnpushableAgg(aggregate) || this.existsUnpushableAgg;
        this.existsCheckSumV2 = CBOUtil.isCheckSumV2(aggregate) || this.existsCheckSumV2;
        return super.visit(aggregate);
    }

    @Override
    public RelNode visit(LogicalOutFile outFile) {
        this.outFileStatistics = outFile.getOutFileParams().getStatistics();
        return super.visit(outFile);
    }

    /**
     * 将 tableScan 替换为 LogicalView
     */
    @Override
    public final RelNode visit(TableScan scan) {
        final List<String> qualifiedName = scan.getTable().getQualifiedName();
        final String tableName = Util.last(scan.getTable().getQualifiedName());

        setShouldRemoveSchemaName(qualifiedName);

        final String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;

        // Ensure that schema name not null.
        final RelNode scanOrLookup = buildTableAccess(scan, tableName,
            null == schemaName ? this.plannerContext.getExecutionContext().getSchemaName() : schemaName);

        final List<String> tableNames = ImmutableList.of(tableName);

        if (!schemaNames.contains(schemaName)) {
            schemaNames.add(schemaName);
        }
        final Map<String, TableProperties> tablePropertiesMap =
            RelUtils.buildTablePropertiesMap(tableNames, schemaName, this.plannerContext.getExecutionContext());

        updateTableProperties(tablePropertiesMap, scan);

        // FIXME: Will anything broken if baseLogicalView is null?
        if (baseLogicalView == null && scanOrLookup instanceof LogicalView) {
            baseLogicalView = (LogicalView) scanOrLookup;
        }
        this.tableNames.add(tableNames.get(0));
        Map<Long, String> tableStorage = null;
        if (tablePropertiesMap != null) {
            TableProperties tableProperties = tablePropertiesMap.get(tableNames.get(0));
            if (tableProperties != null) {
                tableStorage = tableProperties.getStorageIds();
            }
        }
        this.tableStorages.add(tableStorage);
        if (scanOrLookup instanceof LogicalView) {
            AccessPathRule.nomoralizeIndexNode((LogicalView) scanOrLookup);
        }
        if (scan.getFlashback() != null) {
            if (!RexUtil.isDeterministic(scan.getFlashback())) {
                containUncertainValue = true;
                existsNonPushDownFunc = true;
            }
        }
        return scanOrLookup;
    }

    private boolean isGsiVisible(String schema, String table, String indexName) {
        if (StringUtils.isEmpty(indexName)) {
            return true;
        }
        SchemaManager sm = this.plannerContext.getExecutionContext().getSchemaManager(schema);
        if (sm == null) {
            return true;
        }
        TableMeta tableMeta = sm.getTable(table);
        if (tableMeta == null) {
            return true;
        }
        final Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = tableMeta.getGsiPublished();
        if (gsiPublished == null) {
            return true;
        }

        if (gsiPublished.containsKey(indexName)
            && gsiPublished.get(indexName).visibility == IndexVisibility.INVISIBLE) {
            return false;
        } else {
            return true;
        }
    }

    private RelNode buildTableAccess(TableScan scan, String tableName, String schemaName) {
        assert schemaName != null;
        final RelOptSchema catalog = RelUtils.buildCatalogReader(Optional.ofNullable(schemaName)
            .orElse(OptimizerContext.getContext(schemaName).getSchemaName()), plannerContext.getExecutionContext());

        if (scan.getIndexNode() instanceof SqlNodeList) {
            final Iterator<SqlNode> iterator = ((SqlNodeList) scan.getIndexNode()).iterator();
            while (iterator.hasNext()) {
                final SqlNode next = iterator.next();
                if (next instanceof SqlIndexHint) {
                    SqlIndexHint hint = (SqlIndexHint) next;
                    final String indexName =
                        GlobalIndexMeta.getIndexName(RelUtils.lastStringValue(hint.getIndexList()));
                    final String unwrapped = GlobalIndexMeta
                        .getGsiWrappedName(tableName, indexName, schemaName, plannerContext.getExecutionContext());
                    if (unwrapped != null) {
                        // Record the properties.
                        this.withIndexHint = true;
                    }
                }
            }
        }
        final Engine engine = this.plannerContext.getExecutionContext()
            .getSchemaManager(schemaName).getTable(tableName).getEngine();
        return Optional.ofNullable(scan.getIndexNode())
            // FORCE INDEX
            .filter(indexNode -> indexNode instanceof SqlNodeList && ((SqlNodeList) indexNode).size() > 0)
            // If more than one index specified, choose first one only
            .map(indexNode -> (SqlIndexHint) ((SqlNodeList) indexNode).get(0))
            // only support force index
            .filter(SqlIndexHint::forceIndex)
            .filter(hint -> {
                    final String indexName =
                        GlobalIndexMeta.getIndexName(RelUtils.lastStringValue(hint.getIndexList()));
                    final String unwrapped = GlobalIndexMeta
                        .getGsiWrappedName(tableName, indexName, schemaName, plannerContext.getExecutionContext());
                    return isGsiVisible(schemaName, tableName, StringUtils.isEmpty(unwrapped) ? indexName : unwrapped);
                }
            )
            // Dealing with force index(`xxx`), `xxx` will decoded as string.
            .map(indexNode -> GlobalIndexMeta.getIndexName(RelUtils.lastStringValue(indexNode.getIndexList().get(0))))
            .flatMap(indexName -> {
                // check columnar index first
                final String columnarIndexNameUnwrapped = GlobalIndexMeta.getColumnarWrappedName(tableName, indexName,
                    schemaName, plannerContext.getExecutionContext());
                if (columnarIndexNameUnwrapped != null) {
                    indexName = columnarIndexNameUnwrapped;
                }
                if (GlobalIndexMeta.getColumnarIndexType(tableName, indexName,
                    schemaName, plannerContext.getExecutionContext()) == IndexType.PUBLISHED_COLUMNAR) {
                    final RelOptTable indexTable = catalog.getTableForMember(ImmutableList.of(schemaName, indexName));
                    final LogicalTableScan columnarTableScan =
                        LogicalTableScan.create(scan.getCluster(), indexTable, scan.getHints(), null,
                            scan.getFlashback(), scan.getFlashbackOperator(),
                            null);
                    this.withIndexHint = true;
                    // remove force index for physical sql
                    scan.setIndexNode(null);
                    return Optional.of(new OSSTableScan(columnarTableScan, lockMode));
                }

                final String unwrapped = GlobalIndexMeta
                    .getGsiWrappedName(tableName, indexName, schemaName, plannerContext.getExecutionContext());
                if (unwrapped != null) {
                    indexName = unwrapped;
                }
                final IndexType indexType = GlobalIndexMeta
                    .getIndexType(tableName, indexName, schemaName, plannerContext.getExecutionContext());

                switch (indexType) {
                case PUBLISHED_GSI:
                    break;
                case UNPUBLISHED_GSI:
                    // Gsi whose table not finished creating
                case NONE:
                    // Gsi is removed but sql not updated or
                    scan.setIndexNode(null);
                case LOCAL:
                default:
                    return Optional.empty();
                }

                // remove force index for physical sql
                scan.setIndexNode(null);
                final LogicalView primary = RelUtils.createLogicalView(scan, lockMode, engine);
                final RelOptTable indexTable = catalog.getTableForMember(ImmutableList.of(schemaName, indexName));

                final LogicalTableScan indexTableScan =
                    LogicalTableScan.create(scan.getCluster(), indexTable, scan.getHints(), null, scan.getFlashback(),
                        scan.getFlashbackOperator(), null);
                final LogicalIndexScan index = new LogicalIndexScan(indexTable, indexTableScan, this.lockMode);
                this.withIndexHint = true;

                return Optional.of((RelNode) RelUtils.createTableLookup(primary, index, index.getTable()));
            })
            // INDEX HINT
            .orElseGet(() -> Optional.ofNullable(scan.getHints())
                .map(hints -> HintConverter
                    .convertCmd(hints, new ArrayList<>(), false, plannerContext.getExecutionContext()).cmdHintResult)
                .flatMap(cmdHints -> cmdHints.stream()
                    .filter(hint -> hint instanceof HintCmdIndex)
                    .map(hint -> (HintCmdIndex) hint)
                    .filter(hint -> TStringUtil.equalsIgnoreCase(tableName, hint.tableNameLast()))
                    .filter(hint -> {
                        final String unwrapped = GlobalIndexMeta.getGsiWrappedName(hint.tableNameLast(),
                            hint.indexNameLast(), schemaName, plannerContext.getExecutionContext());
                        return GlobalIndexMeta.isPublishedPrimaryAndIndex(hint.tableNameLast(),
                            null == unwrapped ? hint.indexNameLast() : unwrapped,
                            schemaName, plannerContext.getExecutionContext());
                    })
                    // If more than one index specified, choose first one only
                    .findFirst()
                    .map(hint -> {
                        final String unwrapped = GlobalIndexMeta.getGsiWrappedName(hint.tableNameLast(),
                            hint.indexNameLast(), schemaName, plannerContext.getExecutionContext());
                        final List<String> indexTableNames;
                        if (null == unwrapped) {
                            indexTableNames = hint.indexName.names;
                        } else {
                            indexTableNames = ImmutableList.of(schemaName, unwrapped);
                        }
                        final LogicalView primary = RelUtils.createLogicalView(scan, lockMode, engine);
                        final RelOptTable indexTable = catalog.getTableForMember(indexTableNames);
                        final LogicalTableScan indexTableScan =
                            LogicalTableScan.create(scan.getCluster(), indexTable, scan.getHints(), null,
                                scan.getFlashback(), scan.getFlashbackOperator(), null);
                        final LogicalIndexScan index = new LogicalIndexScan(indexTable, indexTableScan, this.lockMode);
                        this.withIndexHint = true;

                        return (RelNode) RelUtils.createTableLookup(primary, index, index.getTable());
                    }))
                .orElse(RelUtils.createLogicalView(scan, lockMode, engine)));
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        existsIntersect = true;
        return super.visit(intersect);
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        existsMinus = true;
        return super.visit(minus);
    }

    @Override
    public final RelNode visit(LogicalProject project) {
        ReplaceTableScanInFilterSubQueryFinder
            replaceTableScanInFilterSubQueryFinder = new ReplaceTableScanInFilterSubQueryFinder(sqlKind,
            lockMode,
            allTableSingleWithSameGroup,
            allTableBroadcast,
            allTableSingleNoBroadcast,
            singleDbIndex,
            schemaNames,
            plannerContext);
        List<RexNode> rexNodeList = Lists.newArrayList();
        for (RexNode r : project.getProjects()) {
            if (r instanceof RexCall) {
                existsWindow |= containsWindowExpr((RexCall) r);
            }
            existsNonPushDownFunc |= RexUtils.containsUnPushableFunction(r, false);
            RexNode rexNode = r.accept(replaceTableScanInFilterSubQueryFinder);
            if (replaceTableScanInFilterSubQueryFinder.baseLogicalView != null && baseLogicalView == null) {
                baseLogicalView = replaceTableScanInFilterSubQueryFinder.baseLogicalView;
            }
            if (replaceTableScanInFilterSubQueryFinder.tableNames.size() > 0) {
                tableNames.addAll(replaceTableScanInFilterSubQueryFinder.tableNames);
                tableStorages.addAll(replaceTableScanInFilterSubQueryFinder.storageIds);
            }

            if (allTableSingle) {
                if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTable()) {
                    allTableSingle = false;
                }
            }

            if (allTableSingleWithSameGroup) {

                if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTableWithSameGroup()) {
                    allTableSingleWithSameGroup = false;
                } else {
                    // singleDbIndex might be null before.
                    singleDbIndex = replaceTableScanInFilterSubQueryFinder.getSingleDbIndex();
                }
                if (this.allTableSingleTgId == null) {
                    this.allTableSingleTgId = replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId();
                } else if (replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId() != null &&
                    this.allTableSingleTgId != replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId()) {
                    allTableSingleWithSameGroup = false;
                }
            }
            if (this.allTableBroadcast && !replaceTableScanInFilterSubQueryFinder.isAllTableBroadcast()) {
                this.allTableBroadcast = false;
            }
            rexNodeList.add(rexNode);
        }
        RelNode logicalProject = super.visit(project);
        RelMetadataQuery mq = logicalProject.getCluster().getMetadataQuery();
        this.containUncertainValue |= replaceTableScanInFilterSubQueryFinder.isContainUncertainValue();
        this.containComplexExpression |= replaceTableScanInFilterSubQueryFinder.isContainComplexExpression();
        this.existsWindow |= replaceTableScanInFilterSubQueryFinder.isExistsWindow();
        this.existsNonPushDownFunc |= replaceTableScanInFilterSubQueryFinder.isExistsNonPushDownFunc();
        this.existsOSSTable |= replaceTableScanInFilterSubQueryFinder.existsOSSTable();
        return LogicalProject.create(logicalProject.getInput(0),
            rexNodeList,
            logicalProject.getRowType(),
            mq.getOriginalRowType(logicalProject),
            logicalProject.getVariablesSet()).setHints(project.getHints());
    }

    private boolean containsWindowExpr(RexCall rexCall) {
        return rexCall instanceof RexOver || rexCall.getOperands().stream()
            .anyMatch(t -> t instanceof RexCall && containsWindowExpr((RexCall) t));
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        existsNonPushDownFunc |=
            RexUtils.containsUnPushableFunction(filter.getCondition(), false);
        ReplaceTableScanInFilterSubQueryFinder
            replaceTableScanInFilterSubQueryFinder = new ReplaceTableScanInFilterSubQueryFinder(sqlKind,
            lockMode,
            allTableSingleWithSameGroup,
            allTableBroadcast,
            allTableSingleNoBroadcast,
            singleDbIndex,
            schemaNames,
            plannerContext);
        RexNode rexNode = filter.getCondition().accept(replaceTableScanInFilterSubQueryFinder);
        RelNode logicalFilter = super.visit(filter);

        if (replaceTableScanInFilterSubQueryFinder.baseLogicalView != null && baseLogicalView == null) {
            baseLogicalView = replaceTableScanInFilterSubQueryFinder.baseLogicalView;
        }
        if (replaceTableScanInFilterSubQueryFinder.tableNames.size() > 0) {
            tableNames.addAll(replaceTableScanInFilterSubQueryFinder.tableNames);
            tableStorages.addAll(replaceTableScanInFilterSubQueryFinder.storageIds);
        }

        if (allTableSingle) {
            if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTable()) {
                allTableSingle = false;
            }
        }

        if (allTableSingleWithSameGroup) {
            if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTableWithSameGroup()) {
                allTableSingleWithSameGroup = false;
            } else {
                // singleDbIndex might be null before.
                singleDbIndex = replaceTableScanInFilterSubQueryFinder.getSingleDbIndex();
            }
            if (this.allTableSingleTgId == null) {
                this.allTableSingleTgId = replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId();
            } else if (replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId() != null &&
                this.allTableSingleTgId != replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId()) {
                allTableSingleWithSameGroup = false;
            }
        }

        if (this.allTableBroadcast && !replaceTableScanInFilterSubQueryFinder.isAllTableBroadcast()) {
            this.allTableBroadcast = false;
        }

        if (this.allTableSingleNoBroadcast && !replaceTableScanInFilterSubQueryFinder.isAllTableSingleNoBroadcast()) {
            this.allTableSingleNoBroadcast = false;
        }

        this.containUncertainValue |= replaceTableScanInFilterSubQueryFinder.isContainUncertainValue();
        this.containComplexExpression |= replaceTableScanInFilterSubQueryFinder.isContainComplexExpression();
        this.existsNonPushDownFunc |= replaceTableScanInFilterSubQueryFinder.isExistsNonPushDownFunc();
        this.existsOSSTable |= replaceTableScanInFilterSubQueryFinder.existsOSSTable();
        this.existsWindow |= replaceTableScanInFilterSubQueryFinder.isExistsWindow();
        return filter.copy(logicalFilter.getTraitSet(), logicalFilter.getInput(0), rexNode).setHints(filter.getHints());
    }

    @Override
    public final RelNode visit(RelNode other) {
        if ((other instanceof LogicalTableModify)) {

            LogicalTableModify modify = (LogicalTableModify) super.visit(other);
            setShouldRemoveSchemaName(modify.getTable().getQualifiedName());
            TableModify.Operation operation = modify.getOperation();

            CheckModifyLimitation.check(modify, plannerContext);

            final boolean modifyFkReferenced = CheckModifyLimitation.checkModifyFkReferenced(modify,
                this.plannerContext.getExecutionContext());

            TableModify newPlan;
            Map<String, TableProperties> targetTableProperties;
            Map<String, TableProperties> refTableProperties;
            if (operation == TableModify.Operation.INSERT || operation == TableModify.Operation.REPLACE) {
                LogicalInsert logicalInsert = new LogicalInsert(modify);
                String schemaName = logicalInsert.getSchemaName();
                String tableName = logicalInsert.getLogicalTableName();

                if (!schemaNames.contains(schemaName)) {
                    schemaNames.add(schemaName);
                }

                // input of LogicalInsert (like LogicalProject) may be removed
                // by planner rules, but its row type must be saved.
                logicalInsert.setInsertRowType(logicalInsert.getInput().getRowType());

                // insertion into broadcast table can't be transformed to
                // DirectTableOperation.
                if (OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(tableName)
                    || SequenceManagerProxy.getInstance().isUsingSequence(schemaName, tableName)) {
                    allTableSingleWithSameGroup = false;
                }

                newPlan = logicalInsert;
                targetTableProperties = RelUtils.buildTablePropertiesMap(logicalInsert.getTargetTableNames(),
                    schemaName, this.plannerContext.getExecutionContext());
                refTableProperties = new HashMap<>(targetTableProperties);

                // Remove #allTableSingle flag for case that single tables not all in one table group
                refTableProperties
                    .values()
                    .stream()
                    .filter(tp -> null != tp.getPartInfo())
                    .forEach(tp -> updateAllTableSingleWithSameTgFlag(tp.getPartInfo()));

                this.modifyShardingColumn |= CheckModifyLimitation.checkUpsertModifyShardingColumn(logicalInsert);

                if (modifyFkReferenced) {
                    logicalInsert.setModifyForeignKey(true);
                }

            } else { // UPDATE / DELETE
                // Currently we do not allow create GSI on broadcast or single table
                targetTableProperties = new HashMap<>();
                refTableProperties = new HashMap<>();
                for (RelOptTable table : modify.getTableInfo().getTargetTableSet()) {
                    final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
                    targetTableProperties.putAll(RelUtils.buildTablePropertiesMap(ImmutableList.of(qn.right), qn.left,
                        this.plannerContext.getExecutionContext()));

                    if (TStringUtil.isNotBlank(qn.left) && !schemaNames.contains(qn.left)) {
                        schemaNames.add(qn.left);
                    }
                }

                for (RelOptTable table : modify.getTableInfo().getRefTables()) {
                    final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
                    refTableProperties.putAll(RelUtils.buildTablePropertiesMap(ImmutableList.of(qn.right), qn.left,
                        this.plannerContext.getExecutionContext()));
                }

                // Remove #allTableSingle flag for case that single tables not all in one table group
                refTableProperties
                    .values()
                    .stream()
                    .filter(tp -> null != tp.getPartInfo())
                    .forEach(tp -> updateAllTableSingleWithSameTgFlag(tp.getPartInfo()));

                if (this.allTableSingleWithSameGroup && !refTableProperties.isEmpty()) {
                    final boolean targetAllBroadcast = RelUtils.allTableBroadcast(targetTableProperties);
                    final boolean targetNoBroadcast = RelUtils.allTableNotBroadcast(targetTableProperties);
                    final boolean refAllBroadcast = RelUtils.allTableBroadcast(refTableProperties);

                    this.allTableSingleWithSameGroup = (targetAllBroadcast && refAllBroadcast) || targetNoBroadcast;
                }

                if ((ast instanceof SqlDelete && ((SqlDelete) ast).getOffset() != null) ||
                    (ast instanceof SqlUpdate && ((SqlUpdate) ast).getOffset() != null)) {
                    modifyWithLimitOffset = true;
                }

                final LogicalModify logicalModify = new LogicalModify(modify);
                this.modifyShardingColumn |= CheckModifyLimitation.checkModifyShardingColumn(logicalModify);

                if (CheckModifyLimitation.checkModifyFkReferencing(logicalModify,
                    this.plannerContext.getExecutionContext()) ||
                    modifyFkReferenced) {
                    this.modifyForeignKey = true;
                }

                if (modifyFkReferenced
                    || logicalModify.isUpdate() && CheckModifyLimitation.checkModifyForeignKeyConstraint(
                    logicalModify, this.plannerContext.getExecutionContext())) {
                    logicalModify.setModifyForeignKey(true);
                }

                logicalModify.setOriginalSqlNode(ast);

                newPlan = logicalModify;
            }

            List<String> modifyingTableNames = Lists.newArrayList(targetTableProperties.keySet());
            this.modifiedTables = ImmutableList.copyOf(targetTableProperties.values());

            updateTableProperties(refTableProperties, newPlan);

            if (!modifyBroadcastTable && RelUtils.containsBroadcastTable(targetTableProperties, modifyingTableNames)) {
                modifyBroadcastTable = true;
            }

            if (!modifyGsiTable && RelUtils.containsGsiTable(targetTableProperties, modifyingTableNames)) {
                modifyGsiTable = true;
            }

            if (!containScaleOutWritableTable && RelUtils
                .containScaleOutWriableTable(targetTableProperties, modifyingTableNames,
                    this.plannerContext.getExecutionContext())) {
                containScaleOutWritableTable = true;
            }

            if (!containReplicateWriableTable && RelUtils
                .containsReplicateWriableTable(targetTableProperties, modifyingTableNames,
                    this.plannerContext.getExecutionContext())) {
                containReplicateWriableTable = true;
            }

            if (!containOnlineModifyColumnTable && RelUtils.containOnlineModifyColumnTable(targetTableProperties,
                modifyingTableNames, this.plannerContext.getExecutionContext())) {
                containOnlineModifyColumnTable = true;
            }

            if (!containGeneratedColumn && RelUtils.containGeneratedColumn(targetTableProperties,
                modifyingTableNames, this.plannerContext.getExecutionContext())) {
                containGeneratedColumn = true;
            }

            return newPlan.setHints(modify.getHints());
        } else if (other instanceof DDL) {
            return convertToLogicalDdlPlan((DDL) other);
        } else if (other instanceof LogicalRecyclebin) {
            return other;
        } else if (other instanceof Dal) {
            final Dal dalNode = (Dal) other;
            final SqlDal sqlDal = dalNode.getAst();
            String schemaName = null;
            SqlKind kind = sqlDal.getKind();
            if (sqlDal instanceof SqlShow) {
                kind = ((SqlShow) sqlDal).getShowKind();
            }
            if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_SCHEMA) && kind == SqlKind.SHOW_TABLES) {
                String fromScehma = ((SqlShowTables) sqlDal).getSchema();
                if (!TStringUtil.equalsIgnoreCase(fromScehma, "information_schema") && !TStringUtil
                    .equalsIgnoreCase(fromScehma, "mysql")) {
                    schemaName = ((SqlShowTables) sqlDal).getSchema();
                } else if (TStringUtil
                    .equalsIgnoreCase(fromScehma, "information_schema")) {
                    schemaName = ((SqlShowTables) sqlDal).getSchema();
                }
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_SCHEMA) && kind == SqlKind.SHOW_LOCALITY_INFO) {
                String fromSchema = ((SqlShowLocalityInfo) sqlDal).getSchema();
                if (!TStringUtil.equalsIgnoreCase(fromSchema, "information_schema") && !TStringUtil
                    .equalsIgnoreCase(fromSchema, "mysql")) {
                    schemaName = ((SqlShowLocalityInfo) sqlDal).getSchema();
                } else if (TStringUtil.equalsIgnoreCase(fromSchema, "information_schema")) {
                    schemaName = ((SqlShowLocalityInfo) sqlDal).getSchema();
                }
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_SCHEMA) && kind == SqlKind.SHOW_PHYSICAL_DDL) {
                String fromSchema = ((SqlShowPhysicalDdl) sqlDal).getSchema();
                if (!TStringUtil.equalsIgnoreCase(fromSchema, "information_schema") && !TStringUtil
                    .equalsIgnoreCase(fromSchema, "mysql")) {
                    schemaName = ((SqlShowPhysicalDdl) sqlDal).getSchema();
                } else if (ConfigDataMode.isPolarDbX() && TStringUtil
                    .equalsIgnoreCase(fromSchema, "information_schema")) {
                    schemaName = ((SqlShowPhysicalDdl) sqlDal).getSchema();
                }
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_TABLE)) {
                if (sqlDal.getTableName() instanceof SqlIdentifier
                    && ((SqlIdentifier) sqlDal.getTableName()).names.size() == 2) {
                    String schemaNameInTable = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    if (!TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable) && !TStringUtil
                        .equalsIgnoreCase("mysql", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    } else if (TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    }
                }
            } else if (kind == SqlKind.SHOW_INDEX) {
                if (sqlDal.getDbName() != null
                    && !TStringUtil.equalsIgnoreCase("information_schema", sqlDal.getDbName().toString())
                    && !TStringUtil.equalsIgnoreCase("mysql", sqlDal.getDbName().toString())) {
                    schemaName = sqlDal.getDbName().toString();
                } else if (sqlDal.getDbName() != null && TStringUtil
                    .equalsIgnoreCase("information_schema", sqlDal.getDbName().toString())) {
                    schemaName = sqlDal.getDbName().toString();
                }
            } else {
                if (sqlDal.getTableName() instanceof SqlIdentifier
                    && ((SqlIdentifier) sqlDal.getTableName()).names.size() == 2) {
                    String schemaNameInTable = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    if (!TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable) && !TStringUtil
                        .equalsIgnoreCase("mysql", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    } else if (TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    }
                }
            }
            // Support cross schema DAL
            final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
            if (optimizerContext == null) {
                GeneralUtil.nestedException("Cannot find schema: " + schemaName + ", please check your sql again.");
            }
            final TddlRuleManager rule = optimizerContext.getRuleManager();
            final boolean singleDbIndex = rule.isSingleDbIndex();
            String dbIndex = rule.getDefaultDbIndex(null);

            String phyTable = "";
            PartitionInfoManager partInfoMgr = optimizerContext.getPartitionInfoManager();
            if (null != sqlDal.getTableName()) {
                String logicalTable = RelUtils.lastStringValue(sqlDal.getTableName());
                PartitionInfo partInfo =
                    partInfoMgr.getPartitionInfo(logicalTable);
                boolean isSchemaValid =
                    !RelUtils.informationSchema(sqlDal.getTableName()) && !RelUtils.mysqlSchema(sqlDal.getTableName());
                if (!singleDbIndex && partInfo == null) {
                    TargetDB target = rule.shardAny(logicalTable);
                    if (isSchemaValid) {
                        phyTable = target.getTableNames().iterator().next();
                    }
                    dbIndex = target.getDbIndex();
                } else if (partInfo != null) {
                    PhysicalPartitionInfo prunedPartitionInfo = partInfoMgr.getFirstPhysicalPartition(logicalTable);
                    dbIndex = prunedPartitionInfo.getGroupKey();
                    if (isSchemaValid) {
                        phyTable = prunedPartitionInfo.getPhyTable();
                    }
                }

            }
            if (kind.belongsTo(SqlKind.LOGICAL_SHOW_QUERY)) {
                final LogicalShow logicalShow = LogicalShow.create((Show) other, dbIndex, phyTable, schemaName);
                final SqlNode dbName = ((Show) other).getAst().getDbName();
                if (null != dbName && (TStringUtil.equalsIgnoreCase("information_schema", RelUtils.stringValue(dbName))
                    || TStringUtil.equalsIgnoreCase("mysql", RelUtils.stringValue(dbName)))) {
                    logicalShow.setRemoveDbPrefix(false);
                }
                return logicalShow;
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_BINLOG)) {
                final LogicalShow logicalShow = LogicalShow.create((Show) other, dbIndex, phyTable, schemaName);
                return logicalShow;
            } else if (kind == SqlKind.SHOW) {
                if (singleDbIndex && sqlDal.getTableName() != null) {
                    phyTable = RelUtils.lastStringValue(sqlDal.getTableName());
                }
                final PhyShow phyShow = PhyShow.create((Show) other, dbIndex, phyTable, schemaName);
                final SqlNode dbName = ((Show) other).getAst().getDbName();
                if (null != dbName && (TStringUtil.equalsIgnoreCase("information_schema", RelUtils.stringValue(dbName))
                    || TStringUtil.equalsIgnoreCase("mysql", RelUtils.stringValue(dbName)))) {
                    phyShow.setRemoveDbPrefix(false);
                }
                return phyShow;
            } else if (kind.belongsTo(SqlKind.SQL_SET_QUERY)) {
                return LogicalSet.create(dalNode, dbIndex, phyTable);
            } else if (kind == SqlKind.MOVE_DATABASE) {
                return LogicalReplicateDatabase.create(dalNode);
            } else {
                switch (kind) {
                case OPTIMIZE_TABLE:
                    return handleOptimizeTable(dalNode);
                case LOCK_TABLE:
                case UNLOCK_TABLE:
                    return EmptyOperation.create(other.getCluster(), dalNode.getRowType());
                case BASELINE:
                    return LogicalBaseline.create(dalNode);
                case CREATE_CCL_RULE:
                case SHOW_CCL_RULE:
                case DROP_CCL_RULE:
                case CLEAR_CCL_RULES:
                case CREATE_CCL_TRIGGER:
                case SHOW_CCL_TRIGGER:
                case DROP_CCL_TRIGGER:
                case CLEAR_CCL_TRIGGERS:
                case SLOW_SQL_CCL:
                    return LogicalCcl.create(dalNode);
                case ALTER_SYSTEM_REFRESH_STORAGE:
                    return LogicalAlterSystemRefreshStorage.create(dalNode);
                case ALTER_SYSTEM_RELOAD_STORAGE:
                    return LogicalAlterSystemReloadStorage.create(dalNode);
                case ALTER_SYSTEM_LEADER:
                    return LogicalAlterSystemLeader.create(dalNode);
                default:
                    return LogicalDal.create(dalNode, dbIndex, phyTable, null);
                }
            }
        } else if (other instanceof DynamicValues) {
            if (!InstanceVersion.isMYSQL80() || !plannerContext.getExecutionContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_VALUES_PUSHDOWN)) {
                this.existsUnPushedDynamicValues = true;
            }
            return super.visit(other);
        } else {
            return super.visit(other);
        }
    }

    private RelNode convertToLogicalDdlPlan(DDL ddl) {
        if (isSupportedByNewDdlEngine(ddl)) {
            // The plan will be executed via new DDL Engine.
            if (ddl instanceof CreateTable) {
                SqlCreateTable sqlCreateTable = (SqlCreateTable) ddl.getSqlNode();
                if (!sqlCreateTable.getAddedForeignKeys().isEmpty()) {
                    ForeignKeyData foreignKeyData = sqlCreateTable.getAddedForeignKeys().get(0);

                    SqlIdentifier tbNameId = (SqlIdentifier) sqlCreateTable.getName();
                    Pair<String, String> dbAndTb = CalciteUtils.getDbNameAndTableNameByTableIdentifier(tbNameId);
                    String dbName = dbAndTb.getKey();
                    String tbName = dbAndTb.getValue();

                    final List<Pair<String, ForeignKeyData>> refTables =
                        sqlCreateTable.getAddedForeignKeys().stream().map(v -> Pair.of(v.refTableName, v))
                            .collect(Collectors.toList());

                    if (refTables.stream().allMatch(refTable ->
                        ExecutionStrategy.pushableForeignConstraint(plannerContext, dbName, tbName, refTable,
                            sqlCreateTable))) {
                        // Can push down.
                        sqlCreateTable.setPushDownForeignKeys(true);
                        for (ForeignKeyData data : sqlCreateTable.getAddedForeignKeys()) {
                            data.setPushDown(true);
                        }
//                        sqlCreateTable.removeForeignKeys();
                    }

                    // Remove referenced table replacement.
                    if (!sqlCreateTable.getPushDownForeignKeys()) {
                        sqlCreateTable.setLogicalReferencedTables(null);
                    }
                }

                return LogicalCreateTable.create((CreateTable) ddl);
            } else if (ddl instanceof AlterTable) {
                boolean isAlterLocalPartition = ddl.sqlNode instanceof SqlAlterTableRepartitionLocalPartition;
                boolean isRemoveLocalPartition = ddl.sqlNode instanceof SqlAlterTableRemoveLocalPartition;
                if (isAlterLocalPartition || isRemoveLocalPartition) {
                    return LogicalAlterTable.create((AlterTable) ddl);
                }
                SqlAlterTable sqlAlterTable = (SqlAlterTable) ddl.getSqlNode();
                SqlIdentifier tbNameId = (SqlIdentifier) sqlAlterTable.getName();
                Pair<String, String> dbAndTb = CalciteUtils.getDbNameAndTableNameByTableIdentifier(tbNameId);
                String dbName = dbAndTb.getKey();
                String tbName = dbAndTb.getValue();
                if (sqlAlterTable.getAlters().size() == 1) {
                    if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartitionByHotValue) {
                        return LogicalAlterTableSplitPartitionByHotValue.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableExtractPartition) {
                        return LogicalAlterTableExtractPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartition) {
                        return LogicalAlterTableSplitPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMergePartition) {
                        return LogicalAlterTableMergePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMovePartition) {
                        return LogicalAlterTableMovePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableAddPartition) {
                        return LogicalAlterTableAddPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableDropPartition) {
                        return LogicalAlterTableDropPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableTruncatePartition) {
                        return LogicalAlterTableTruncatePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableReorgPartition) {
                        return LogicalAlterTableReorgPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableModifyPartitionValues) {
                        return LogicalAlterTableModifyPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableModifySubPartitionValues) {
                        return LogicalAlterTableModifyPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableRenamePartition) {
                        return LogicalAlterTableRenamePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAddForeignKey) {
                        // Check and set FK before all with EC context.
                        ForeignKeyUtils.checkSetForeignKey(sqlAlterTable, plannerContext, tbName);
                        return LogicalAlterTable.create((AlterTable) ddl);
                    } else {
                        return LogicalAlterTable.create((AlterTable) ddl);
                    }
                } else {
                    return LogicalAlterTable.create((AlterTable) ddl);
                }

            } else if (ddl instanceof RenameTables) {
                return LogicalRenameTables.create((RenameTables) ddl);
            } else if (ddl instanceof RenameTable) {
                return LogicalRenameTable.create((RenameTable) ddl);
            } else if (ddl instanceof TruncateTable) {
                if (((TruncateTable) ddl).isInsertOverwriteSql()) {
                    return LogicalInsertOverwrite.create((TruncateTable) ddl);
                } else {
                    return LogicalTruncateTable.create((TruncateTable) ddl);
                }
            } else if (ddl instanceof DropTable) {
                return LogicalDropTable.create((DropTable) ddl);

            } else if (ddl instanceof DropMaterializedView) {
                return convertDropMaterializedView((DropMaterializedView) ddl);
            } else if (ddl instanceof CreateIndex) {
                return LogicalCreateIndex.create((CreateIndex) ddl);

            } else if (ddl instanceof DropIndex) {
                return LogicalDropIndex.create((DropIndex) ddl);

            } else if (ddl instanceof AlterRule) {
                return LogicalAlterRule.create((AlterRule) ddl);

            } else if (ddl.getSqlNode() instanceof SqlCheckGlobalIndex) {
                return LogicalCheckGsi.create((GenericDdl) ddl, (SqlCheckGlobalIndex) ddl.getSqlNode());

            } else if (ddl.getSqlNode() instanceof SqlCheckColumnarIndex) {
                return LogicalCheckCci.create((GenericDdl) ddl, (SqlCheckColumnarIndex) ddl.getSqlNode());

            } else if (ddl instanceof GenericDdl) {
                return LogicalGenericDdl.create((GenericDdl) ddl);

            } else if (ddl instanceof AlterTableGroupSplitPartition) {
                return LogicalAlterTableGroupSplitPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupMergePartition) {
                return LogicalAlterTableGroupMergePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupMovePartition) {
                return LogicalAlterTableGroupMovePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupExtractPartition) {
                return LogicalAlterTableGroupExtractPartition.create(ddl);

            } else if (ddl instanceof AlterTableSetTableGroup) {
                return LogicalAlterTableSetTableGroup.create(ddl);

            } else if (ddl instanceof AlterTableGroupRenamePartition) {
                return LogicalAlterTableGroupRenamePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupSetLocality) {
                return LogicalAlterTableGroupSetLocality.create(ddl);

            } else if (ddl instanceof AlterTableGroupSetPartitionsLocality) {
                return LogicalAlterTableGroupSetPartitionsLocality.create(ddl);

            } else if (ddl instanceof RefreshTopology) {
                return LogicalRefreshTopology.create(ddl);

            } else if (ddl instanceof AlterTableGroupAddPartition) {
                return LogicalAlterTableGroupAddPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupDropPartition) {
                return LogicalAlterTableGroupDropPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupTruncatePartition) {
                return LogicalAlterTableGroupTruncatePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupReorgPartition) {
                return LogicalAlterTableGroupReorgPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupModifyPartition) {
                return LogicalAlterTableGroupModifyPartition.create(ddl);

            } else if (ddl instanceof MoveDatabase) {
                return LogicalMoveDatabases.create(ddl);

            } else if (ddl instanceof AlterTablePartitionCount) {
                return LogicalAlterTablePartitionCount.create((AlterTablePartitionCount) ddl);

            } else if (ddl instanceof AlterTableRemovePartitioning) {
                return LogicalAlterTableRemovePartitioning.create((AlterTableRemovePartitioning) ddl);

            } else if (ddl instanceof AlterTableRepartition) {
                return LogicalAlterTableRepartition.create((AlterTableRepartition) ddl);

            } else if (ddl instanceof AlterTableGroupSplitPartitionByHotValue) {
                return LogicalAlterTableGroupSplitPartitionByHotValue.create(ddl);

            } else if (ddl instanceof CreateJoinGroup) {
                return LogicalCreateJoinGroup.create((CreateJoinGroup) ddl);

            } else if (ddl instanceof DropJoinGroup) {
                return LogicalDropJoinGroup.create(ddl);

            } else if (ddl instanceof AlterJoinGroup) {
                return LogicalAlterJoinGroup.create(ddl);

            } else if (ddl instanceof MergeTableGroup) {
                return LogicalMergeTableGroup.create(ddl);

            } else if (ddl instanceof AlterTableGroupAddTable) {
                return LogicalAlterTableGroupAddTable.create(ddl);

            } else if (ddl instanceof AlterFileStorageAsOfTimestamp
                || ddl instanceof AlterFileStoragePurgeBeforeTimestamp
                || ddl instanceof AlterFileStorageBackup) {
                return LogicalAlterFileStorage.create(ddl);

            } else if (ddl instanceof DropFileStorage) {
                return LogicalDropFileStorage.create(ddl);
            } else if (ddl instanceof ClearFileStorage) {
                return LogicalClearFileStorage.create(ddl);
            } else if (ddl instanceof CreateFileStorage) {
                return LogicalCreateFileStorage.create(ddl);
            } else if (ddl instanceof CreateStoragePool) {
                return LogicalCreateStoragePool.create(ddl);
            } else if (ddl instanceof AlterStoragePool) {
                return LogicalAlterStoragePool.create(ddl);
            } else if (ddl instanceof DropStoragePool) {
                return LogicalDropStoragePool.create(ddl);
            } else if (ddl instanceof OptimizeTable) {
                return LogicalOptimizeTable.create((OptimizeTable) ddl);
            } else if (ddl instanceof AnalyzeTable) {
                return LogicalAnalyzeTable.create((AnalyzeTable) ddl);
            } else if (ddl instanceof PushDownUdf) {
                return LogicalPushDownUdf.create((PushDownUdf) ddl);
            } else if (ddl instanceof CreateView) {
                return LogicalCreateView.create((CreateView) ddl);
            } else if (ddl instanceof DropView) {
                return LogicalDropView.create((DropView) ddl);
            } else if (ddl instanceof CreateFunction) {
                return LogicalCreateFunction.create((CreateFunction) ddl);

            } else if (ddl instanceof DropFunction) {
                return LogicalDropFunction.create((DropFunction) ddl);

            } else if (ddl instanceof CreateJavaFunction) {
                return LogicalCreateJavaFunction.create((CreateJavaFunction) ddl);
            } else if (ddl instanceof DropJavaFunction) {
                return LogicalDropJavaFunction.create((DropJavaFunction) ddl);
            } else if (ddl instanceof CreateProcedure) {
                return LogicalCreateProcedure.create((CreateProcedure) ddl);

            } else if (ddl instanceof DropProcedure) {
                return LogicalDropProcedure.create((DropProcedure) ddl);
            } else if (ddl instanceof AlterProcedure) {
                return LogicalAlterProcedure.create((AlterProcedure) ddl);
            } else if (ddl instanceof AlterFunction) {
                return LogicalAlterFunction.create((AlterFunction) ddl);
            } else if (ddl instanceof AlterDatabase) {
                return LogicalAlterDatabase.create((AlterDatabase) ddl);
            } else if (ddl instanceof ImportDatabase) {
                return LogicalImportDatabase.create((ImportDatabase) ddl);
            } else if (ddl instanceof ImportSequence) {
                return LogicalImportSequence.create((ImportSequence) ddl);
            } else if (ddl instanceof AlterInstance) {
                return LogicalAlterInstance.create((AlterInstance) ddl);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                    "operation " + ddl.getSqlNode().getKind());
            }
        } else {
            // The plan will be executed separately (not via DDL Engine).
            if (ddl instanceof CreateDatabase) {
                return LogicalCreateDatabase.create((CreateDatabase) ddl);

            } else if (ddl instanceof DropDatabase) {
                return LogicalDropDatabase.create((DropDatabase) ddl);
            }

            if (ddl instanceof CreateMaterializedView) {
                return convertCreateMaterializedView((CreateMaterializedView) ddl);
            } else if (ddl.getSqlNode() instanceof SqlRebalance) {
                return LogicalRebalance.create((GenericDdl) ddl, (SqlRebalance) ddl.getSqlNode());
            } else if (ddl.getSqlNode() instanceof SqlCheckGlobalIndex) {
                return LogicalCheckGsi.create((GenericDdl) ddl, (SqlCheckGlobalIndex) ddl.getSqlNode());
            } else if (ddl.getSqlNode() instanceof SqlCheckColumnarIndex) {
                return LogicalCheckCci.create((GenericDdl) ddl, (SqlCheckColumnarIndex) ddl.getSqlNode());
            } else if (ddl instanceof ChangeConsensusRole) {
                return LogicalChangeConsensusLeader.create((ChangeConsensusRole) ddl);
            } else if (ddl instanceof AlterSystemSetConfig) {
                return LogicalAlterSystemSetConfig.create((AlterSystemSetConfig) ddl);
            } else if (ddl instanceof CreateTableGroup) {
                return LogicalCreateTableGroup.create((CreateTableGroup) ddl);
            } else if (ddl instanceof DropTableGroup) {
                return LogicalDropTableGroup.create((DropTableGroup) ddl);
            } else if (ddl instanceof UnArchive) {
                return LogicalUnArchive.create(ddl);
            } else if (ddl instanceof InspectIndex) {
                return LogicalInspectIndex.create((InspectIndex) ddl);
            } else if (ddl instanceof CreateJoinGroup) {
                return LogicalCreateJoinGroup.create((CreateJoinGroup) ddl);
            } else if (ddl instanceof DropJoinGroup) {
                return LogicalDropJoinGroup.create(ddl);
            } else if (ddl instanceof ConvertAllSequences) {
                return LogicalConvertAllSequences.create((ConvertAllSequences) ddl);
            }

            if (ddl instanceof SequenceDdl) {
                return LogicalSequenceDdl.create((SequenceDdl) ddl);
            }

            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                "operation " + ddl.getSqlNode().getKind());
        }
    }

    private boolean isSupportedByNewDdlEngine(DDL ddl) {
        return ddl.kind().belongsTo(SqlKind.DDL_SUPPORTED_BY_NEW_ENGINE);
    }

    private RelNode convertCreateMaterializedView(final CreateMaterializedView ddl) {
        CreateMaterializedView newView = (CreateMaterializedView) super.visit(ddl);
        return LogicalCreateMaterializedView.createMaterializedView(
            newView, newView.getTraitSet().replace(DrdsConvention.NONE), ddl.bRefresh);
    }

    private RelNode convertDropMaterializedView(final DropMaterializedView ddl) {
        return new LogicalDropMaterializedView(ddl.getCluster(), ddl.getSchemaName(), ddl.getViewName(), true);
    }

    private void updateTableProperties(Map<String, TableProperties> tablePropertiesMap, RelNode scanOrLookup) {

        this.allTableSingle = (allTableSingle && tablePropertiesMap.values().stream().allMatch(
            t -> t.isSingleTable()));
        final boolean allTableInOneGroup = RelUtils.allTableInOneGroup(tablePropertiesMap);
        if (allTableSingleWithSameGroup) {
            if (!allTableInOneGroup) {
                allTableSingleWithSameGroup = false;
            }
            if (scanOrLookup != null) {
                if (scanOrLookup instanceof LogicalView) {
                    if (!((LogicalView) scanOrLookup).isSingleGroup()) {
                        allTableSingleWithSameGroup = false;
                    }
                }

                if (allTableSingleWithSameGroup && scanOrLookup instanceof TableScan) {
                    TableScan tblScan = (TableScan) scanOrLookup;
                    final List<String> qualifiedName = tblScan.getTable().getQualifiedName();
                    final String tbName = Util.last(qualifiedName);
                    // final String dbName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;

                    final TableProperties tblProps = tablePropertiesMap.get(tbName);
                    if (tblProps != null && tblProps.getPartInfo() != null) {
                        updateAllTableSingleWithSameTgFlag(tblProps.getPartInfo());
                    }
                }
            }
        }

        final boolean allTableBroadcast = RelUtils.allTableBroadcast(tablePropertiesMap);
        if (this.allTableBroadcast && !allTableBroadcast) {
            this.allTableBroadcast = false;
        }

        final boolean allTableNotBroadcast = RelUtils.allTableNotBroadcast(tablePropertiesMap);
        if (this.allTableSingleNoBroadcast && !(allTableSingleWithSameGroup && allTableNotBroadcast)) {
            this.allTableSingleNoBroadcast = false;
        }
        for (Map.Entry<String, TableProperties> entry : tablePropertiesMap.entrySet()) {
            if (Engine.isFileStore(entry.getValue().getEngine())) {
                existsOSSTable = true;
            }
        }
    }

    /**
     * Check and update flag {@link #allTableSingleWithSameGroup}.
     * Initialize {@link #allTableSingleTgId} at first call
     *
     * @param tblPartInfo partition info of single table
     * @return updated allTableSingle flag value
     */
    private boolean updateAllTableSingleWithSameTgFlag(PartitionInfo tblPartInfo) {
        if (allTableSingleWithSameGroup
            && null != tblPartInfo
            && DbInfoManager.getInstance().isNewPartitionDb(tblPartInfo.getTableSchema())) {
            if (tblPartInfo.isGsiSingleOrSingleTable()) {
                Long tgId = tblPartInfo.getTableGroupId();
                if (allTableSingleTgId == null) {
                    allTableSingleTgId = tgId;
                } else {
                    /**
                     * For autodb, only the single tables in the same tablegroup are allowed to
                     * push down join
                     */
                    if (!allTableSingleTgId.equals(tgId)) {
                        allTableSingleWithSameGroup = false;
                    }
                }
            } else if (tblPartInfo.isGsiOrPartitionedTable()) {
                allTableSingleWithSameGroup = false;
            }
        }

        return allTableSingleWithSameGroup;
    }

    private RelNode handleOptimizeTable(Dal dalNode) {
        final SqlOptimizeTable optimizeTable = (SqlOptimizeTable) dalNode.getAst();
        final String defaultSchemaName = PlannerContext.getPlannerContext(dalNode).getSchemaName();

        final Map<String, List<List<String>>> targetTable = new LinkedHashMap<>();
        final List<String> tableNames = new LinkedList<>();

        /* 获得所有逻辑表 */
        for (SqlNode tableNameNode : optimizeTable.getTableNames()) {
            String tableName = RelUtils.lastStringValue(tableNameNode);
            if (tableNames.contains(tableName)) {
                continue;
            }
            tableNames.add(tableName);

            PartitionInfoUtil.getTableTopology(defaultSchemaName, tableName).forEach((db, tables) -> {
                targetTable.computeIfAbsent(db, (x) -> new ArrayList<>())
                    .addAll(tables);
            });
        }

        int tableCount = PlannerUtils.tableCount(targetTable);

        if (tableCount == 0) {
            throw new IllegalArgumentException("Can't find proper actual target!");
        }

        SqlOptimizeTable newSqlNode = new SqlOptimizeTable(optimizeTable.getParserPosition(),
            ImmutableList.of(optimizeTable.getTableName()),
            optimizeTable.isNoWriteToBinlog(),
            optimizeTable.isLocal());
        String schemaName = newSqlNode.getDbName() != null ? newSqlNode.getDbName().toString() : null;
        PhyDal phyDal = new PhyDal(dalNode.getCluster(),
            dalNode.getTraitSet(),
            newSqlNode,
            dalNode.getRowType(),
            targetTable,
            tableNames,
            schemaName);
        if (tableCount == 1) {
            return phyDal;
        }

        return Gather.create(phyDal);
    }

    public LogicalView getBaseLogicalView() {
        if (tableNames.size() > 0) {
            baseLogicalView.setTableName(tableNames);
        }
        return baseLogicalView;
    }

    public boolean isDirectInTheSameDB() {
        return allTableSingleWithSameGroup && baseLogicalView != null && schemaNames.size() <= 1;
    }

    public boolean isDirectInDifferentDB() {
        boolean ret = allTableSingle && baseLogicalView != null;
        if (ret) {
            String lastStorageId = null;
            for (Map<Long, String> storages : tableStorages) {
                if (storages == null || storages.isEmpty()) {
                    ret = false;
                    return ret;
                }
                for (String id : storages.values()) {
                    if (lastStorageId == null) {
                        lastStorageId = id;
                    } else {
                        if (!lastStorageId.equalsIgnoreCase(id)) {
                            ret = false;
                            return ret;
                        }
                    }
                }
            }
        }
        return ret;
    }

    public List<String> getSchemaNames() {
        return schemaNames;
    }

    @VisibleForTesting
    public boolean isAllTableSingle() {
        return allTableSingle;
    }

    public boolean isAllTableBroadcast() {
        return allTableSingleWithSameGroup && allTableBroadcast;
    }

    public boolean isAllTableSingleNoBroadcast() {
        return allTableSingleNoBroadcast;
    }

    public boolean isShouldRemoveSchemaName() {
        return shouldRemoveSchemaName;
    }

    /**
     * TODO: 需要考虑 information_schema 等这些比较特殊的系统库
     */
    public void setShouldRemoveSchemaName(List<String> qualifiedName) {
        if (!shouldRemoveSchemaName && qualifiedName.size() == 2) {

            String dbName = qualifiedName.get(0);

            if (systemDbName.contains(dbName)) {
                shouldRemoveSchemaName = false;
            } else {
                shouldRemoveSchemaName = true;

            }
        }

    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public boolean isWithIndexHint() {
        return withIndexHint;
    }

    public boolean isModifyBroadcastTable() {
        return modifyBroadcastTable;
    }

    public boolean isOnlyBroadcastTable() {
        return allTableBroadcast;
    }

    public boolean isModifyGsiTable() {
        return modifyGsiTable;
    }

    public boolean isModifyForeignKey() {
        return modifyForeignKey;
    }

    public static class ReplaceTableScanInFilterSubQueryFinder extends RexShuttle {

        private final PlannerContext plannerContext;
        // Whether all tables are broadcast
        private boolean allTableBroadcast = true;
        // Whether all tables are single and in the same group and no broadcast table
        private boolean allTableSingleNoBroadcast = true;
        private boolean allTableSingle = true;
        private boolean allTableSingleWithSameGroup = true;
        private boolean containUncertainValue = false;
        private boolean containComplexExpression = false;
        private boolean existsNonPushDownFunc = false;
        private String singleDbIndex = null;
        private LogicalView baseLogicalView = null;
        private List<String> tableNames = new ArrayList<>();
        private List<Map<Long, String>> storageIds = new ArrayList<>();
        private SqlKind sqlKind;
        private LockMode lockMode = LockMode.UNDEF;
        private List<String> schemaNames;
        private boolean existsOSSTable;
        private boolean existsWindow = false;
        private Long allTableSingleTgId = null;

        public ReplaceTableScanInFilterSubQueryFinder(SqlKind kind, LockMode lockMode, boolean allTableSingle,
                                                      boolean allTableBroadcast, boolean allTableSingleNoBroadcast,
                                                      String singleDbIndex, List<String> schemaNames,
                                                      PlannerContext pc) {

            this.sqlKind = kind;
            this.lockMode = lockMode;
            this.allTableBroadcast = allTableBroadcast;
            this.allTableSingleNoBroadcast = allTableSingleNoBroadcast;
            this.allTableSingleWithSameGroup = allTableSingleWithSameGroup;
            this.singleDbIndex = singleDbIndex;
            this.schemaNames = schemaNames;
            this.plannerContext = pc;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            /**
             * Do not support UPDATE and DELETE with subQuery
             */
            containComplexExpression = true;
            if (sqlKind == SqlKind.UPDATE || sqlKind == SqlKind.DELETE) {
                if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                    throw new TddlRuntimeException(ERR_DML_WITH_SUBQUERY);
                }
            }

            ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
            visitor.plannerContext = plannerContext;
            visitor.lockMode = lockMode;
            visitor.allTableSingleWithSameGroup = allTableSingleWithSameGroup;
            visitor.allTableSingle = allTableSingle;
            visitor.singleDbIndex = singleDbIndex;
            visitor.schemaNames = this.schemaNames;
            visitor.allTableBroadcast = allTableBroadcast;
            visitor.allTableSingleNoBroadcast = allTableSingleNoBroadcast;
            RelNode r = subQuery.rel.accept(visitor);
            this.allTableSingleWithSameGroup = visitor.allTableSingleWithSameGroup;
            this.allTableSingle = visitor.allTableSingle;
            this.singleDbIndex = visitor.singleDbIndex;
            this.baseLogicalView = visitor.baseLogicalView;
            this.tableNames = visitor.tableNames;
            this.allTableBroadcast = visitor.allTableBroadcast;
            this.allTableSingleNoBroadcast = visitor.allTableSingleNoBroadcast;
            this.existsOSSTable = visitor.existsOSSTable;
            this.allTableSingleTgId = visitor.allTableSingleTgId;
            this.existsNonPushDownFunc |= visitor.existsNonPushDownFunc;
            this.existsWindow |= visitor.existsWindow;

            return subQuery.clone(r);
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            checkUncertainValue(call);
            return super.visitCall(call);
        }

        private void checkUncertainValue(final RexCall call) {
            if (containUncertainValue) {
                return;
            } else {
                final SqlOperator operator = call.getOperator();
                if (operator.isDynamicFunction()) {
                    containUncertainValue = true;
                }
            }
        }

        /**
         * all tables are single, and they in the same table group.
         */
        public boolean isAllSingleTableWithSameGroup() {
            return allTableSingleWithSameGroup;
        }

        /**
         * all tables are single although they maybe from different table group and schema.
         */
        public boolean isAllSingleTable() {
            return allTableSingle;
        }

        public boolean existsOSSTable() {
            return existsOSSTable;
        }

        public boolean isAllTableBroadcast() {
            return allTableBroadcast;
        }

        public boolean isAllTableSingleNoBroadcast() {
            return allTableSingleNoBroadcast;
        }

        public String getSingleDbIndex() {
            return singleDbIndex;
        }

        public LogicalView getBaseLogicalView() {
            return baseLogicalView;
        }

        public List<String> getTableNames() {
            return tableNames;
        }

        public boolean isContainUncertainValue() {
            return containUncertainValue;
        }

        public boolean isContainComplexExpression() {
            return containComplexExpression;
        }

        public boolean isExistsNonPushDownFunc() {
            return existsNonPushDownFunc;
        }

        public boolean isExistsWindow() {
            return existsWindow;
        }

        public Long getAllTableSingleTgId() {
            return allTableSingleTgId;
        }
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public List<TableProperties> getModifiedTables() {
        return modifiedTables;
    }

    public boolean isModifyShardingColumn() {
        return modifyShardingColumn;
    }

    public boolean isContainUncertainValue() {
        return containUncertainValue;
    }

    public boolean isContainComplexExpression() {
        return containComplexExpression;
    }

    public boolean isContainScaleOutWritableTable() {
        return containScaleOutWritableTable;
    }

    public boolean isContainReplicateWriableTable() {
        return containReplicateWriableTable;
    }

    /**
     * (modifyBroadcastTable && containUncertainValue) 条件：
     * DML 语句情况下，广播表如果包含不确定值，不能下推执行，例如：
     * update/delete from t where id > rand();
     * update/delete from t set time = current_time() order by rand();
     * <p>
     * 某些特殊情况似乎又能下推：(没有很好的办法识别，暂时先禁止下推)
     * update/delete where 2 > rand()
     */
    public boolean existsCannotPushDown() {
        return existsWindow ||
            existsIntersect ||
            existsMinus || existsCheckSum || existsUnpushableAgg || existsNonPushDownFunc ||
            (modifyBroadcastTable && containUncertainValue) || existsCheckSumV2 ||
            existsUnPushedDynamicValues;
    }

    public boolean isContainOnlineModifyColumnTable() {
        return containOnlineModifyColumnTable;
    }

    public boolean isContainGeneratedColumn() {
        return containGeneratedColumn;
    }

    public boolean isExistsGroupingSets() {
        return existsGroupingSets;
    }

    public boolean isModifyWithLimitOffset() {
        return modifyWithLimitOffset;
    }

    public boolean existsOSSTable() {
        return existsOSSTable;
    }
    public boolean isExistsCheckSum() {
        return existsCheckSum;
    }

    public boolean isExistsUnpushableAgg() {
        return existsUnpushableAgg;
    }

    public boolean isExistsCheckSumV2() {
        return existsCheckSumV2;
    }

    public boolean isOutFileStatistics() {
        return outFileStatistics;
    }
}
