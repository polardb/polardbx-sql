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

package com.alibaba.polardbx.executor.partitionmanagement;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfoBuilder;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRefreshTopology;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PartitionUtils {

    public static SqlNode getSqlTemplate(String schemaName, String logicalTableName, String primaryTableDefinition,
                                         ExecutionContext ec) {
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
        if (tableMeta.isGsi() && !tableMeta.isClustered() && !tableMeta.getGsiTableMetaBean().gsiMetaBean.nonUnique) {
            final MySqlCreateTableStatement tableStatement =
                (MySqlCreateTableStatement) SQLUtils.parseStatements(primaryTableDefinition, JdbcConstants.MYSQL)
                    .get(0)
                    .clone();

            final MySqlUnique uniqueIndex = new MySqlUnique();
            List<String> primaryKeys = Extractor.getPrimaryKeys(tableMeta, ec);
            List<SQLSelectOrderByItem> sqlSelectOrderByItems = new ArrayList<>();
            primaryKeys.forEach(e -> {
                SQLSelectOrderByItem sqlSelectOrderByItem = new SQLSelectOrderByItem(new SQLIdentifierExpr(e));
                sqlSelectOrderByItem.setParent(uniqueIndex);
                sqlSelectOrderByItems.add(sqlSelectOrderByItem);
            });

            uniqueIndex.getIndexDefinition().setType("UNIQUE");
            uniqueIndex.getIndexDefinition().setKey(true);
            uniqueIndex.getIndexDefinition().setName(new SQLIdentifierExpr(TddlConstants.UGSI_PK_UNIQUE_INDEX_NAME));
            uniqueIndex.getIndexDefinition().getOptions().setIndexType("BTREE");
            uniqueIndex.getIndexDefinition().setParent(uniqueIndex);
            uniqueIndex.getIndexDefinition().getColumns().addAll(sqlSelectOrderByItems);
            uniqueIndex.setParent(tableStatement);
            tableStatement.getTableElementList().add(uniqueIndex);

            primaryTableDefinition = SQLUtils.toSQLString(tableStatement, com.alibaba.polardbx.druid.DbType.mysql);
        }

        final SqlCreateTable primaryTableNode = (SqlCreateTable) new FastsqlParser()
            .parse(primaryTableDefinition, ec)
            .get(0);

        updateBinaryColumnDefault(primaryTableNode, tableMeta);

        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(DefaultSchema.getSchemaName(), ec);
        return primaryTableNode.accept(visitor);
    }

    public static void updateBinaryColumnDefault(SqlCreateTable sqlCreateTable, TableMeta tableMeta) {
        // Handle binary default value
        List<Pair<SqlIdentifier, SqlColumnDeclaration>> newColDefs = new ArrayList<>();
        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            String columnName = colDef.getKey().getLastName();
            ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
            if (columnMeta.isBinaryDefault()) {
                // Replace default value with SqlBinaryStringLiteral
                SqlColumnDeclaration oldColDef = colDef.getValue();
                SqlBinaryStringLiteral newDefaultVal = SqlLiteral.createBinaryString(columnMeta.getField().getDefault(),
                    oldColDef.getDefaultVal().getParserPosition());
                SqlColumnDeclaration newColDef = new SqlColumnDeclaration(oldColDef.getParserPosition(),
                    oldColDef.getName(),
                    oldColDef.getDataType(),
                    oldColDef.getNotNull(),
                    newDefaultVal,
                    oldColDef.getDefaultExpr(),
                    oldColDef.isAutoIncrement(),
                    oldColDef.getSpecialIndex(),
                    oldColDef.getComment(),
                    oldColDef.getColumnFormat(),
                    oldColDef.getStorage(),
                    oldColDef.getReferenceDefinition(),
                    oldColDef.isOnUpdateCurrentTimestamp(),
                    oldColDef.getAutoIncrementType(),
                    oldColDef.getUnitCount(),
                    oldColDef.getUnitIndex(),
                    oldColDef.getInnerStep());
                newColDefs.add(new Pair<>(colDef.getKey(), newColDef));
            } else {
                newColDefs.add(colDef);
            }
        }
        sqlCreateTable.setColDefs(newColDefs);
    }

    /**
     * Get all the source phy tables of a logical table by alter item of alterTableGroup stmt
     * the return info is :
     * key: grp
     * val: list of phy tables
     */
    public static Map<String, Set<String>> getSourcePhyTables(SqlNode sqlNode,
                                                              String schemaName,
                                                              String logicalTable,
                                                              ExecutionContext executionContext,
                                                              boolean getSrcPhyForBackFill) {

        /**
         * <pre>
         *     key: group key
         *     val: the physical tables
         * </pre>
         */
        Map<String, Set<String>> sourcePhyTables = new HashMap<>();
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        TableMeta tableMeta = schemaManager.getTable(logicalTable);

        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigById(partitionInfo.getTableGroupId());
        if (sqlNode instanceof SqlAlterTableGroupSplitPartition) {
            final SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
                (SqlAlterTableGroupSplitPartition) sqlNode;
            String splitPartitionName =
                Util.last(((SqlIdentifier) (sqlAlterTableGroupSplitPartition.getSplitPartitionName())).names);
            PartitionGroupRecord splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(o -> o.partition_name.equalsIgnoreCase(splitPartitionName)).findFirst().orElse(null);
            if (splitPartRecord == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    "the partition:" + splitPartitionName + " is not exists this current table group");
            }
            PartitionSpec
                partitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getLocation().getPartitionGroupId().longValue() == splitPartRecord.id.longValue())
                .findFirst().orElse(null);
            if (partitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    "the partition:" + splitPartitionName + " is not exists this table:" + logicalTable);
            }
            Set<String> phyTables = new HashSet<>();
            phyTables.add(partitionSpec.getLocation().getPhyTableName());
            sourcePhyTables.put(partitionSpec.getLocation().getGroupKey(), phyTables);
        } else if (sqlNode instanceof SqlAlterTableGroupMergePartition) {
            final SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
                (SqlAlterTableGroupMergePartition) sqlNode;
            Set<String> partitionsToBeMerged = sqlAlterTableGroupMergePartition.getOldPartitions().stream()
                .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                    Collectors.toSet());
            if (partitionsToBeMerged.size() < 2) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "merge single partition is meaningless");
            }

            // check whether partitions to be merged are contiguous
            List<PartitionGroupRecord> mergeRecords = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(o -> partitionsToBeMerged.contains(o.partition_name.toLowerCase()))
                .collect(Collectors.toList());
            assert GeneralUtil.isNotEmpty(mergeRecords);
            Set<Long> partGroupIds = mergeRecords.stream().map(o -> o.id).collect(Collectors.toSet());

            List<PartitionSpec> mergePartSpecs = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> partGroupIds.contains(o.getLocation().getPartitionGroupId())).collect(
                    Collectors.toList());
            assert mergePartSpecs.size() == partitionsToBeMerged.size();
            for (PartitionSpec spec : mergePartSpecs) {
                sourcePhyTables.computeIfAbsent(spec.getLocation().getGroupKey(), k -> new HashSet<String>())
                    .add(spec.getLocation().getPhyTableName());
            }

        } else if (sqlNode instanceof SqlAlterTableGroupMovePartition) {
            final SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
                (SqlAlterTableGroupMovePartition) sqlNode;

            Set<String> partitionsToBeMoved = new TreeSet<>(String::compareToIgnoreCase);
            for (Map.Entry<SqlNode, List<SqlNode>> entry : sqlAlterTableGroupMovePartition.getInstPartitions()
                .entrySet()) {
                entry.getValue().stream().forEach(o -> partitionsToBeMoved.add(Util.last(((SqlIdentifier) (o)).names)));
            }

            List<PartitionGroupRecord> moveRecords = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(o -> partitionsToBeMoved.contains(o.partition_name))
                .collect(Collectors.toList());
            assert GeneralUtil.isNotEmpty(moveRecords);
            Set<Long> partGroupIds = moveRecords.stream().map(o -> o.id).collect(Collectors.toSet());

            List<PartitionSpec> movePartSpecs = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> partGroupIds.contains(o.getLocation().getPartitionGroupId())).collect(
                    Collectors.toList());
            assert movePartSpecs.size() == moveRecords.size();
            for (PartitionSpec spec : movePartSpecs) {
                sourcePhyTables.computeIfAbsent(spec.getLocation().getGroupKey(), k -> new HashSet<String>())
                    .add(spec.getLocation().getPhyTableName());
            }
        } else if (sqlNode instanceof SqlAlterTableGroupExtractPartition) {
            final SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
                (SqlAlterTableGroupExtractPartition) sqlNode;
            Map<SqlNode, RexNode> rexInfoCtx = sqlAlterTableGroupExtractPartition.getParent().getPartRexInfoCtx();
            List<RexNode> hotkeyExpress = new ArrayList<>();
            sqlAlterTableGroupExtractPartition.getHotKeys().forEach(o -> hotkeyExpress.add(rexInfoCtx.get(o)));

            PartitionSpec partitionSpec = PartitionTupleRouteInfoBuilder
                .getPartitionSpecByExprValues(partitionInfo, hotkeyExpress, executionContext);
            assert partitionSpec != null;
            sourcePhyTables.computeIfAbsent(partitionSpec.getLocation().getGroupKey(), k -> new HashSet<String>())
                .add(partitionSpec.getLocation().getPhyTableName());
        } else if (sqlNode instanceof SqlAlterTableSetTableGroup) {
            final SqlAlterTableSetTableGroup sqlAlterTableSetTableGroup =
                (SqlAlterTableSetTableGroup) sqlNode;
            TableGroupConfig targetTableGroupConfig =
                tableGroupInfoManager.getTableGroupConfigByName(sqlAlterTableSetTableGroup.getTargetTableGroup());
            List<PartitionGroupRecord> partitionGroupRecords = targetTableGroupConfig.getPartitionGroupRecords();
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                    .filter(o -> o.partition_name.equalsIgnoreCase(partitionSpec.getName())).findFirst().orElse(null);
                if (!partitionSpec.getLocation().getGroupKey()
                    .equalsIgnoreCase(GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db))) {
                    sourcePhyTables
                        .computeIfAbsent(partitionSpec.getLocation().getGroupKey(), k -> new HashSet<String>())
                        .add(partitionSpec.getLocation().getPhyTableName());
                }
            }
        } else if (sqlNode instanceof SqlAlterTableAddPartition) {
            // No source tables for add partition
            // so ignore
        } else if (sqlNode instanceof SqlAlterTableDropPartition) {

            // the source is the target partition name to be drop
            final SqlAlterTableDropPartition dropPartition = (SqlAlterTableDropPartition) sqlNode;

            String tgName = tableGroupConfig.getTableGroupRecord().tg_name;

            // Get target partition name to be drop
            String partNameToBeDrop = ((SqlIdentifier) dropPartition.getPartitionNames().get(0)).getLastName();

            // Find the target partition group to be drop by the target partition name
            PartitionGroupRecord targetPartGroupRecordToBeDrop = tableGroupInfoManager
                .getPartitionGroupByPartName(schemaName, tgName, partNameToBeDrop, executionContext);

            // Find the next partition group for the target partition group that is to be drop
            PartitionGroupRecord nextPartGroupRecord = tableGroupInfoManager
                .getNextNeighborPartitionGroupsByPartNames(schemaName, tgName, partNameToBeDrop, executionContext);

            Set<Long> srcPartGrpIdSet = new HashSet<>();
            if (!getSrcPhyForBackFill) {
                srcPartGrpIdSet.add(targetPartGroupRecordToBeDrop.id);
            }
            if (nextPartGroupRecord != null) {
                srcPartGrpIdSet.add(nextPartGroupRecord.id);
            }

            // The target partition spec by target partition group id 
            List<PartitionSpec> targetPartSpecs = new ArrayList<>();
            targetPartSpecs = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> srcPartGrpIdSet.contains(o.getLocation().getPartitionGroupId())).collect(
                    Collectors.toList());

            // put the groupKey and phyTbl into sourcePhyTables by PartitionSpec.location
            for (PartitionSpec spec : targetPartSpecs) {
                String grpKey = spec.getLocation().getGroupKey();
                String phyTbl = spec.getLocation().getPhyTableName();
                Set<String> tblSet = sourcePhyTables.get(grpKey);
                if (tblSet == null) {
                    tblSet = new HashSet<>();
                    sourcePhyTables.put(grpKey, tblSet);
                }
                tblSet.add(phyTbl);
            }

        } else if (sqlNode instanceof SqlAlterTableModifyPartitionValues) {

            /**
             * For modify partitions values , it need to create new phy tbl and move data
             */

            // The source is the target partition name to be modified
            final SqlAlterTableModifyPartitionValues modifyPartition = (SqlAlterTableModifyPartitionValues) sqlNode;

            // Get target partition name to be drop
            String partName = ((SqlIdentifier) modifyPartition.getPartition().getName()).getLastName();

            // FIXME: should take into account the "catch-all" partition ,such default partition
            // Find the target partition group to be modified by the target partition name
            PartitionGroupRecord targetPartGroupRecordToBeModified =
                tableGroupConfig.getPartitionGroupRecords().stream()
                    .filter(o -> o.partition_name.equalsIgnoreCase(partName)).findFirst().orElse(null);

            // the target partition spec by target partition group id 
            List<PartitionSpec> targetPartSpecs = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getLocation().getPartitionGroupId().equals(targetPartGroupRecordToBeModified.id))
                .collect(
                    Collectors.toList());

            // put the groupKey and phyTbl into sourcePhyTables by PartitionSpec.location
            for (PartitionSpec spec : targetPartSpecs) {
                String grpKey = spec.getLocation().getGroupKey();
                String phyTbl = spec.getLocation().getPhyTableName();
                Set<String> tblSet = sourcePhyTables.get(grpKey);
                if (tblSet == null) {
                    tblSet = new HashSet<>();
                    sourcePhyTables.put(grpKey, tblSet);
                }
                tblSet.add(phyTbl);
            }

        } else if (sqlNode instanceof SqlRefreshTopology) {
            int partitionCount = partitionInfo.getPartitionBy().getPartitions().size();
            int randomIndex = (int) (Math.random() * partitionCount);
            randomIndex = Math.min(randomIndex, partitionCount - 1);
            PartitionSpec spec = partitionInfo.getPartitionBy().getPartitions().get(randomIndex);
            String grpKey = spec.getLocation().getGroupKey();
            String phyTbl = spec.getLocation().getPhyTableName();
            sourcePhyTables.computeIfAbsent(grpKey, o -> new HashSet<>()).add(phyTbl);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Not support yet");
        }
        return sourcePhyTables;
    }
}
