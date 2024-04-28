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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreatePartitionTableBuilder extends CreateTableBuilder {

    final PartitionTableType tblType;

    public CreatePartitionTableBuilder(DDL ddl, CreateTablePreparedData preparedData, ExecutionContext executionContext,
                                       PartitionTableType tblType) {
        super(ddl, preparedData, executionContext);
        this.tblType = tblType;
    }

    @Override
    public void buildTableRuleAndTopology() {
        this.tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(getPartitionInfo());
        buildCreatePartitionReferenceTableTopology();
    }

    @Override
    public PartitionInfo getPartitionInfo() {
        if (partitionInfo == null) {
            partitionInfo = buildPartitionInfo();
        }
        this.tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(partitionInfo);
        return partitionInfo;
    }

    protected void rectifyPartitionInfoForImportTable(PartitionInfo partitionInfo) {
        partitionInfo.setRandomTableNamePatternEnabled(false);
        partitionInfo.setTableNamePattern(null);
        String tableName = partitionInfo.getTableName();
        PartitionSpec spec = partitionInfo.getPartitionBy().getNthPartition(1);
        spec.getLocation().setPhyTableName(tableName);
    }

    protected PartitionInfo buildPartitionInfo() {
        String tbName = null;
        TableMeta tableMeta = null;
        List<ColumnMeta> allColMetas = null;
        List<ColumnMeta> pkColMetas = null;
        String tableGroupName = preparedData.getTableGroupName() == null ? null :
            ((SqlIdentifier) preparedData.getTableGroupName()).getLastName();
        String joinGroupName = preparedData.getJoinGroupName() == null ? null :
            ((SqlIdentifier) preparedData.getJoinGroupName()).getLastName();
        PartitionInfo partitionInfo = null;
        tableMeta = preparedData.getTableMeta();
        tbName = preparedData.getTableName();
        allColMetas = tableMeta.getAllColumns();
        pkColMetas = new ArrayList<>(tableMeta.getPrimaryKey());
        LocalityDesc localityDesc = LocalityInfoUtils.parse(preparedData.getLocality().toString());
        preparedData.setLocality(localityDesc);

//        if (localityDesc != null && tblType == PartitionTableType.SINGLE_TABLE) {
//            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
//                String.format("%s with locality", tblType.getTableTypeName()));
//        }

        PartitionTableType partitionTableType = tblType;
        if (preparedData.isGsi() && preparedData.isBroadcast()) {
            partitionTableType = PartitionTableType.GSI_BROADCAST_TABLE;
        } else if (preparedData.isGsi() && !preparedData.isSharding() && preparedData.getPartitioning() == null) {
            partitionTableType = PartitionTableType.GSI_SINGLE_TABLE;
        }
        partitionInfo =
            PartitionInfoBuilder.buildPartitionInfoByPartDefAst(preparedData.getSchemaName(), tbName, tableGroupName,
                preparedData.isWithImplicitTableGroup(), joinGroupName, (SqlPartitionBy) preparedData.getPartitioning(),
                preparedData.getPartBoundExprInfo(),
                pkColMetas, allColMetas, partitionTableType, executionContext, localityDesc);
        partitionInfo.setTableType(partitionTableType);

        //
        // Set auto partition flag only on primary table.
        if (tblType == PartitionTableType.PARTITION_TABLE) {
            assert relDdl.sqlNode instanceof SqlCreateTable;
            partitionInfo.setPartFlags(
                ((SqlCreateTable) relDdl.sqlNode).isAutoPartition() ? TablePartitionRecord.FLAG_AUTO_PARTITION : 0);
            // TODO(moyi) write a builder for autoFlag
            int autoFlag = ((SqlCreateTable) relDdl.sqlNode).isAutoSplit() ?
                TablePartitionRecord.PARTITION_AUTO_BALANCE_ENABLE_ALL : 0;
            partitionInfo.setAutoFlag(autoFlag);
        }

        /**
         * 对于import table, 需要修正
         * 1. 物理表名和逻辑表名应一致
         * 2. partition中的location
         * */
        if (preparedData.isImportTable() || preparedData.isReimportTable()) {
            rectifyPartitionInfoForImportTable(partitionInfo);
        }

        return partitionInfo;
    }

    public void buildCreatePartitionReferenceTableTopology() {
        if (preparedData.getReferencedTables() != null) {
            SqlCreateTable sqlCreateTable = (SqlCreateTable) this.relDdl.getSqlNode();

            final List<Pair<String, ForeignKeyData>> refTables =
                preparedData.getAddedForeignKeys().stream().map(v -> Pair.of(v.refTableName, v))
                    .collect(Collectors.toList());

            for (ForeignKeyData data : preparedData.getAddedForeignKeys()) {
                data.setPushDown(false);
            }
            (sqlCreateTable).setPushDownForeignKeys(false);
            (sqlCreateTable).setIsAddLogicalForeignKeyOnly(isAddLogicalForeignKeyOnly());

            if (refTables.stream().allMatch(refTable ->
                pushableForeignConstraint(refTable.right.refSchema, preparedData.getTableName(), refTable,
                    sqlCreateTable))) {
                // Can push down.
                (sqlCreateTable).setPushDownForeignKeys(true);
                for (ForeignKeyData data : preparedData.getAddedForeignKeys()) {
                    data.setPushDown(true);
                }
            }

            // Remove referenced table replacement.
            if (!(sqlCreateTable).getPushDownForeignKeys()) {
                (sqlCreateTable).setLogicalReferencedTables(null);
                preparedData.setReferencedTables(null);
                return;
            }

            for (String referencedTable : preparedData.getReferencedTables()) {
                Map<String, List<List<String>>> refTopo;
                PartitionInfo refPartInfo = null;
                if (referencedTable.equals(preparedData.getTableName())) {
                    refTopo = this.tableTopology;
                } else {
                    refPartInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                        .getPartitionInfo(referencedTable);
                    refTopo = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(refPartInfo);
                }
                if (refPartInfo != null && refPartInfo.isBroadcastTable()) {
                    final String phyTable =
                        refTopo.values().stream().map(l -> l.get(0).get(0)).findFirst().orElse(null);
                    assert phyTable != null;
                    for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
                        for (List<String> l : entry.getValue()) {
                            l.add(phyTable);
                        }
                    }
                } else {
                    for (Map.Entry<String, List<List<String>>> entry : refTopo.entrySet()) {
                        final List<List<String>> partList = tableTopology.get(entry.getKey());
                        assert partList != null;
                        assert partList.size() == entry.getValue().size();
                        for (int i = 0; i < entry.getValue().size(); ++i) {
                            partList.get(i).addAll(entry.getValue().get(i));
                        }
                    }
                }
            }
        }
    }

    public boolean pushableForeignConstraint(String schema,
                                             String targetTable,
                                             Pair<String, ForeignKeyData> refTable,
                                             SqlCreateTable sqlCreateTable) {
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);
        final boolean isRefNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(refTable.right.refSchema);

        if (isNewPartDb != isRefNewPartDb) {
            return false;
        }

        //import table场景下, 肯定都是单表，所以是pushdown
        boolean isImportTable = executionContext.getParamManager().getBoolean(ConnectionParams.IMPORT_TABLE)
            || executionContext.getParamManager().getBoolean(ConnectionParams.REIMPORT_TABLE);

        if (isImportTable) {
            return true;
        }

        final PartitionInfo leftPartitionInfo = partitionInfo;
        final PartitionInfo rightPartitionInfo =
            OptimizerContext.getContext(schema).getPartitionInfoManager()
                .getPartitionInfo(refTable.left);

        if (targetTable.equals(refTable.left)) {
            return sqlCreateTable.isSingle() || sqlCreateTable.isBroadCast();
        }

        if (leftPartitionInfo.isBroadcastTable() && rightPartitionInfo.isBroadcastTable()) {
            return true;
        }

        if (leftPartitionInfo.getTableGroupId() == null || rightPartitionInfo == null
            || rightPartitionInfo.getTableGroupId() == null) {
            return false;
        } else if (!leftPartitionInfo.getTableGroupId().equals(rightPartitionInfo.getTableGroupId())) {
            return false;
        }

        return leftPartitionInfo.isSingleTable() && rightPartitionInfo.isSingleTable();
    }

}
