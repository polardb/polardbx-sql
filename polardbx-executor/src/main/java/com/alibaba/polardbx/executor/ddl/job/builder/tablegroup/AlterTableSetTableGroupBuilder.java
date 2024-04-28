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

package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AlterTableSetTableGroupBuilder extends DdlPhyPlanBuilder {

    protected final AlterTableSetTableGroupPreparedData preparedData;

    protected Map<String, Set<String>> sourceTableTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, Set<String>> targetTableTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected List<PartitionGroupRecord> newPartitionRecords = new ArrayList<>();
    private boolean onlyChangeSchemaMeta = false;
    private boolean alignPartitionNameFirst = false;
    private boolean repartition = false;
    private Map<String, String> partitionNamesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public AlterTableSetTableGroupBuilder(DDL ddl, AlterTableSetTableGroupPreparedData preparedData,
                                          ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    @Override
    public AlterTableSetTableGroupBuilder build() {
        checkAndSetChangeSchemaMeta();
        if (!onlyChangeSchemaMeta && !alignPartitionNameFirst && !repartition) {
            buildSqlTemplate();
            buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
            buildPhysicalPlans(preparedData.getTableName());
        }
        built = true;
        return this;
    }

    private void checkAndSetChangeSchemaMeta() {
        onlyChangeSchemaMeta = false;

        String tableGroupName = preparedData.getTableGroupName();
        String schemaName = preparedData.getSchemaName();
        String logicTableName = preparedData.getTableName();

        if (StringUtils.isEmpty(tableGroupName)) {
            onlyChangeSchemaMeta = true;
        } else {
            OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

            TableGroupConfig tableGroupInfo =
                optimizerContext.getTableGroupInfoManager().getTableGroupConfigByName(tableGroupName);

            if (tableGroupInfo == null && preparedData.isImplicit()) {
                onlyChangeSchemaMeta = true;
                return;
            }
            if (tableGroupInfo == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                    "tablegroup:" + tableGroupName + " does not exist");
            }

            if (GeneralUtil.isEmpty(tableGroupInfo.getAllTables())) {
                onlyChangeSchemaMeta = true;
            } else {
                TableGroupRecord tgRecord = tableGroupInfo.getTableGroupRecord();
                String tableInTbGrp = tableGroupInfo.getAllTables().get(0);

                PartitionInfo targetPartInfo =
                    executionContext.getSchemaManager(schemaName).getTable(tableInTbGrp).getPartitionInfo();
                PartitionInfo sourcePartInfo =
                    optimizerContext.getPartitionInfoManager().getPartitionInfo(logicTableName);

                PartitionByDefinition sourcePartByDef = sourcePartInfo.getPartitionBy();
                PartitionByDefinition targetPartByDef = targetPartInfo.getPartitionBy();

                PartitionByDefinition sourceSubPartByDef = sourcePartByDef.getSubPartitionBy();
                PartitionByDefinition targetSubPartByDef = targetPartByDef.getSubPartitionBy();

                boolean isNumPartsSame =
                    sourcePartByDef.getPartitions().size() == targetPartByDef.getPartitions().size();

                boolean isNumSubPartsSame =
                    isNumSubPartsSame(sourcePartByDef, targetPartByDef, sourceSubPartByDef, targetSubPartByDef);

                boolean match = isNumPartsSame && isNumSubPartsSame ?
                    PartitionInfoUtil.actualPartColsEquals(sourcePartInfo, targetPartInfo) : false;

                if (!match && !preparedData.isForce()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "the partition policy of tablegroup:" + tableGroupName + " is not match to table: "
                            + logicTableName);
                } else if (!match) {
                    sourcePartInfo = sourcePartInfo.copy();
                    sourcePartByDef = sourcePartInfo.getPartitionBy();

                    if (isNumPartsSame) {
                        // Change source partitionInfo's partitionName
                        for (int i = 0; i < targetPartByDef.getPartitions().size(); i++) {
                            PartitionSpec sourcePartSpec = sourcePartByDef.getPartitions().get(i);
                            PartitionSpec targetPartSpec = targetPartByDef.getPartitions().get(i);
                            if (!sourcePartSpec.getName().equalsIgnoreCase(targetPartSpec.getName())) {
                                partitionNamesMap.put(sourcePartSpec.getName(), targetPartSpec.getName());
                                sourcePartSpec.setName(targetPartSpec.getName());
                            }
                            if (isNumSubPartsSame && sourceSubPartByDef != null) {
                                for (int j = 0; j < sourcePartSpec.getSubPartitions().size(); j++) {
                                    PartitionSpec sourceSubPartSpec = sourcePartSpec.getSubPartitions().get(j);
                                    PartitionSpec targetSubPartSpec = targetPartSpec.getSubPartitions().get(j);
                                    if (!sourceSubPartSpec.getName().equalsIgnoreCase(targetSubPartSpec.getName())) {
                                        partitionNamesMap.put(sourceSubPartSpec.getName(), targetSubPartSpec.getName());
                                        sourceSubPartSpec.setName(targetSubPartSpec.getName());
                                    }
                                }
                            }
                        }
                    }

                    match = isNumPartsSame && isNumSubPartsSame ?
                        PartitionInfoUtil.actualPartColsEquals(sourcePartInfo, targetPartInfo) : false;

                    if (match) {
                        alignPartitionNameFirst = true;
                    } else {
                        List<ColumnMeta> sourcePartFields = sourcePartByDef.getPartitionFieldList();
                        List<List<String>> targetActualPartColumns = targetPartByDef.getAllLevelActualPartCols();
                        int actualPartColsSize = targetActualPartColumns.get(0).size();
                        if (actualPartColsSize <= sourcePartFields.size()) {
                            List<ColumnMeta> targetPartitionFields = targetPartByDef.getPartitionFieldList();
                            for (int i = 0; i < actualPartColsSize; i++) {
                                if (!PartitionInfoUtil.partitionDataTypeEquals(sourcePartFields.get(i),
                                    targetPartitionFields.get(i))) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_COLUMN_IS_NOT_MATCH,
                                        "the partition key of tablegroup:" + tableGroupName + " is not match to table: "
                                            + logicTableName);
                                }
                            }
                        }
                        if (sourceSubPartByDef != null && targetSubPartByDef != null) {
                            List<ColumnMeta> sourceSubPartFields = sourceSubPartByDef.getPartitionFieldList();
                            List<List<String>> targetActualSubPartColumns = targetPartByDef.getAllLevelActualPartCols();
                            int actualSubPartColsSize = targetActualSubPartColumns.get(1).size();
                            if (actualSubPartColsSize <= sourceSubPartFields.size()) {
                                List<ColumnMeta> targetSubPartFields = targetSubPartByDef.getPartitionFieldList();
                                for (int i = 0; i < actualSubPartColsSize; i++) {
                                    if (!PartitionInfoUtil.partitionDataTypeEquals(sourceSubPartFields.get(i),
                                        targetSubPartFields.get(i))) {
                                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_COLUMN_IS_NOT_MATCH,
                                            "the subpartition key of tablegroup:" + tableGroupName
                                                + " is not match to table: "
                                                + logicTableName);
                                    }
                                }
                            }
                        } else if (sourceSubPartByDef == null && targetSubPartByDef != null
                            || sourceSubPartByDef != null && targetSubPartByDef == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_COLUMN_IS_NOT_MATCH,
                                "the subpartition strategy of tablegroup:" + tableGroupName
                                    + " is not match to table: "
                                    + logicTableName);
                        }
                        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicTableName);
                        if (tableMeta.isGsi()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "it's not support to change the GSI's tablegroup to " + tableGroupName
                                    + " due to need repartition");
                        }
                        repartition = true;
                    }
                }

                if (!StringUtils.equals(sourcePartInfo.getLocality(), targetPartInfo.getLocality())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "the locality of tablegroup:" + tableGroupName + " does not match to locality of "
                            + logicTableName);
                }

                if (targetPartInfo.getTableGroupId().longValue() == sourcePartInfo.getTableGroupId().longValue()) {
                    onlyChangeSchemaMeta = true;
                    // Do nothing
                } else if (match) {
                    boolean allPartLocationsAreSame = true;
                    for (int i = 0; i < targetPartByDef.getPartitions().size(); i++) {
                        PartitionSpec sourcePartSpec = sourcePartByDef.getPartitions().get(i);
                        PartitionSpec targetPartSpec = targetPartByDef.getPartitions().get(i);
                        if (isNumSubPartsSame && sourceSubPartByDef != null) {
                            for (int j = 0; j < sourcePartSpec.getSubPartitions().size(); j++) {
                                PartitionSpec sourceSubPartSpec = sourcePartSpec.getSubPartitions().get(j);
                                PartitionSpec targetSubPartSpec = targetPartSpec.getSubPartitions().get(j);
                                if (!sourceSubPartSpec.getLocation().getGroupKey()
                                    .equalsIgnoreCase(targetSubPartSpec.getLocation().getGroupKey())) {
                                    allPartLocationsAreSame = false;
                                    break;
                                }
                            }
                        } else if (!sourcePartSpec.getLocation().getGroupKey()
                            .equalsIgnoreCase(targetPartSpec.getLocation().getGroupKey())) {
                            allPartLocationsAreSame = false;
                            break;
                        }
                    }
                    onlyChangeSchemaMeta = allPartLocationsAreSame;
                }

                boolean isSingleOrBrdTg = tgRecord.isSingleTableGroup() || tgRecord.isBroadCastTableGroup();
                boolean isSingleOrBrdTb =
                    sourcePartInfo.isGsiSingleOrSingleTable() || sourcePartInfo.isGsiBroadcastOrBroadcast();

                if ((repartition && isSingleOrBrdTb) || (isSingleOrBrdTb && !isSingleOrBrdTg) || (!isSingleOrBrdTb
                    && isSingleOrBrdTg)) {
                    String tbType = sourcePartInfo.isGsiSingleOrSingleTable() ? "single table" :
                        sourcePartInfo.isGsiBroadcastOrBroadcast() ? "broadcast table" : "partition table";
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("can't set the tablegroup of %s:%s to %s",
                            tbType, preparedData.getTableName(), tableGroupName));
                }
            }

            if (!onlyChangeSchemaMeta && !preparedData.isForce()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("the physical location of %s is not the same as tables in %s",
                        preparedData.getTableName(), tableGroupName));
            }
        }
    }

    private boolean isNumSubPartsSame(PartitionByDefinition sourcePartByDef,
                                      PartitionByDefinition targetPartByDef,
                                      PartitionByDefinition sourceSubPartByDef,
                                      PartitionByDefinition targetSubPartByDef) {
        boolean isNumSubPartsSame = false;

        if (sourceSubPartByDef != null && targetSubPartByDef != null) {
            // Both are two-level partitioned tables.
            boolean sourceTemplated = sourceSubPartByDef.isUseSubPartTemplate();
            boolean targetTemplated = targetSubPartByDef.isUseSubPartTemplate();

            boolean bothTemplatedOrNonTemplated =
                sourceTemplated && targetTemplated || !sourceTemplated && !targetTemplated;

            boolean areTotalNumPhyPartsSame =
                sourcePartByDef.getPhysicalPartitions().size() == targetPartByDef.getPhysicalPartitions().size();

            if (bothTemplatedOrNonTemplated && areTotalNumPhyPartsSame) {
                // Total num is the same, then check for each partition.
                List<PartitionSpec> sourceParts = sourcePartByDef.getPartitions();
                List<PartitionSpec> targetParts = targetPartByDef.getPartitions();
                if (sourceParts.size() == targetParts.size()) {
                    isNumSubPartsSame = true;
                    for (int i = 0; i < sourceParts.size(); i++) {
                        if (sourceParts.get(i).getSubPartitions().size() !=
                            targetParts.get(i).getSubPartitions().size()) {
                            isNumSubPartsSame = false;
                            break;
                        }
                    }
                }
            }
        } else if (sourceSubPartByDef == null && targetSubPartByDef == null) {
            // Both are one-level partitioned tables.
            isNumSubPartsSame = true;
        }

        return isNumSubPartsSame;
    }

    @Override
    protected void buildTableRuleAndTopology() {
    }

    @Override
    protected void buildPhysicalPlans() {
    }

    @Override
    public void buildChangedTableTopology(String schemaName, String tableName) {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(preparedData.getTableGroupName());
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();

        tableTopology = new HashMap<>();

        for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
            PartitionSpec partitionSpec = AlterTableGroupUtils.findPartitionSpec(partitionInfo, partitionGroupRecord);
            if (partitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can't find the partition:" + partitionGroupRecord.getPartition_name());
            }
            String targetPhyDb = partitionGroupRecord.getPhy_db();
            String targetGroupKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(targetPhyDb);
            if (!partitionSpec.getLocation().getGroupKey().equalsIgnoreCase(targetGroupKey)) {
                String targetGroupName = targetGroupKey;
                String sourceGroupName = partitionSpec.getLocation().getGroupKey();
                String phyTable = partitionSpec.getLocation().getPhyTableName();
                sourceTableTopology.computeIfAbsent(sourceGroupName, k -> new HashSet<>())
                    .add(partitionSpec.getLocation().getPhyTableName());
                targetTableTopology.computeIfAbsent(targetGroupName, k -> new HashSet<>())
                    .add(partitionSpec.getLocation().getPhyTableName());

                List<String> phyTables = new ArrayList<>();
                phyTables.add(phyTable);
                tableTopology.computeIfAbsent(targetGroupName, k -> new ArrayList<>()).add(phyTables);
                newPartitionRecords.add(partitionGroupRecord);
            }
        }
    }

    @Override
    protected void buildSqlTemplate() {
        String schemaName = preparedData.getSchemaName();
        String logicalTableName = preparedData.getTableName();

        PartitionInfo partitionInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
            .getPartitionInfo(logicalTableName);

        PartitionLocation location;
        PartitionSpec firstPartSpec = partitionInfo.getPartitionBy().getPartitions().get(0);
        if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            location = firstPartSpec.getSubPartitions().get(0).getLocation();
        } else {
            location = firstPartSpec.getLocation();
        }

        String createTableStr =
            AlterTableGroupUtils.fetchCreateTableDefinition(relDdl, executionContext, location.getGroupKey(),
                location.getPhyTableName(), schemaName);

        sqlTemplate =
            AlterTableGroupUtils.getSqlTemplate(schemaName, logicalTableName, createTableStr, executionContext);
    }

    public Map<String, Set<String>> getSourceTableTopology() {
        return sourceTableTopology;
    }

    public Map<String, Set<String>> getTargetTableTopology() {
        return targetTableTopology;
    }

    public List<PartitionGroupRecord> getNewPartitionRecords() {
        return newPartitionRecords;
    }

    public boolean isOnlyChangeSchemaMeta() {
        return onlyChangeSchemaMeta;
    }

    public boolean isAlignPartitionNameFirst() {
        return alignPartitionNameFirst;
    }

    public boolean isRepartition() {
        return repartition;
    }

    public Map<String, String> getPartitionNamesMap() {
        return partitionNamesMap;
    }
}

