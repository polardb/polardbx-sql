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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;

public class AlterTableGroupBasePreparedData extends DdlPreparedData {

    public AlterTableGroupBasePreparedData() {
    }

    private String tableGroupName;
    private String targetImplicitTableGroupName;

    /**
     * After doing alter, the phy parts of newPartitionNames
     * will be moved to table_partitions from table_partitions_delta
     */
    private List<String> newPartitionNames;
    private Map<String, String> newPartitionLocalities = new HashMap<>();

    /**
     * After doing alter, the phy parts of oldPartitionNames
     * will be removed from table_partitions_delta and table_partitions
     */
    private List<String> oldPartitionNames;

    private List<String> excludeStorageInstIds;
    private ComplexTaskMetaManager.ComplexTaskType taskType;
    /*
     * the new created partition group, it's invisible when
     * the alter tablegroup operation is not finish
     */
    private List<PartitionGroupRecord> invisiblePartitionGroups;
    private String sourceSql;

    private boolean remainInOriginalTableGroup;
    private boolean moveToExistTableGroup;
    private String targetTableGroup;
    private Map<String, Long> firstTableVersionInTargetTableGroup;
    private boolean createNewTableGroup;
    private boolean isDropVal = false;
    private boolean useTemplatePart;
    private boolean operateOnSubPartition;
    private List<String> logicalParts;

    protected boolean usePhysicalBackfill = false;

    protected List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords;

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public List<String> getNewPartitionNames() {
        return newPartitionNames;
    }

    public void setNewPartitionNames(List<String> newPartitionNames) {
        this.newPartitionNames = newPartitionNames;
    }

    public Map<String, String> getNewPartitionLocalities() {
        return newPartitionLocalities;
    }

    public void setNewPartitionLocalities(Map<String, String> newPartitionLocalities) {
        this.newPartitionLocalities = newPartitionLocalities;
    }

    public List<String> getOldPartitionNames() {
        return oldPartitionNames;
    }

    public void setOldPartitionNames(List<String> oldPartitionNames) {
        this.oldPartitionNames = oldPartitionNames;
    }

    public List<PartitionGroupRecord> getInvisiblePartitionGroups() {
        return invisiblePartitionGroups;
    }

    public void setInvisiblePartitionGroups(List<PartitionGroupRecord> invisiblePartitionGroups) {
        this.invisiblePartitionGroups = invisiblePartitionGroups;
    }

    public List<String> getExcludeStorageInstIds() {
        return excludeStorageInstIds;
    }

    public void setExcludeStorageInstIds(List<String> excludeStorageInstIds) {
        this.excludeStorageInstIds = excludeStorageInstIds;
    }

    public List<GroupDetailInfoExRecord> getTargetGroupDetailInfoExRecords() {
        return targetGroupDetailInfoExRecords;
    }

    public void setTargetGroupDetailInfoExRecords(List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords) {
        this.targetGroupDetailInfoExRecords = targetGroupDetailInfoExRecords;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public boolean isUsePhysicalBackfill() {
        return false;
    }

    public void setUsePhysicalBackfill(boolean usePhysicalBackfill) {
        throw new NotSupportException("not support physical backfill for the operation");
    }

    public void prepareInvisiblePartitionGroup() {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        int i = 0;
        int targetDbCount = targetGroupDetailInfoExRecords.size();
        Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
        int logicalPartSize = GeneralUtil.emptyIfNull(logicalParts).size();
        int j = 0;
        LocalityDesc defaultLocalityDesc = new LocalityDesc();
        if (!CollectionUtils.isEmpty(oldPartitionNames)) {
            if (tableGroupConfig.getPartitionGroupByName(oldPartitionNames.get(0)) != null) {
                defaultLocalityDesc =
                    LocalityInfoUtils.parse(
                        tableGroupConfig.getPartitionGroupByName(oldPartitionNames.get(0)).locality);
            }
        }
        do {
            for (String newPartitionName : getNewPartitionNames()) {
                PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                partitionGroupRecord.visible = 0;
                LocalityDesc localityDesc = LocalityInfoUtils.parse(newPartitionLocalities.get(newPartitionName));
                if (useTemplatePart && logicalPartSize > 0) {
                    partitionGroupRecord.partition_name = logicalParts.get(j) + newPartitionName;
                } else {
                    partitionGroupRecord.partition_name = newPartitionName;
                }
                partitionGroupRecord.tg_id = tableGroupId;
                if (localityDesc.holdEmptyLocality() && defaultLocalityDesc.holdEmptyLocality()) {
                    partitionGroupRecord.phy_db = targetGroupDetailInfoExRecords.get(i % targetDbCount).phyDbName;
                    partitionGroupRecord.locality = "";
                    i++;
                } else if (!localityDesc.holdEmptyLocality()) {
                    partitionGroupRecord.phy_db = LocalityInfoUtils.allocatePhyDb(getSchemaName(), localityDesc);
                    partitionGroupRecord.locality = localityDesc.toString();
                } else {
                    partitionGroupRecord.phy_db = LocalityInfoUtils.allocatePhyDb(getSchemaName(), defaultLocalityDesc);
                    partitionGroupRecord.locality = defaultLocalityDesc.toString();
                }

                partitionGroupRecord.pax_group_id = 0L;
                inVisiblePartitionGroups.add(partitionGroupRecord);
            }
            j++;
        } while (useTemplatePart && logicalPartSize > j);
        setInvisiblePartitionGroups(inVisiblePartitionGroups);
    }

    public ComplexTaskMetaManager.ComplexTaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(ComplexTaskMetaManager.ComplexTaskType taskType) {
        this.taskType = taskType;
    }

    public List<String> getRelatedPartitions() {
        List<String> relatedParts = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(newPartitionNames)) {
            relatedParts.addAll(newPartitionNames);
        }
        if (GeneralUtil.isNotEmpty(oldPartitionNames)) {
            relatedParts.addAll(oldPartitionNames);
        }
        return relatedParts;
    }

    public Set<String> getTargetPhysicalGroups() {
        return GeneralUtil.emptyIfNull(targetGroupDetailInfoExRecords).stream().map(GroupDetailInfoRecord::getGroupName)
            .collect(Collectors.toSet());
    }

    public boolean isRemainInOriginalTableGroup() {
        return remainInOriginalTableGroup;
    }

    public void setRemainInOriginalTableGroup(boolean remainInOriginalTableGroup) {
        this.remainInOriginalTableGroup = remainInOriginalTableGroup;
    }

    public boolean isMoveToExistTableGroup() {
        return moveToExistTableGroup;
    }

    public void setMoveToExistTableGroup(boolean moveToExistTableGroup) {
        this.moveToExistTableGroup = moveToExistTableGroup;
    }

    public String getTargetTableGroup() {
        return targetTableGroup;
    }

    public void setTargetTableGroup(String targetTableGroup) {
        this.targetTableGroup = targetTableGroup;
    }

    public boolean isCreateNewTableGroup() {
        return createNewTableGroup;
    }

    public void setCreateNewTableGroup(boolean createNewTableGroup) {
        this.createNewTableGroup = createNewTableGroup;
    }

    public Map<String, Long> getFirstTableVersionInTargetTableGroup() {
        return firstTableVersionInTargetTableGroup;
    }

    public void setFirstTableVersionInTargetTableGroup(Map<String, Long> firstTableVersionInTargetTableGroup) {
        this.firstTableVersionInTargetTableGroup = firstTableVersionInTargetTableGroup;
    }

    public boolean isDropVal() {
        return isDropVal;
    }

    public void setDropVal(boolean dropVal) {
        isDropVal = dropVal;
    }

    public boolean isUseTemplatePart() {
        return useTemplatePart;
    }

    public void setUseTemplatePart(boolean useTemplatePart) {
        this.useTemplatePart = useTemplatePart;
    }

    public List<String> getLogicalParts() {
        return logicalParts;
    }

    public void setLogicalParts(List<String> logicalParts) {
        this.logicalParts = logicalParts;
    }

    public boolean isOperateOnSubPartition() {
        return operateOnSubPartition;
    }

    public void setOperateOnSubPartition(boolean operateOnSubPartition) {
        this.operateOnSubPartition = operateOnSubPartition;
    }

    public String getTargetImplicitTableGroupName() {
        return targetImplicitTableGroupName;
    }

    public void setTargetImplicitTableGroupName(String targetImplicitTableGroupName) {
        this.targetImplicitTableGroupName = targetImplicitTableGroupName;
    }

    public void updatePrepareDate(TableGroupConfig targetTableConfig, PartitionInfo curPartitionInfo,
                                  PartitionInfo newPartitionInfo) {
        List<PartitionGroupRecord> partitionGroupRecords = targetTableConfig.getPartitionGroupRecords();
        List<PartitionSpec> partitionSpecs = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitionSpecs = newPartitionInfo.getPartitionBy().getPartitions();
        assert newPartitionSpecs.size() == partitionSpecs.size();
        List<String> newPartitionNames = new ArrayList<>();
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
        for (int i = 0; i < partitionSpecs.size(); i++) {
            if (!newPartitionSpecs.get(i).getLocation().isVisiable()) {
                String partName = partitionSpecs.get(i).getName();
                newPartitionNames.add(partName);
                inVisiblePartitionGroups.add(
                    partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(partName)).findFirst()
                        .get());
            }
        }
        setTargetTableGroup(targetTableConfig.getTableGroupRecord().tg_name);
        setNewPartitionNames(newPartitionNames);
        setInvisiblePartitionGroups(inVisiblePartitionGroups);
    }

    public void findCandidateTableGroupAndUpdatePrepareDate(TableGroupConfig curTableGroupConfig,
                                                            PartitionInfo newPartitionInfo,
                                                            List<SqlPartition> sqlPartitions,
                                                            String partitionNamePrefix,
                                                            int flag,
                                                            ExecutionContext ec) {
        findCandidateTableGroupAndUpdatePrepareDate(curTableGroupConfig, newPartitionInfo, sqlPartitions,
            partitionNamePrefix, flag, false, ec);
    }

    public void findCandidateTableGroupAndUpdatePrepareDate(TableGroupConfig curTableGroupConfig,
                                                            PartitionInfo newPartitionInfo,
                                                            List<SqlPartition> sqlPartitions,
                                                            String partitionNamePrefix,
                                                            int flag,
                                                            boolean isReorg,
                                                            ExecutionContext ec) {
        boolean withImplicitTableGroup = false;
        if (!StringUtils.isEmpty(targetImplicitTableGroupName)) {
            withImplicitTableGroup = true;
            TableGroupConfig srcTgInfo = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigById(newPartitionInfo.getTableGroupId());
            if (srcTgInfo.getTableGroupRecord().tg_name.equalsIgnoreCase(targetImplicitTableGroupName)) {
                if (srcTgInfo.getTables().size() != 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "the tablegroup：" + targetImplicitTableGroupName + " has other tables:"
                            + srcTgInfo.getTables());
                }
                setRemainInOriginalTableGroup(true);
                return;
            }
        }

        TableMeta tableMeta = ec.getSchemaManager(getSchemaName()).getTable(newPartitionInfo.getTableName());
        String primaryTableName = newPartitionInfo.getTableName();
        if (tableMeta.isGsi()) {
            primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        JoinGroupInfoRecord joinGroupInfoRecord =
            JoinGroupUtils.getJoinGroupInfoByTable(getSchemaName(), primaryTableName, null);
        String joinGroup = joinGroupInfoRecord == null ? null : joinGroupInfoRecord.joinGroupName;
        Map<String, String> newPartNamesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Map<String, String>> subNewPartNamesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        TableGroupConfig candidateTGConfig =
            PartitionInfoUtil.getTheBestTableGroupInfo(newPartitionInfo, targetImplicitTableGroupName,
                withImplicitTableGroup,
                joinGroup, partitionNamePrefix,
                flag,
                operateOnSubPartition, taskType, newPartNamesMap, subNewPartNamesMap, ec);
        if (candidateTGConfig != null) {
            setMoveToExistTableGroup(true);
            setTargetTableGroup(candidateTGConfig.getTableGroupRecord().tg_name);

            List<PartitionGroupRecord> partitionGroupRecords = candidateTGConfig.getPartitionGroupRecords();
            //List<PartitionSpec> newPartitionSpecs = newPartitionInfo.getPartitionBy().getPartitions();
            List<PartitionSpec> newPartitionSpecs = newPartitionInfo.getPartitionBy().getPhysicalPartitions();
            assert newPartitionSpecs.size() == partitionGroupRecords.size();
            List<String> newPartitionNames = new ArrayList<>();
            Set<String> newPartitionNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
            List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
                LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(getSchemaName(),
                    candidateTGConfig.getTableGroupRecord().tg_name);
            int targetDbCount = targetGroupDetailInfoExRecords.size();
            List<GroupDetailInfoExRecord> newGroupDetailInfoExRecords = new ArrayList<>();
            for (int i = 0; i < newPartitionSpecs.size(); i++) {
                if (!newPartitionSpecs.get(i).getLocation().isVisiable()) {
                    String partName = newPartitionSpecs.get(i).getName();
                    Optional<PartitionGroupRecord> partitionGroupRecord =
                        partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(partName))
                            .findFirst();
                    if (!partitionGroupRecord.isPresent()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                            "the partition group:[" + candidateTGConfig.getTableGroupRecord().getTg_name() + "."
                                + partName + "] is not exists");
                    }
                    if (!isDropVal()) {
                        /*if (isOperateOnSubPartition() && useTemplatePart) {
                            if (!newPartitionNameSet.contains(newPartitionSpecs.get(i).getTemplateName())) {
                                newPartitionNames.add(newPartitionSpecs.get(i).getTemplateName());
                                newPartitionNameSet.add(newPartitionSpecs.get(i).getTemplateName());
                            }
                        } else {
                            newPartitionNames.add(partName);
                        }*/
                        newPartitionNames.add(partName);
                        inVisiblePartitionGroups.add(partitionGroupRecord.get());
                        Optional<GroupDetailInfoExRecord> groupDetailInfoExRecord =
                            targetGroupDetailInfoExRecords.stream()
                                .filter(o -> o.phyDbName.equalsIgnoreCase(partitionGroupRecord.get().phy_db))
                                .findFirst();
                        if (!groupDetailInfoExRecord.isPresent()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD, String.format(
                                "the metadata of tableGroup[%s].[%s] is too old, physical database[%s] is not available, please retry this command",
                                candidateTGConfig.getTableGroupRecord().tg_name,
                                partitionGroupRecord.get().getPhy_db()));
                        }
                        newGroupDetailInfoExRecords.add(groupDetailInfoExRecord.get());
                    }
                }
            }
            if (!isDropVal()) {
                setNewPartitionNames(newPartitionNames);
                setInvisiblePartitionGroups(inVisiblePartitionGroups);
                setTargetGroupDetailInfoExRecords(newGroupDetailInfoExRecords);
                boolean hasSubPartition = newPartitionInfo.getPartitionBy().getSubPartitionBy() != null;
                boolean useTemplateSubPartition =
                    hasSubPartition && newPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
                if (ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION == taskType && !isOperateOnSubPartition()
                    && hasSubPartition && !useTemplateSubPartition) {
                    ((AlterTableMergePartitionPreparedData) this).setNewPhysicalPartName(newPartitionNames.get(0));
                }
            } else {
                int i = 0;
                List<GroupDetailInfoExRecord> newTargetGroupDetailInfoExRecords = new ArrayList<>();
                for (PartitionGroupRecord invisiblePartitionGroupRecord : GeneralUtil.emptyIfNull(
                    invisiblePartitionGroups)) {
                    String partName = invisiblePartitionGroupRecord.getPartition_name();
                    Optional<PartitionGroupRecord> partitionGroupRecord =
                        partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(partName))
                            .findFirst();
                    if (partitionGroupRecord.isPresent()) {
                        invisiblePartitionGroupRecord.setPhy_db(partitionGroupRecord.get().getPhy_db());
                        invisiblePartitionGroupRecord.setId(partitionGroupRecord.get().getId());
                        invisiblePartitionGroupRecord.setTg_id(partitionGroupRecord.get().getTg_id());
                        invisiblePartitionGroupRecord.setLocality(partitionGroupRecord.get().getLocality());
                        invisiblePartitionGroupRecord.setVisible(partitionGroupRecord.get().getVisible());
                        Optional<GroupDetailInfoExRecord> groupDetailInfoExRecord =
                            targetGroupDetailInfoExRecords.stream()
                                .filter(o -> o.phyDbName.equalsIgnoreCase(partitionGroupRecord.get().phy_db))
                                .findFirst();
                        if (!groupDetailInfoExRecord.isPresent()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD, String.format(
                                "the metadata of tableGroup[%s].[%s] is too old, physical database[%s] is not available, please retry this command",
                                candidateTGConfig.getTableGroupRecord().tg_name,
                                partitionGroupRecord.get().getPhy_db()));
                        }
                        newTargetGroupDetailInfoExRecords.add(groupDetailInfoExRecord.get());
                    } else {
                        newTargetGroupDetailInfoExRecords.add(targetGroupDetailInfoExRecords.get(i % targetDbCount));
                        i++;
                    }
                }
                setTargetGroupDetailInfoExRecords(newTargetGroupDetailInfoExRecords);
            }
            if (GeneralUtil.isNotEmpty(sqlPartitions) && !isReorg
                && ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION != taskType) {
                int i = 0;
                boolean hasSubPartition = newPartitionInfo.getPartitionBy().getSubPartitionBy() != null;
                for (SqlPartition sqlPartition : sqlPartitions) {
                    boolean changeNewLogicAndPhyPartName =
                        (flag & IGNORE_PARTNAME_LOCALITY) == IGNORE_PARTNAME_LOCALITY && !operateOnSubPartition
                            && hasSubPartition && GeneralUtil.isNotEmpty(newPartNamesMap) && GeneralUtil.isNotEmpty(
                            subNewPartNamesMap);
                    boolean changeNewPhyPartName =
                        (flag & IGNORE_PARTNAME_LOCALITY) == IGNORE_PARTNAME_LOCALITY && operateOnSubPartition
                            && hasSubPartition && GeneralUtil.isNotEmpty(newPartNamesMap);
                    if (changeNewLogicAndPhyPartName) {
                        String partName = ((SqlIdentifier) sqlPartition.getName()).getSimple();
                        sqlPartition.setName(new SqlIdentifier(newPartNamesMap.get(partName), SqlParserPos.ZERO));
                        for (SqlNode subPartition : GeneralUtil.emptyIfNull(sqlPartition.getSubPartitions())) {
                            String subPartName =
                                ((SqlIdentifier) ((SqlSubPartition) (subPartition)).getName()).getSimple();
                            ((SqlSubPartition) (subPartition)).setName(
                                new SqlIdentifier(subNewPartNamesMap.get(partName).get(subPartName),
                                    SqlParserPos.ZERO));
                        }
                    } else if (changeNewPhyPartName) {
                        String partName = ((SqlIdentifier) sqlPartition.getName()).getSimple();
                        sqlPartition.setName(new SqlIdentifier(newPartNamesMap.get(partName), SqlParserPos.ZERO));
                    } else if (!hasSubPartition) {
                        sqlPartition.setName(new SqlIdentifier(newPartitionNames.get(i), SqlParserPos.ZERO));
                        i++;
                    }
                }
            }

            assert candidateTGConfig.getTableCount() > 0;
            String tableInTargetTableGroup = candidateTGConfig.getTables().get(0);
            TableMeta tm = ec.getSchemaManager(getSchemaName()).getTable(tableInTargetTableGroup);
            if (tm.isGsi()) {
                tableInTargetTableGroup = tm.getGsiTableMetaBean().gsiMetaBean.tableName;
                tm = ec.getSchemaManager(getSchemaName()).getTable(tableInTargetTableGroup);
            }
            Map<String, Long> tableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            tableVersions.put(tableInTargetTableGroup, tm.getVersion());
            this.setFirstTableVersionInTargetTableGroup(tableVersions);
        } else if (curTableGroupConfig.getTableCount() == 1) {
            setRemainInOriginalTableGroup(true);
        } else {
            setCreateNewTableGroup(true);
        }
    }

    public boolean needFindCandidateTableGroup() {
        TableGroupConfig implicitTableGroupConfig =
            TStringUtil.isEmpty(this.getTargetImplicitTableGroupName()) ? null :
                OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
                    .getTableGroupConfigByName(this.getTargetImplicitTableGroupName());
        return TStringUtil.isEmpty(this.getTargetImplicitTableGroupName()) || (implicitTableGroupConfig != null
            && !implicitTableGroupConfig.isEmpty());
    }
}
