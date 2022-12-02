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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
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
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class AlterTableGroupBasePreparedData extends DdlPreparedData {

    public AlterTableGroupBasePreparedData() {
    }

    private String tableGroupName;

    private List<String> newPartitionNames;
    private List<String> oldPartitionNames;
    private List<String> excludeStorageInstIds;
    private List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords;
    private ComplexTaskMetaManager.ComplexTaskType taskType;
    /*
     * the new create partition group, it's invisible when
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

    public List<String> getOldPartitionNames() {
        return oldPartitionNames;
    }

    public void setOldPartitionNames(List<String> oldPartitionNames) {
        this.oldPartitionNames = oldPartitionNames;
    }

    public List<PartitionGroupRecord> getInvisiblePartitionGroups() {
        return invisiblePartitionGroups;
    }

    public void setInvisiblePartitionGroups(
        List<PartitionGroupRecord> invisiblePartitionGroups) {
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

    public void setTargetGroupDetailInfoExRecords(
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords) {
        this.targetGroupDetailInfoExRecords = targetGroupDetailInfoExRecords;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public void prepareInvisiblePartitionGroup() {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        int i = 0;
        int targetDbCount = targetGroupDetailInfoExRecords.size();
        Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
        for (String newPartitionName : getNewPartitionNames()) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.visible = 0;
            partitionGroupRecord.partition_name = newPartitionName;
            partitionGroupRecord.tg_id = tableGroupId;

            partitionGroupRecord.phy_db = targetGroupDetailInfoExRecords.get(i % targetDbCount).phyDbName;

            partitionGroupRecord.locality = "";
            partitionGroupRecord.pax_group_id = 0L;
            inVisiblePartitionGroups.add(partitionGroupRecord);
            i++;
        }
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

    public void setFirstTableVersionInTargetTableGroup(
        Map<String, Long> firstTableVersionInTargetTableGroup) {
        this.firstTableVersionInTargetTableGroup = firstTableVersionInTargetTableGroup;
    }

    public boolean isDropVal() {
        return isDropVal;
    }

    public void setDropVal(boolean dropVal) {
        isDropVal = dropVal;
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
        TableMeta tableMeta = ec.getSchemaManager(getSchemaName()).getTable(newPartitionInfo.getTableName());
        String primaryTableName = newPartitionInfo.getTableName();
        if (tableMeta.isGsi()) {
            primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        JoinGroupInfoRecord
            joinGroupInfoRecord = JoinGroupUtils.getJoinGroupInfoByTable(getSchemaName(), primaryTableName, null);
        String joinGroup = joinGroupInfoRecord == null ? null : joinGroupInfoRecord.joinGroupName;
        TableGroupConfig candidateTGConfig =
            PartitionInfoUtil.getTheBestTableGroupInfo(newPartitionInfo, null, joinGroup, partitionNamePrefix, flag,
                ec);
        if (candidateTGConfig != null) {
            setMoveToExistTableGroup(true);
            setTargetTableGroup(candidateTGConfig.getTableGroupRecord().tg_name);

            List<PartitionGroupRecord> partitionGroupRecords = candidateTGConfig.getPartitionGroupRecords();
            List<PartitionSpec> newPartitionSpecs = newPartitionInfo.getPartitionBy().getPartitions();
            assert newPartitionSpecs.size() == partitionGroupRecords.size();
            List<String> newPartitionNames = new ArrayList<>();
            List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
            List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
                LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(getSchemaName(),
                    candidateTGConfig.getTableGroupRecord().tg_name);
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
                        newPartitionNames.add(partName);
                        inVisiblePartitionGroups.add(partitionGroupRecord.get());
                        Optional<GroupDetailInfoExRecord> groupDetailInfoExRecord =
                            targetGroupDetailInfoExRecords.stream()
                                .filter(o -> o.phyDbName.equalsIgnoreCase(partitionGroupRecord.get().phy_db))
                                .findFirst();
                        if (!groupDetailInfoExRecord.isPresent()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                                String.format(
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
                            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                                String.format(
                                    "the metadata of tableGroup[%s].[%s] is too old, physical database[%s] is not available, please retry this command",
                                    candidateTGConfig.getTableGroupRecord().tg_name,
                                    partitionGroupRecord.get().getPhy_db()));
                        }
                        newTargetGroupDetailInfoExRecords.add(groupDetailInfoExRecord.get());
                    } else {
                        newTargetGroupDetailInfoExRecords.add(targetGroupDetailInfoExRecords.get(i));
                    }
                    i++;
                }
                setTargetGroupDetailInfoExRecords(newTargetGroupDetailInfoExRecords);
            }
            if (GeneralUtil.isNotEmpty(sqlPartitions)) {
                int i = 0;
                for (SqlPartition sqlPartition : sqlPartitions) {
                    sqlPartition.setName(new SqlIdentifier(newPartitionNames.get(i), SqlParserPos.ZERO));
                    i++;
                }
            }

            assert candidateTGConfig.getTableCount() > 0;
            String tableInTargetTableGroup = candidateTGConfig.getTables().get(0).getTableName();
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
}
