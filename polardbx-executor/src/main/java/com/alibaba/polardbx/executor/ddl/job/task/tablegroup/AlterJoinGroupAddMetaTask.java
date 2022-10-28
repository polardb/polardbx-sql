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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import lombok.Getter;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@Getter
@TaskName(name = "AlterJoinGroupAddMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterJoinGroupAddMetaTask extends BaseDdlTask {

    private String joinGroupName;
    private boolean addToTargetJoinGroup;
    /**
     * key:tableGroupName
     * value:
     * key:tables be altered
     * value:all tables in current tableGroup
     */
    private Map<String, Pair<Set<String>, Set<String>>> tableGroupInfos = new TreeMap<>();

    @JSONCreator
    public AlterJoinGroupAddMetaTask(String schemaName, String joinGroupName,
                                     boolean addToTargetJoinGroup,
                                     Map<String, Pair<Set<String>, Set<String>>> tableGroupInfos) {
        super(schemaName);
        this.joinGroupName = joinGroupName;
        this.addToTargetJoinGroup = addToTargetJoinGroup;
        for (Map.Entry<String, Pair<Set<String>, Set<String>>> entry : tableGroupInfos.entrySet()) {
            Pair<Set<String>, Set<String>> tables =
                new Pair<>(new TreeSet<>(String::compareToIgnoreCase),
                    new TreeSet<>(String::compareToIgnoreCase));
            tables.getKey().addAll(entry.getValue().getKey());
            tables.getValue().addAll(entry.getValue().getValue());
            this.tableGroupInfos.put(entry.getKey(), tables);
        }
        onExceptionTryRollback();
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        joinGroupTableDetailAccessor.setConnection(metaDbConnection);
        joinGroupInfoAccessor.setConnection(metaDbConnection);

        Set<String> relatedLogicalTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Pair<Set<String>, Set<String>>> entry : tableGroupInfos.entrySet()) {
            Set<String> tableTobeAlter = entry.getValue().getKey();
            Set<String> tableInCurrentTableGroup = entry.getValue().getValue();

            JoinGroupInfoRecord joinGroupInfoRecord =
                joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, joinGroupName, true);

            Long joinGroupId = joinGroupInfoRecord.id;
            for (String tableName : tableTobeAlter) {
                TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
                if (tableMeta.isGsi()) {
                    continue;
                }
                if (!addToTargetJoinGroup) {
                    joinGroupTableDetailAccessor.deleteJoinGroupTableDetailBySchemaJoinIdTable(schemaName,
                        joinGroupId,
                        tableName);
                } else {
                    JoinGroupTableDetailRecord joinGroupTableDetailRecord =
                        joinGroupTableDetailAccessor.getJoinGroupDetailBySchemaTableName(schemaName, tableName);
                    if (joinGroupTableDetailRecord != null) {
                        joinGroupTableDetailAccessor.updateJoinGroupId(joinGroupId,
                            joinGroupTableDetailRecord.id);
                    } else {
                        joinGroupTableDetailAccessor.insertJoingroupTableDetail(schemaName, joinGroupId, tableName);
                    }
                }
            }
            if (!tableTobeAlter.equals(tableInCurrentTableGroup)) {
                addNewTableGroupInfo(tableTobeAlter, metaDbConnection, executionContext);
                for (String tableName : tableTobeAlter) {
                    TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
                    if (!tableMeta.isGsi()) {
                        relatedLogicalTables.add(tableName);
                    }
                }
            }
        }
        for (String relatedTable : relatedLogicalTables) {
            tableInfoManager.updateVersionAndNotify(schemaName, relatedTable);
        }
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private void addNewTableGroupInfo(Set<String> tableTobeAlter, Connection metaDbConnection,
                                      ExecutionContext executionContext) {
        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tableGroupAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);

        TableGroupRecord tableGroupRecord = new TableGroupRecord();
        tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;
        tableGroupRecord.schema = schemaName;
        tableGroupRecord.tg_name = String.valueOf(System.currentTimeMillis());
        tableGroupRecord.setInited(0);
        tableGroupRecord.meta_version = 1L;
        tableGroupRecord.manual_create = 0;

        Long newTableGroupId = tableGroupAccessor.addNewTableGroup(tableGroupRecord);
        String finalTgName = TableGroupNameUtil.autoBuildTableGroupName(newTableGroupId,
            tableGroupRecord.tg_type);
        List<TableGroupRecord> tableGroupRecords =
            tableGroupAccessor
                .getTableGroupsBySchemaAndName(schemaName, finalTgName,
                    false);
        if (GeneralUtil.isNotEmpty(tableGroupRecords)) {
            finalTgName = "tg" + String.valueOf(System.currentTimeMillis());
        }
        tableGroupAccessor.updateTableGroupName(newTableGroupId, finalTgName);

        String firstTableName = tableTobeAlter.iterator().next();
        PartitionInfo partitionInfo =
            executionContext.getSchemaManager(schemaName).getTable(firstTableName).getPartitionInfo();
        Long oldTableGroupId = partitionInfo.getTableGroupId();
        TableGroupConfig tableGroupConfig =
            TableGroupUtils.getTableGroupInfoByGroupId(metaDbConnection, oldTableGroupId);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        Map<Long, Long> partitionIdsMap = new HashMap<>();
        for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
            partitionGroupRecord.tg_id = newTableGroupId;
            Long newPartGroupId = partitionGroupAccessor.addNewPartitionGroup(partitionGroupRecord, false);
            partitionIdsMap.put(partitionGroupRecord.id, newPartGroupId);
        }
        for (String tableName : tableTobeAlter) {
            Optional<TablePartRecordInfoContext> tablePartRecordInfoContext =
                tableGroupConfig.getAllTables().stream().filter(o -> o.getTableName().equalsIgnoreCase(tableName))
                    .findFirst();
            if (!tablePartRecordInfoContext.isPresent()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                    String.format(
                        "the metadata of tableGroup[%s] is too old, please retry this command",
                        tableGroupConfig.getTableGroupRecord().tg_name));
            }
            TablePartitionRecord tablePartitionRecord = tablePartRecordInfoContext.get().getLogTbRec();
            tablePartitionAccessor.updateGroupIdById(newTableGroupId, tablePartitionRecord.id);
            List<TablePartitionRecord> partitionRecords = tablePartRecordInfoContext.get().getPartitionRecList();
            for (TablePartitionRecord partitionRecord : partitionRecords) {
                if (!partitionIdsMap.containsKey(partitionRecord.groupId)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format(
                            "the metadata of tableGroup[%s] is too old, please retry this command",
                            tableGroupConfig.getTableGroupRecord().tg_name));
                }
                Long newGroupId = partitionIdsMap.get(partitionRecord.groupId);
                tablePartitionAccessor.updateGroupIdById(newGroupId, partitionRecord.id);
            }
        }

    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

}
