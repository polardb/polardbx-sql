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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */

@Getter
@TaskName(name = "AlterTableGroupRemoveTempPartitionTask")
public class AlterTableGroupRemoveTempPartitionTask extends BaseDdlTask {

    //final String tempPartition;

    final List<String> tempPartNames = new ArrayList<>();
    final List<String> parentPartNames = new ArrayList<>();
    boolean needRemoveParentPart;
    final Long tableGroupId;

//    @JSONCreator
//    public AlterTableGroupRemoveTempPartitionTask(String schemaName, String tempPartition, Long tableGroupId) {
//        super(schemaName);
//        this.tempPartition = tempPartition;
//        this.tableGroupId = tableGroupId;
//    }

//    @JSONCreator
//    public AlterTableGroupRemoveTempPartitionTask(String schemaName, List<String> tempPartNames, Long tableGroupId) {
//        super(schemaName);
//        this.tempPartNames.addAll(tempPartNames);
//        this.tableGroupId = tableGroupId;
//    }

    @JSONCreator
    public AlterTableGroupRemoveTempPartitionTask(String schemaName,
                                                  List<String> tempPartNames,
                                                  List<String> parentPartNames,
                                                  boolean needRemoveParentPart,
                                                  Long tableGroupId) {
        super(schemaName);
        this.tempPartNames.addAll(tempPartNames);
        if (parentPartNames != null) {
            this.parentPartNames.addAll(parentPartNames);
        }
        this.needRemoveParentPart = needRemoveParentPart;
        this.tableGroupId = tableGroupId;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);
        List<PartitionGroupRecord> partitionGroupRecords =
            partitionGroupAccessor.getTempPartitionGroupsByTableGroupIdAndNameListFromDelta(tableGroupId,
                tempPartNames);
        List<TablePartitionRecord> tablePartitionRecords =
            tablePartitionAccessor.getTablePartitionsByDbNameGroupId(schemaName, tableGroupId);

        for (PartitionGroupRecord record : partitionGroupRecords) {
            partitionGroupAccessor.deletePartitionGroupByIdFromDelta(record.getTg_id(), record.partition_name);

            if (!needRemoveParentPart) {
                for (TablePartitionRecord tablePartitionRecord : GeneralUtil.emptyIfNull(tablePartitionRecords)) {
                    tablePartitionAccessor.deleteTablePartitionByGidAndPartNameFromDelta(schemaName,
                        tablePartitionRecord.getTableName(), record.id,
                        record.getPartition_name());
                }

            } else {
                /**
                 * Clear all the logical partition record from table_partition_delta
                 */
                for (TablePartitionRecord tablePartitionRecord : GeneralUtil.emptyIfNull(tablePartitionRecords)) {
                    List<TablePartitionRecord> partRecList =
                        tablePartitionAccessor.getTablePartitionByGidAndPartNameFromDelta(schemaName,
                            tablePartitionRecord.getTableName(), record.id,
                            record.getPartition_name());
                    for (int i = 0; i < partRecList.size(); i++) {
                        TablePartitionRecord partRec = partRecList.get(i);
                        long partLevel = partRec.getPartLevel();
                        String schemaName = partRec.getTableSchema();
                        String tblName = partRec.getTableName();
                        if (partLevel == TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION) {
                            Long parentId = partRec.getParentId();
                            tablePartitionAccessor.deletePartitionConfigsFromDelta(schemaName, tblName, parentId);
                        }
                    }
                    /**
                     * Clear all the physical partition record from table_partition_delta
                     */
                    tablePartitionAccessor.deleteTablePartitionByGidAndPartNameFromDelta(schemaName,
                        tablePartitionRecord.getTableName(), record.id,
                        record.getPartition_name());
                }
            }
        }

        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }
}