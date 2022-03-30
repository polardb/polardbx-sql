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
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Getter
@TaskName(name = "AlterTableGroupAddMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableGroupAddMetaTask extends BaseDdlTask {

    protected String tableGroupName;
    protected String sourceSql;
    protected int type;
    protected Long tableGroupId;
    protected int status;
    protected Set<Long> outDataPartitionGroupIds;
    protected List<String> targetDbList;
    protected List<String> newPartitions;

    @JSONCreator
    public AlterTableGroupAddMetaTask(String schemaName, String tableGroupName, Long tableGroupId, String sourceSql,
                                      int status, int type, Set<Long> outDataPartitionGroupIds,
                                      List<String> targetDbList, List<String> newPartitions) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.sourceSql = sourceSql;
        this.type = type;
        this.tableGroupId = tableGroupId;
        this.status = status;
        this.outDataPartitionGroupIds = outDataPartitionGroupIds;
        this.targetDbList = targetDbList;
        this.newPartitions = newPartitions;
        assert newPartitions.size() == targetDbList.size();
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        List<String> relatedParts = new ArrayList<>();
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
        List<PartitionGroupRecord> outDataPartitionGroups = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(outDataPartitionGroupIds)) {
            for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
                if (outDataPartitionGroupIds.contains(partitionGroupRecord.getId())) {
                    outDataPartitionGroups.add(partitionGroupRecord);
                    relatedParts.add(partitionGroupRecord.partition_name);
                }
            }
            assert GeneralUtil.isNotEmpty(outDataPartitionGroups);
            TableGroupUtils.insertOldDatedPartitionGroupToDeltaTable(outDataPartitionGroups, metaDbConnection);
        }
        relatedParts.addAll(newPartitions);
        for (String relatedPart : relatedParts) {
            ComplexTaskOutlineRecord complexTaskOutlineRecord = new ComplexTaskOutlineRecord();
            complexTaskOutlineRecord.setObjectName(relatedPart);
            complexTaskOutlineRecord.setJob_id(getJobId());
            complexTaskOutlineRecord.setTableSchema(getSchemaName());
            complexTaskOutlineRecord.setTableGroupName(tableGroupName);
            complexTaskOutlineRecord.setSubTask(0);
            complexTaskOutlineRecord.setType(type);
            complexTaskOutlineRecord.setStatus(status);
            ComplexTaskMetaManager.insertComplexTask(complexTaskOutlineRecord, metaDbConnection);
        }

        addNewPartitionGroup(metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        List<String> relatedParts = new ArrayList<>();
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
        List<PartitionGroupRecord> outDataPartitionGroups = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(outDataPartitionGroupIds)) {
            for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
                if (outDataPartitionGroupIds.contains(partitionGroupRecord.getId())) {
                    relatedParts.add(partitionGroupRecord.partition_name);
                }
            }
            assert GeneralUtil.isNotEmpty(outDataPartitionGroups);
        }
        relatedParts.addAll(newPartitions);
        for (String relatedPart : relatedParts) {
            ComplexTaskMetaManager
                .deleteComplexTaskByJobIdAndObjName(getJobId(), schemaName, relatedPart, metaDbConnection);
        }
        if (GeneralUtil.isNotEmpty(outDataPartitionGroupIds)) {
            TableGroupUtils.deleteOldDatedPartitionGroupFromDeltaTableByIds(new ArrayList<>(outDataPartitionGroupIds),
                metaDbConnection);
        }
        if (GeneralUtil.isNotEmpty(newPartitions)) {
            TableGroupUtils
                .deleteNewPartitionGroupFromDeltaTableByTgIDAndPartNames(tableGroupId, newPartitions, metaDbConnection);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    public void addNewPartitionGroup(Connection metaDbConnection) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        for (int i = 0; i < newPartitions.size(); i++) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.visible = 0;
            // in case broke the rule: UNIQUE KEY `u_pname` (`tg_id`,`partition_name`,`meta_version`) in partition_group_delta
            // for drop partition, we may has two entries have the same tg_id and partition_name, i.e:
            // create tablegroup new_tg_for_add_part;
            //create table if not exists tbl_range1 (a int not null,primary key (`a`))
            //partition by range(a)
            //( partition p1 values less than (100),
            //  partition p2 values less than (200),
            //  partition p3 values less than (300)
            //);
            //alter table tbl_range1 set tablegroup=new_tg_for_add_part;
            //create table if not exists tbl_range2 (a int not null,primary key (`a`))
            //partition by range(a)
            //( partition p1 values less than (100),
            //  partition p2 values less than (200),
            //  partition p3 values less than (300)
            //);
            //alter tablegroup new_tg_for_add_part drop partition p1;
            // we will drop p1/p2 and recreate p2
            partitionGroupRecord.meta_version = 2L;
            partitionGroupRecord.partition_name = newPartitions.get(i);
            partitionGroupRecord.tg_id = tableGroupId;

            partitionGroupRecord.phy_db = targetDbList.get(i);

            partitionGroupRecord.locality = "";
            partitionGroupRecord.pax_group_id = 0L;
            partitionGroupAccessor.addNewPartitionGroup(partitionGroupRecord, true);
        }
    }

}
