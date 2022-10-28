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
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "AlterTableSetGroupAddSubTaskMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableSetGroupAddSubTaskMetaTask extends AlterTableGroupAddSubTaskMetaTask {

    protected String targetTableGroupName;
    protected String curJoinGroup;

    @JSONCreator
    public AlterTableSetGroupAddSubTaskMetaTask(String schemaName, String tableName, String tableGroupName,
                                                Long tableGroupId, String sourceSql, int status, int type,
                                                TablePartitionRecord logTableRec,
                                                List<TablePartitionRecord> partRecList,
                                                Map<String, List<TablePartitionRecord>> subPartRecInfos,
                                                String targetTableGroupName,
                                                String curJoinGroup) {
        super(schemaName, tableName, tableGroupName, tableGroupId, sourceSql, status, type, logTableRec, partRecList,
            subPartRecInfos);
        this.targetTableGroupName = targetTableGroupName;
        this.curJoinGroup = curJoinGroup;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();
        joinGroupInfoAccessor.setConnection(metaDbConnection);
        joinGroupTableDetailAccessor.setConnection(metaDbConnection);

        if (curJoinGroup != null) {
            joinGroupTableDetailAccessor.deleteJoinGroupTableDetailBySchemaTable(schemaName, tableName);
        }
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(targetTableGroupName);
        if (tableGroupConfig != null && GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables())) {
            String firstTable = tableGroupConfig.getAllTables().get(0).getTableName();
            JoinGroupTableDetailRecord joinGroupTableDetailRecord =
                joinGroupTableDetailAccessor.getJoinGroupDetailBySchemaTableName(schemaName, firstTable);
            if (joinGroupTableDetailRecord != null) {
                joinGroupTableDetailAccessor.insertJoingroupTableDetail(schemaName,
                    joinGroupTableDetailRecord.joinGroupId, tableName);
            }
        }
        super.executeImpl(metaDbConnection, executionContext);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();

        joinGroupTableDetailAccessor.setConnection(metaDbConnection);
        joinGroupInfoAccessor.setConnection(metaDbConnection);
        joinGroupTableDetailAccessor.deleteJoinGroupTableDetailBySchemaTable(schemaName, tableName);
        if (curJoinGroup != null) {
            JoinGroupInfoRecord joinGroupInfoRecord =
                joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, curJoinGroup, false);
            if (joinGroupInfoRecord != null) {
                joinGroupTableDetailAccessor.insertJoingroupTableDetail(schemaName, joinGroupInfoRecord.id, tableName);
            }
        }
        super.rollbackImpl(metaDbConnection, executionContext);
    }

}
