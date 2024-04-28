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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "StorePartitionGroupLocalityTask")
public class StorePartitionGroupLocalityTask extends BaseDdlTask {

    private String tableGroupName;
    private Long tableGroupId;
    private List<Long> outDatedPartitionGroupIds;
    private List<String> newPartitionGroupNames;
    private List<String> localities;

    @JSONCreator
    public StorePartitionGroupLocalityTask(String schemaName, String tableGroupName,
                                           Long tableGroupId, List<Long> outDatedPartitionGroupIds,
                                           List<String> newPartitionGroupNames, List<String> localities) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.tableGroupId = tableGroupId;
        this.outDatedPartitionGroupIds = outDatedPartitionGroupIds;
        this.newPartitionGroupNames = newPartitionGroupNames;
        this.localities = localities;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        LocalityManager localityManager = LocalityManager.getInstance();
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        outDatedPartitionGroupIds.forEach(id -> localityManager.deleteLocalityOfPartitionGroup(id));
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        for (int i = 0; i < newPartitionGroupNames.size(); i++) {
            Long pgId = tableGroupConfig.getPartitionGroupByName(newPartitionGroupNames.get(i)).getId();
            localityManager.setLocalityOfPartitionGroup(schemaName, pgId, localities.get(i));
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        //TODO: rollback locality settings
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

}
