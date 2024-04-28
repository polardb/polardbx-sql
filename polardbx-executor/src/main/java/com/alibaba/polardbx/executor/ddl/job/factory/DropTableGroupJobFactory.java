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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableGroupRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.EmptyTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropTableGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author luoyanxin
 */
public class DropTableGroupJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final DropTableGroupPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public DropTableGroupJobFactory(DDL ddl, DropTableGroupPreparedData preparedData,
                                    ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    public static ExecutableDdlJob create(DDL ddl, DropTableGroupPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new DropTableGroupJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected ExecutableDdlJob doCreate() {

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<DdlTask> taskList = new ArrayList<>();

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigByName(preparedData.getTableGroupName());

        if (preparedData.isIfExists() && tableGroupConfig == null) {
            return new TransientDdlJob();
        }
        EmptyTableGroupValidateTask dropTableGroupValidateTask =
            new EmptyTableGroupValidateTask(preparedData.getSchemaName(),
                preparedData.getTableGroupName());
        taskList.add(dropTableGroupValidateTask);
        DropTableGroupRemoveMetaTask dropTableGroupRemoveMetaTask = new DropTableGroupRemoveMetaTask(
            preparedData.getSchemaName(), preparedData.getTableGroupName());
        taskList.add(dropTableGroupRemoveMetaTask);
        CdcDropTableGroupMarkTask cdcDropTableGroupMarkTask = new CdcDropTableGroupMarkTask(
            preparedData.getSchemaName(), preparedData.getTableGroupName()
        );
        taskList.add(cdcDropTableGroupMarkTask);
        TableGroupSyncTask tableGroupSyncTask =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());
        taskList.add(tableGroupSyncTask);
        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
