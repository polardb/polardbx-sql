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

import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateTableGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCreateTableGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author luoyanxin
 */
public class CreateTableGroupJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final CreateTableGroupPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public CreateTableGroupJobFactory(DDL ddl, CreateTableGroupPreparedData preparedData,
                                      ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    public static ExecutableDdlJob create(DDL ddl, CreateTableGroupPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new CreateTableGroupJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected ExecutableDdlJob doCreate() {

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<DdlTask> taskList = new ArrayList<>();

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigByName(preparedData.getTableGroupName());

        if (preparedData.isIfNotExists() && tableGroupConfig != null) {
            return new TransientDdlJob();
        }
        CreateTableGroupValidateTask createTableGroupValidateTask =
            new CreateTableGroupValidateTask(preparedData.getSchemaName(),
                Lists.newArrayList(preparedData.getTableGroupName()));
        taskList.add(createTableGroupValidateTask);
        CreateTableGroupAddMetaTask createTableGroupAddMetaTask = new CreateTableGroupAddMetaTask(
            preparedData.getSchemaName(), preparedData.getTableGroupName(), preparedData.getLocality(),
            preparedData.getPartitionBy(), preparedData.isSingle(), false);
        taskList.add(createTableGroupAddMetaTask);
        CdcCreateTableGroupMarkTask cdcCreateTableGroupMarkTask = new CdcCreateTableGroupMarkTask(
            preparedData.getSchemaName(), preparedData.getTableGroupName()
        );
        taskList.add(cdcCreateTableGroupMarkTask);
        TableGroupSyncTask tableGroupSyncTask =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());
        taskList.add(tableGroupSyncTask);
        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    @Override
    protected void validate() {
        TableGroupValidator.validatePartitionDefs((SqlCreateTableGroup) ddl.sqlNode, executionContext);
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
