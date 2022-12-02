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

package com.alibaba.polardbx.executor.ddl.job.factory.oss;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UnArchiveValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UnBindingArchiveTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4UnArchive;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.UnArchivePreparedData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Shi Yuxuan
 */
public class UnArchiveJobFactory extends DdlJobFactory {

    UnArchivePreparedData preparedData;
    ExecutionContext executionContext;

    public UnArchiveJobFactory(UnArchivePreparedData preparedData, ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        String schemaName = preparedData.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                "Unarchive be only be used for database in auto mode!");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        List<DdlTask> taskList = new ArrayList<>();
        List<String> tables = new ArrayList<>(preparedData.getTables().keySet());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getSchemaName(), preparedData.getTables());
        taskList.add(validateTableVersionTask);

        UnArchiveValidateTask unArchiveValidateTask =
            new UnArchiveValidateTask(preparedData.getSchemaName(), tables);
        taskList.add(unArchiveValidateTask);

        UnBindingArchiveTableMetaTask unBindingArchiveTableMetaTask =
            new UnBindingArchiveTableMetaTask(preparedData.getSchemaName(), tables);
        taskList.add(unBindingArchiveTableMetaTask);

        TablesSyncTask tablesSyncTask = new TablesSyncTask(preparedData.getSchemaName(), tables);
        taskList.add(tablesSyncTask);

        ExecutableDdlJob4UnArchive result = new ExecutableDdlJob4UnArchive();
        result.addSequentialTasks(taskList);
        result.setValidateTableVersionTask(validateTableVersionTask);
        result.setUnArchiveValidateTask(unArchiveValidateTask);
        result.setUnBindingArchiveTableMetaTask(unBindingArchiveTableMetaTask);
        result.setTablesSyncTask(tablesSyncTask);
        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String table : preparedData.getTables().keySet()) {
            resources.add(concatWithDot(preparedData.getSchemaName(), table));
        }
        if (!StringUtils.isEmpty(preparedData.getTableGroup())) {
            resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroup()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    public static ExecutableDdlJob create(UnArchivePreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new UnArchiveJobFactory(preparedData, executionContext).create();
    }
}
