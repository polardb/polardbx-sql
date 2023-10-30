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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTablesCdcSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTablesUpdateMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameTablesValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableListDataIdSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablesPreparedData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.executor.ddl.job.validator.TableValidator.validateAllowRenameMultiTable;

public class RenameTablesJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final RenameTablesPreparedData preparedData;
    private final ExecutionContext executionContext;

    public RenameTablesJobFactory(String schemaName,
                                  RenameTablesPreparedData preparedData,
                                  ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        // 校验，不能有GSI表
        for (DdlPreparedData item : preparedData.getOldTableNamePrepareDataList()) {
            validateAllowRenameMultiTable(schemaName, item.getTableName(), executionContext);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        List<DdlTask> taskList = new ArrayList<>();

        boolean enablePreemptiveMdl =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.RENAME_PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.RENAME_PREEMPTIVE_MDL_INTERVAL);

        List<String> oldNames = preparedData.getFromTableNames();
        List<String> newNames = preparedData.getToTableNames();
        DdlTask validateTask = new RenameTablesValidateTask(schemaName, oldNames, newNames);
        RenameTablesUpdateMetaTask metaTask = new RenameTablesUpdateMetaTask(schemaName, oldNames, newNames);
        RenameTablesCdcSyncTask cdcSyncTask =
            new RenameTablesCdcSyncTask(schemaName, preparedData.getDistinctNames(),
                enablePreemptiveMdl, initWait, interval, TimeUnit.MILLISECONDS,
                oldNames, newNames, preparedData.getCollate(), preparedData.getCdcMetas(),
                preparedData.getNewTableTopologies());
        TableListDataIdSyncTask tableListDataIdSyncTask = new TableListDataIdSyncTask(schemaName);

        taskList.add(validateTask);
        taskList.add(metaTask);
        // lock + cdc + sync + unlock
        taskList.add(cdcSyncTask);
        taskList.add(tableListDataIdSyncTask);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(validateTask);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String distinctName : preparedData.getDistinctNames()) {
            resources.add(concatWithDot(schemaName, distinctName));
        }
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPartDb) {
            for (String sourceTableName : preparedData.getRealSourceTables()) {
                String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, sourceTableName);
                if (tgName != null) {
                    resources.add(concatWithDot(schemaName, tgName));
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, schemaName, sourceTableName);
                }
            }
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}
