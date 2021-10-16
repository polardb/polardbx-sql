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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabasesPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveDatabasesJobFactory extends DdlJobFactory {

    private final DDL ddl;
    private final MoveDatabasesPreparedData preparedData;
    private final ExecutionContext executionContext;

    public MoveDatabasesJobFactory(DDL ddl, MoveDatabasesPreparedData preparedData,
                                   ExecutionContext executionContext) {
        this.ddl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        String defaultSchemaName = preparedData.getSchemaName();
        EmptyTask emptyTask = new EmptyTask(defaultSchemaName);
        executableDdlJob.addTask(emptyTask);
        executableDdlJob.labelAsHead(emptyTask);
        EmptyTask tailTask = new EmptyTask(defaultSchemaName);
        executableDdlJob.labelAsTail(tailTask);

        boolean stayAtPublic = true;
        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.SCALE_OUT_FINAL_TABLE_STATUS_DEBUG);
        DdlTask pauseCurrentJobTask = new PauseCurrentJobTask(defaultSchemaName);
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }
        if (!stayAtPublic) {
            executableDdlJob.addTask(pauseCurrentJobTask);
        }
        for (Map.Entry<String, Map<String, List<String>>> entry : preparedData.getLogicalDbStorageGroups()
            .entrySet()) {
            MoveDatabasePreparedData moveDatabasePreparedData =
                new MoveDatabasePreparedData(entry.getKey(), entry.getValue(), preparedData.getSourceSql());
            ExecutableDdlJob dbExecDdlJob =
                MoveDatabaseJobFactory.create(ddl, moveDatabasePreparedData, executionContext);
            executableDdlJob.combineTasks(dbExecDdlJob);
            executableDdlJob.addTaskRelationship(emptyTask, dbExecDdlJob.getHead());
            if (!stayAtPublic) {
                executableDdlJob.addTaskRelationship(dbExecDdlJob.getTail(), pauseCurrentJobTask);
            }
            executableDdlJob.getExcludeResources().addAll(dbExecDdlJob.getExcludeResources());
            executableDdlJob.addTaskRelationship(dbExecDdlJob.getTail(), tailTask);
        }
        executableDdlJob.setMaxParallelism(ScaleOutUtils.getScaleoutTaskParallelism(executionContext));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          MoveDatabasesPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new MoveDatabasesJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
