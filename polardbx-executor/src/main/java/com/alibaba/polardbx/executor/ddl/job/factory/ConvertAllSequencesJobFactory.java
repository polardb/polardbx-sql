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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ConvertAllSequenceValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ConvertSequenceInSchemasTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcLogicalSequenceMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ConvertAllSequencesJobFactory extends DdlJobFactory {
    final List<String> schemaNames;
    final Type fromType;
    final Type toType;
    final boolean onlySingleSchema;

    final ExecutionContext executionContext;

    public ConvertAllSequencesJobFactory(List<String> schemaNames, Type fromType, Type toType, boolean onlySingleSchema,
                                         ExecutionContext executionContext) {
        this.schemaNames = schemaNames;
        this.fromType = fromType;
        this.toType = toType;
        this.onlySingleSchema = onlySingleSchema;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        resources.addAll(schemaNames);
    }

    @Override
    protected void excludeResources(Set<String> resources) {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ConvertAllSequenceValidateTask validateTask = new ConvertAllSequenceValidateTask(schemaNames, onlySingleSchema);
        ConvertSequenceInSchemasTask convertSequenceTask =
            new ConvertSequenceInSchemasTask(schemaNames, fromType, toType);
        CdcLogicalSequenceMarkTask cdcLogicalSequenceMarkTask = new CdcLogicalSequenceMarkTask(
            schemaNames.size() == 1 ? schemaNames.get(0) : SystemDbHelper.DEFAULT_DB_NAME,
            "*",
            executionContext.getOriginSql(),
            SqlKind.CONVERT_ALL_SEQUENCES
        );
        List<DdlTask> ddlTaskList = ImmutableList.of(
            validateTask,
            convertSequenceTask,
            cdcLogicalSequenceMarkTask
        );

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(ddlTaskList);
        return executableDdlJob;
    }

}
