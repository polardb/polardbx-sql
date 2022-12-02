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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.pl.ProcedureManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class AbstractProcedureJobFactory extends DdlJobFactory {
    protected final String executionSchema;

    AbstractProcedureJobFactory(String executionSchema) {
        this.executionSchema = executionSchema;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<DdlTask> taskList = new ArrayList<>();
        taskList.addAll(createTasksForOneJob());
        if (taskList.isEmpty()) {
            return new TransientDdlJob();
        }
        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    abstract List<DdlTask> createTasksForOneJob();

    protected boolean procedureExists(String procedureSchema, String procedureName) {
        return ProcedureManager.getInstance().search(procedureSchema, SQLUtils.normalize(procedureName)) != null;
    }
}
