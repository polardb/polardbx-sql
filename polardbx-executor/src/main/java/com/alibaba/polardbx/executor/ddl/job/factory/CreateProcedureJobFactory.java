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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.procedure.CreateProcedureRegisterMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.procedure.CreateProcedureSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateProcedure;
import com.google.common.collect.Lists;

import java.util.List;

public class CreateProcedureJobFactory extends AbstractProcedureJobFactory {

    private final LogicalCreateProcedure createProcedure;

    public CreateProcedureJobFactory(LogicalCreateProcedure createProcedure, String executionSchema) {
        super(executionSchema);
        this.createProcedure = createProcedure;
    }

    @Override
    protected void validate() {
        SQLName procedureName = createProcedure.getSqlCreateProcedure().getProcedureName();
        String procedureSchema = PLUtils.getProcedureSchema(procedureName, executionSchema);
        checkSchemaExist(procedureSchema);
        if (procedureExists(procedureSchema, procedureName.getSimpleName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_ALREADY_EXISTS,
                String.format("procedure: %s.%s already exist", procedureSchema, procedureName));
        }
    }

    private void checkSchemaExist(String schema) {
        OptimizerContext oc = OptimizerContext.getContext(schema);
        if (oc == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, "unknown database: " + schema);
        }
    }

    @Override
    List<DdlTask> createTasksForOneJob() {
        SQLName procedureName = createProcedure.getSqlCreateProcedure().getProcedureName();
        String procedureSchema = PLUtils.getProcedureSchema(procedureName, executionSchema);

        DdlTask addMetaTask = new CreateProcedureRegisterMetaTask(executionSchema, null,
            procedureSchema, SQLUtils.normalize(procedureName.getSimpleName()),
            createProcedure.getSqlCreateProcedure().getText());

        DdlTask syncTask =
            new CreateProcedureSyncTask(executionSchema, procedureSchema,
                SQLUtils.normalize(procedureName.getSimpleName()));

        return Lists.newArrayList(
            addMetaTask,
            syncTask
        );
    }

    public static ExecutableDdlJob createProcedure(LogicalCreateProcedure logicalCreateProcedure,
                                                   ExecutionContext ec) {

        return new CreateProcedureJobFactory(logicalCreateProcedure, ec.getSchemaName()).create();
    }
}