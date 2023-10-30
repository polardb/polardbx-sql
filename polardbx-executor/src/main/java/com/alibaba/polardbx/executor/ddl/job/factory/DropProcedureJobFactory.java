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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.procedure.DropProcedureDropMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.procedure.DropProcedureSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.ProcedureManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropProcedure;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlDropProcedure;

import java.util.ArrayList;
import java.util.List;

public class DropProcedureJobFactory extends AbstractProcedureJobFactory {

    private final LogicalDropProcedure dropProcedure;

    private final boolean forceDrop;

    public DropProcedureJobFactory(LogicalDropProcedure dropProcedure, String executionSchema, boolean forceDrop) {
        super(executionSchema);
        this.dropProcedure = dropProcedure;
        this.forceDrop = forceDrop;
    }

    @Override
    protected void validate() {
        SqlDropProcedure sqlDropProcedure = dropProcedure.getSqlDropProcedure();
        SQLName procedureName = sqlDropProcedure.getProcedureName();
        String procedureSchema = PLUtils.getProcedureSchema(procedureName, executionSchema);

        if (!forceDrop && !sqlDropProcedure.isIfExists() && !procedureExists(procedureSchema,
            procedureName.getSimpleName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_NOT_FOUND,
                String.format("procedure: %s.%s not exist", procedureSchema, procedureName));
        }
    }

    @Override
    List<DdlTask> createTasksForOneJob() {
        SQLName procedureName = dropProcedure.getSqlDropProcedure().getProcedureName();
        String procedureSchema = PLUtils.getProcedureSchema(procedureName, executionSchema);
        String simpleName = SQLUtils.normalize(procedureName.getSimpleName());
        if (!forceDrop && ProcedureManager.getInstance().notFound(procedureSchema, simpleName)) {
            return new ArrayList<>();
        }

        DdlTask dropMetaTask = new DropProcedureDropMetaTask(executionSchema, null, procedureSchema, simpleName);
        DdlTask syncTask = new DropProcedureSyncTask(executionSchema, procedureSchema, simpleName);
        return Lists.newArrayList(dropMetaTask, syncTask);
    }

    public static ExecutableDdlJob dropProcedure(LogicalDropProcedure logicalDropProcedure,
                                                 ExecutionContext ec) {

        return new DropProcedureJobFactory(logicalDropProcedure, ec.getSchemaName(), ec.getParamManager().getBoolean(
            ConnectionParams.FORCE_DROP_PROCEDURE)).create();
    }
}