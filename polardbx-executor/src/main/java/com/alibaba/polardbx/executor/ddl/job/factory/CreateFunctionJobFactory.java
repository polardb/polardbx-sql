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
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.CreateFunctionOnAllDnTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.CreateFunctionRegisterMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.CreateFunctionSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFunction;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCreateFunction;

import java.util.List;

public class CreateFunctionJobFactory extends AbstractFunctionJobFactory {

    private final LogicalCreateFunction createFunction;

    public CreateFunctionJobFactory(LogicalCreateFunction createFunction, String schema) {
        super(schema);
        this.createFunction = createFunction;
    }

    @Override
    protected void validate() {
        String udfName = createFunction.getSqlCreateFunction().getFunctionName();
        if (StoredFunctionManager.getInstance().containsFunction(udfName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_ALREADY_EXISTS,
                String.format("function: %s already exist", udfName));
        }
    }

    @Override
    List<DdlTask> createTasksForOneJob() {
        SqlCreateFunction sqlCreateFunction = createFunction.getSqlCreateFunction();
        String functionName = sqlCreateFunction.getFunctionName();
        String createContent = sqlCreateFunction.getText();
        boolean canPush = sqlCreateFunction.isCanPush();

        DdlTask addMetaTask = new CreateFunctionRegisterMetaTask(schema, null,
            functionName, createContent);
        DdlTask syncTask = new CreateFunctionSyncTask(schema, functionName, createContent, canPush);

        if (canPush) {
            String createFunctionOnDn = PLUtils.getCreateFunctionOnDn(createContent);
            DdlTask createOnAllDbTask = new CreateFunctionOnAllDnTask(schema, functionName, createFunctionOnDn);
            return Lists.newArrayList(addMetaTask, createOnAllDbTask, syncTask);
        } else {
            return Lists.newArrayList(addMetaTask, syncTask);
        }
    }

    public static ExecutableDdlJob createFunction(LogicalCreateFunction logicalCreateFunction,
                                                  ExecutionContext ec) {

        return new CreateFunctionJobFactory(logicalCreateFunction, ec.getSchemaName()).create();
    }
}
