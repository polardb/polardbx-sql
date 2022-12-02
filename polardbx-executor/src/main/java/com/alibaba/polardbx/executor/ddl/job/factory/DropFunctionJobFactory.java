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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SqlDataAccess;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.DropFunctionOnAllDnTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.DropFunctionDropMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.DropFunctionSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropFunction;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlDropFunction;

import java.util.ArrayList;
import java.util.List;

public class DropFunctionJobFactory extends AbstractFunctionJobFactory {

    private final LogicalDropFunction dropFunction;

    public DropFunctionJobFactory(LogicalDropFunction dropFunction, String schema) {
        super(schema);
        this.dropFunction = dropFunction;
    }

    @Override
    protected void validate() {
        SqlDropFunction sqlDropFunction = dropFunction.getSqlDropFunction();
        String udfName = sqlDropFunction.getFunctionName();
        if (!sqlDropFunction.isIfExists() && StoredFunctionManager.getInstance().search(udfName) == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_NOT_FOUND,
                String.format("function: %s not exist", udfName));
        }
    }

    @Override
    List<DdlTask> createTasksForOneJob() {
        String functionName = dropFunction.getSqlDropFunction().getFunctionName();
        SQLCreateFunctionStatement statement = StoredFunctionManager.getInstance().search(functionName);
        if (statement == null) {
            return new ArrayList<>();
        }

        DdlTask dropMetaTask = new DropFunctionDropMetaTask(schema, null, functionName);
        DdlTask syncTask = new DropFunctionSyncTask(schema, functionName);
        if (statement.getSqlDataAccess() == SqlDataAccess.NO_SQL) {
            DdlTask dropFuncOnAllDbTask = new DropFunctionOnAllDnTask(schema, functionName);
            return Lists.newArrayList(dropFuncOnAllDbTask, dropMetaTask, syncTask);
        } else {
            return Lists.newArrayList(dropMetaTask, syncTask);
        }
    }

    public static ExecutableDdlJob dropFunction(LogicalDropFunction dropFunction, ExecutionContext ec) {

        return new DropFunctionJobFactory(dropFunction, ec.getSchemaName()).create();
    }
}