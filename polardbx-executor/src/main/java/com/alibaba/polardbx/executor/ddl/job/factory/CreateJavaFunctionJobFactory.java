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
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.CreateJavaFunctionRegisterMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.CreateJavaFunctionSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateJavaFunctionMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJavaFunction;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.utils.CompileUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCreateJavaFunction;

import java.util.List;
import java.util.Optional;

public class CreateJavaFunctionJobFactory extends AbstractFunctionJobFactory {

    private final LogicalCreateJavaFunction createFunction;

    public CreateJavaFunctionJobFactory(LogicalCreateJavaFunction createFunction, String schema) {
        super(schema);
        this.createFunction = createFunction;
    }

    @Override
    protected void validate() {
        checkReachedMaxNum();
        String udfName = createFunction.getSqlCreateFunction().getFuncName();
        if (JavaFunctionManager.getInstance().containsFunction(udfName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_ALREADY_EXISTS,
                String.format("function: %s already exist", udfName));
        }
        if (ExtraFunctionManager.constainsFunction(udfName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("internal function: %s already exist, please choose another name", udfName));
        }
        checkJavaCodeValid();
        checkDataTypeValid();
    }

    private void checkReachedMaxNum() {
        long maxUdfNum = InstConfUtil.getLong(ConnectionParams.MAX_JAVA_UDF_NUM);
        long udfNum = JavaFunctionManager.getInstance().getFuncNum();
        if (udfNum >= maxUdfNum) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format(
                    "number of java udf reached %s, you can use the following command to increase the max number, set global MAX_JAVA_UDF_NUM = @higher_value",
                    maxUdfNum));
        }
    }

    private void checkJavaCodeValid() {
        SqlCreateJavaFunction sqlCreateFunction = createFunction.getSqlCreateFunction();
        String javaCode = sqlCreateFunction.getJavaCode();
        String functionName = sqlCreateFunction.getFuncName();
        String className = StringUtils.funcNameToClassName(functionName);
        if (InstConfUtil.getBool(ConnectionParams.CHECK_INVALID_JAVA_UDF)) {
            CompileUtils.checkInvalidJavaCode(javaCode, className);
        }
    }

    private void checkDataTypeValid() {
        SqlCreateJavaFunction sqlCreateFunction = createFunction.getSqlCreateFunction();
        String returnType = sqlCreateFunction.getReturnType();
        // validate return type
        SQLDataType returnDataType = FastsqlUtils.parseDataType(returnType).get(0);
        DataTypeUtil.createBasicSqlType(TddlRelDataTypeSystemImpl.getInstance(), returnDataType);
        String inputTypes = Optional.ofNullable(sqlCreateFunction.getInputTypes())
            .map(types -> String.join(",", types)).orElse("");
        if (!org.apache.commons.lang.StringUtils.isEmpty(inputTypes)) {
            // validate input types
            List<SQLDataType> inputDataTypes = FastsqlUtils.parseDataType(inputTypes);
            for (SQLDataType type : inputDataTypes) {
                DataTypeUtil.createBasicSqlType(TddlRelDataTypeSystemImpl.getInstance(), type);
            }
        }
    }

    @Override
    List<DdlTask> createTasksForOneJob() {
        SqlCreateJavaFunction sqlCreateFunction = createFunction.getSqlCreateFunction();
        String functionName = sqlCreateFunction.getFuncName();
        String javaCode = sqlCreateFunction.getJavaCode();
        String returnType = sqlCreateFunction.getReturnType();
        List<String> inputTypes = sqlCreateFunction.getInputTypes();
        boolean noState = sqlCreateFunction.isNoState();

        DdlTask addMetaTask = new CreateJavaFunctionRegisterMetaTask(schema, null,
            functionName, javaCode, returnType, inputTypes == null ? "" : String.join(",", inputTypes), noState);
        DdlTask cdcMarkTask = new CdcCreateJavaFunctionMarkTask(schema, functionName);
        DdlTask syncTask = new CreateJavaFunctionSyncTask(schema, functionName);

        return Lists.newArrayList(addMetaTask, syncTask, cdcMarkTask);
    }

    public static ExecutableDdlJob createFunction(LogicalCreateJavaFunction logicalCreateFunction,
                                                  ExecutionContext ec) {

        if (!ec.isSuperUserOrAllPrivileges()) {
            PrivilegeContext pc = ec.getPrivilegeContext();
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "create java udf should be super user or has all privileges", pc.getUser(), pc.getHost());
        }
        return new CreateJavaFunctionJobFactory(logicalCreateFunction, ec.getSchemaName()).create();
    }
}
