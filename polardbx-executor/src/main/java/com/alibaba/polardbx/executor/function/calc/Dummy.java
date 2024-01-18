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

package com.alibaba.polardbx.executor.function.calc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.executor.pl.PlContext;
import com.alibaba.polardbx.executor.pl.RuntimeFunction;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.FunctionReturnedException;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;

/**
 * 处理自定义函数
 *
 * @author yuehan.wcf
 */
public class Dummy extends AbstractScalarFunction {
    String functionName;

    public Dummy(String functionName, List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
        this.functionName = functionName;
    }

    public Dummy(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
        this.functionName = "dummy";
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return handle(functionName, args, ec);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {functionName};
    }

    private SQLCreateFunctionStatement getFunction(String functionName) {
        SQLCreateFunctionStatement createFunctionStatement =
            StoredFunctionManager.getInstance().search(functionName.toLowerCase());
        if (createFunctionStatement == null) {
            if (!functionName.toLowerCase().startsWith("mysql.")) {
                String udfNameInternal = "mysql." + functionName.toLowerCase();
                createFunctionStatement =
                    StoredFunctionManager.getInstance().search(udfNameInternal);
            }
            if (createFunctionStatement == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "function " + functionName);
            }
        }
        return createFunctionStatement;
    }

    private long getCursorMemLimit(ExecutionContext executionContext) {
        long functionMemoryLimit =
            Math.max(executionContext.getParamManager().getLong(ConnectionParams.PL_MEMORY_LIMIT),
                MemoryAllocatorCtx.BLOCK_SIZE);
        long cursorMemoryLimit =
            Math.max(Math.min(executionContext.getParamManager().getLong(ConnectionParams.PL_CURSOR_MEMORY_LIMIT),
                functionMemoryLimit), MemoryAllocatorCtx.BLOCK_SIZE);
        return cursorMemoryLimit;
    }

    public Object handle(String functionName, Object[] args, ExecutionContext executionContext) {
        SQLCreateFunctionStatement createFunctionStatement = getFunction(functionName);

        long cursorMemoryLimit = getCursorMemLimit(executionContext);

        PlContext plContext = new PlContext(cursorMemoryLimit);
        plContext.setSpillMonitor(executionContext.getQuerySpillSpaceMonitor());

        RuntimeFunction function = new RuntimeFunction(createFunctionStatement, executionContext, plContext);

        Object result = null;
        try {
            function.open();

            function.init(Arrays.asList(args));

            function.run();
        } catch (FunctionReturnedException ex) {
            result = function.getResult();
        } finally {
            function.close();
        }
        try {
            return getReturnType() != DataTypes.UndecidedType ? getReturnType().convertFrom(result) : result;
        } catch (Exception ex) {
            // ignore
            logger.error("udf: convert result type failed: ", ex);
        }
        return result;
    }

}
