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

package com.alibaba.polardbx.optimizer.core.function.calc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;

import java.util.List;

@SuppressWarnings("rawtypes")
public abstract class UserDefinedJavaFunction extends AbstractScalarFunction {
    protected static final Logger logger = LoggerFactory.getLogger(UserDefinedJavaFunction.class);
    protected List<DataType> inputTypes;
    protected DataType returnType;
    protected String funcName;
    protected boolean noState;

    public UserDefinedJavaFunction() {
        super(null, null);
    }

    protected UserDefinedJavaFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != inputTypes.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Parameters do not match input types");
        }

        // convert input parameters to java style, e.g. slice -> string
        for (int i = 0; i < args.length; i++) {
            DataType type = inputTypes.get(i);
            if (type instanceof VarcharType || type instanceof CharType) {
                args[i] = DataTypes.StringType.convertFrom(args[i]);
            } else if (type instanceof DecimalType) {
                args[i] = DataTypes.DecimalType.convertFrom(args[i]).toBigDecimal();
            } else if (type instanceof ULongType) {
                args[i] = DataTypes.ULongType.convertFrom(args[i]).toBigInteger();
            } else {
                args[i] = type.convertFrom(args[i]);
            }
        }

        Object result = compute(args);

        return returnType.convertFrom(result);
    }

    public abstract Object compute(Object[] args);

    public void setInputTypes(List<DataType> inputTypes) {
        this.inputTypes = inputTypes;
    }

    public void setReturnType(DataType returnType) {
        this.returnType = returnType;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {funcName};
    }

    public boolean isNoState() {
        return noState;
    }

    public void setNoState(boolean noState) {
        this.noState = noState;
    }
}
