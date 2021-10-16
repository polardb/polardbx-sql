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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * 假函数，不能参与任何运算
 *
 * @author Whisper
 */
public class Dummy extends AbstractScalarFunction {

    private String name;

    public Dummy(String name, List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
        this.name = name;
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "function " + name);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {name};
    }
}
