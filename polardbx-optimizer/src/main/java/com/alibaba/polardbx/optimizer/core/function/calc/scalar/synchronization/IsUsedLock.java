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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.synchronization;

import com.alibaba.polardbx.common.lock.LockingFunctionHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.sql.SQLException;
import java.util.List;

public class IsUsedLock extends AbstractScalarFunction {
    public IsUsedLock(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        String lockName = DataTypes.StringType.convertFrom(args[0]);
        LockingFunctionHandle lockHandle = ec.getConnection().getLockHandle(ec);
        return lockHandle.isUsedLock(lockName);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"IS_USED_LOCK"};
    }
}
