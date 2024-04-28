package com.alibaba.polardbx.optimizer.core.function.calc.scalar.synchronization;

import com.alibaba.polardbx.common.lock.LockingFunctionHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class IsMyLock extends AbstractScalarFunction {
    public IsMyLock(List<DataType> operandTypes,
                       DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"IS_MY_LOCK"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        String lockName = DataTypes.StringType.convertFrom(args[0]);
        LockingFunctionHandle handle = ec.getConnection().getLockHandle(ec);
        return handle.isMyLockValid(lockName);
    }
}
