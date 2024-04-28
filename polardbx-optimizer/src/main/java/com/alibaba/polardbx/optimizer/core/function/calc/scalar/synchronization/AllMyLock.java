package com.alibaba.polardbx.optimizer.core.function.calc.scalar.synchronization;

import com.alibaba.polardbx.common.lock.LockingFunctionHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;
import java.util.StringJoiner;

public class AllMyLock extends AbstractScalarFunction {

    public AllMyLock(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ALL_MY_LOCK"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        LockingFunctionHandle handle = ec.getConnection().getLockHandle(ec);
        StringJoiner sj = new StringJoiner(",");
        for (String lockName : handle.getAllMyLocks()) {
            sj.add(lockName);
        }
        return sj.toString();
    }
}
