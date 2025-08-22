package com.alibaba.polardbx.executor.function.calc;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.SqlListFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.ArrayList;

public class List extends AbstractScalarFunction {
    public List(java.util.List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        java.util.List<Object> ret = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof java.util.List) {
                ret.addAll((java.util.List<Object>) arg);
            } else {
                ret.add(arg);
            }
        }
        return ret;
    }

    public Object evaluateResult(Object[] args, ExecutionContext ec) {
        return compute(args, ec);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {SqlListFunction.name};
    }
}