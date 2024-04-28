package com.alibaba.polardbx.optimizer.core.function.calc.scalar.lbac;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class LBACRead extends LBACCheck {
    public LBACRead(List<DataType> operandTypes,
                       DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LBAC_READ"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object[] newArgs = new Object[args.length + 1];
        newArgs[0] = "read";
        System.arraycopy(args, 0, newArgs, 1, args.length);
        return super.compute(newArgs, ec);
    }
}
