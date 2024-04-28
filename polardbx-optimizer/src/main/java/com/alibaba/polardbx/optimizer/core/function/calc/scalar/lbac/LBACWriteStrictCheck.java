package com.alibaba.polardbx.optimizer.core.function.calc.scalar.lbac;

import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class LBACWriteStrictCheck extends LBACWrite {

    public LBACWriteStrictCheck(List<DataType> operandTypes,
                                   DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LBAC_WRITE_STRICT_CHECK"};
    }

    /**
     * args只包含一个参数,即labelName
     */
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object res = super.compute(args, ec);
        if (new Long(0L).equals(res)) {
            throw new LBACException("check lbac privilege failed on row");
        }
        return args[args.length - 1];
    }

}
