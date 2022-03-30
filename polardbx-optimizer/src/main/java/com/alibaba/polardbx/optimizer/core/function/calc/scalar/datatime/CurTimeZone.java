package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import java.util.List;

/**
 * CUR_TIME_ZONE()
 * CUR_TIME_ZONE() returns current session Time Zone ID.
 */
public class CurTimeZone extends AbstractScalarFunction {

    public CurTimeZone(List<DataType> operandTypes,
                          DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return ec.getTimeZone().getMySqlTimeZoneName();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CUR_TIME_ZONE"};
    }
}
