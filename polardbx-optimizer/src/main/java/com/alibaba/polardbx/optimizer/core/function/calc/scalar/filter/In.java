package com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter;

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.RowType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @since 5.0.0
 */
public class In extends AbstractCollationScalarFunction {
    public In(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object left = args[0];
        Object right = args[1];

        if (FunctionUtils.isNull(left)) {
            return null;
        }

        if (args[1] instanceof RowValue) {
            right = ((RowValue) args[1]).getValues();
        }

        if (right instanceof List) {
            if (left instanceof RowValue) {
                if (FunctionUtils.allElementsNull((List) right)) {
                    return null; // MySQL behavior
                }
                if (!GeneralUtil.isEmpty((Collection) right)) {
                    if (((List) right).get(0) instanceof List) {
                        right = ((List) right).get(0);
                    }
                }
                Long ret = 0L;
                boolean hasNull = false;
                for (Object eachRight : (List) right) {
                    // 是否出现(id,name) in ((1,'a'),(2,'b'))
                    RowValue rightArgs = null;
                    if (eachRight instanceof RowValue) {
                        rightArgs = ((RowValue) eachRight);
                    } else {
                        rightArgs = new RowValue((List<Object>) eachRight);
                    }
                    ret = ((RowType) DataTypes.RowType).nullNotSafeEqual(left, rightArgs);
                    if (ret != null && ret == 1L) {
                        return 1L;
                    } else if (ret == null) {
                        hasNull = true;
                    }
                }
                return hasNull ? null : ret;
            } else {
                if (FunctionUtils.allElementsNull((List) right)) {
                    return null; // MySQL behavior
                }
                if (!GeneralUtil.isEmpty((Collection) right)) {
                    if (((List) right).get(0) instanceof List) {
                        right = ((List) right).get(0);
                    }
                }
                for (Object eachRight : (List) right) {
                    DataType type = getEqualType(Arrays.asList(left, eachRight));
                    if (type.compare(left, eachRight) == 0) {
                        return 1L;
                    }
                }
            }
        }
        if (FunctionUtils.atLeastOneElementsNull((List<Object>) right)) {
            return null;
        }
        return 0L;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"IN"};
    }
}
