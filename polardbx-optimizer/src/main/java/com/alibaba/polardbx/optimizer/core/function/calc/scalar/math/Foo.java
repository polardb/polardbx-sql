package com.alibaba.polardbx.optimizer.core.function.calc.scalar.math;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

public class Foo extends AbstractScalarFunction {
  public Foo(List<DataType> operandTypes, DataType resultType) {
    super(operandTypes, resultType);
  }

  @Override
  public Object compute(Object[] args, ExecutionContext ec) {

    if (FunctionUtils.isNull(args[0])) {
      return null;
    }

    Double d = DataTypes.DoubleType.convertFrom(args[0]);
    if (d > 1 || d < -1) {
      return null;
    } else {
      return Math.acos(d);
    }
  }

  @Override
  public String[] getFunctionNames() {
    return new String[] {"Foo"};
  }
}

