package com.alibaba.polardbx.optimizer.core.function.calc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.AbstractDataType;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;

import java.util.List;

@SuppressWarnings("rawtypes")
public abstract class UserDefinedJavaFunction extends AbstractScalarFunction {
  protected static final Logger logger = LoggerFactory.getLogger(UserDefinedJavaFunction.class);
  protected List<DataType> userInputType;
  protected DataType userResultType;

  protected UserDefinedJavaFunction(List<DataType> operandTypes, DataType resultType) {
    super(operandTypes, resultType);
  }

  @Override
  public Object compute(Object[] args, ExecutionContext ec) {
    if (userInputType.isEmpty()) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Need input type");
    }
    if (args.length != userInputType.size()) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Parameters do not match input types");
    }

    //对入参进行处理
    for (int i = 0; i < args.length; i++) {
      DataType type = userInputType.get(i);

      if (type instanceof VarcharType || type instanceof CharType) {
        args[i] = DataTypes.StringType.convertFrom(args[i]);
        continue;
      }

      args[i] = type.convertFrom(args[i]);
    }
    return resultType.convertFrom(compute(args));
  }

  //用户复写方法
  public abstract Object compute(Object[] input);

  public void setUserInputType(List<DataType> userInputType) {
    this.userInputType = userInputType;
  }

  public void setUserResultType(DataType userResultType) {
    this.userResultType = userResultType;
  }
}
