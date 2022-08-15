package com.alibaba.polardbx.optimizer.core.function.calc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;

@SuppressWarnings("rawtypes")
public abstract class UserDefinedJavaFunction extends AbstractScalarFunction {
  protected static final Logger logger = LoggerFactory.getLogger(UserDefinedJavaFunction.class);

  protected String inputClazz;
  protected String resultClazz;

  protected UserDefinedJavaFunction(String inputClazz, String resultClazz) {
    super(
        computeDataType(inputClazz) == null ? null : ImmutableList.of(computeDataType(inputClazz)),
        computeDataType(resultClazz));
    this.inputClazz = inputClazz;
    this.resultClazz = resultClazz;
  }


  @Override
  public String[] getFunctionNames() {
    return new String[0];
  }

  @Override
  public Object compute(Object[] args, ExecutionContext ec) {
    Object argument = args[0];
    //对入参进行处理
    return compute(argument);
  }

  public abstract Object compute(Object input);

  private static DataType computeDataType(String className) {
    if (className == null) return null;

    switch (className.toLowerCase()) {
      case "long":
        return DataTypes.LongType;
      case "char":
        return DataTypes.CharType;
      case "string":
        return DataTypes.VarcharType;
      case "double":
        return DataTypes.DoubleType;
      case "int":
        return DataTypes.IntegerType;
      case "short":
        return DataTypes.ShortType;
      case "byte":
        return DataTypes.BytesType;
      case "float":
        return DataTypes.FloatType;
      case "boolean":
        return DataTypes.BooleanType;
      default:
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Class not support yet " + className);
    }

//    if (boolean.class.equals(clazz)) {
//      return DataTypes.BooleanType;
//    } else if (int.class.equals(clazz) || Integer.class.equals(clazz)) {
//      return DataTypes.LongType;
//    } else if (short.class.equals(clazz)) {
//      return DataTypes.IntegerType;
//    } else if (byte.class.equals(clazz)) {
//      return DataTypes.SmallIntType;
//    } else if (float.class.equals(clazz)) {
//      return DataTypes.FloatType;
//    } else if (double.class.equals(clazz)) {
//      return DataTypes.DoubleType;
//    } else if (String.class.equals(clazz)) {
//      return DataTypes.VarcharType;
//    }
//    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Class not support yet");
  }
}
