package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.ByteType;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DoubleType;
import com.alibaba.polardbx.optimizer.core.datatype.FloatType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ShortType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserDefinedJavaFunctionManager extends ExtraFunctionManager{
  private static final Logger logger = LoggerFactory.getLogger(UserDefinedJavaFunctionManager.class);
  public static Map<FunctionSignature, Constructor<?>> javaFunctionCaches = new HashMap<>();
  // 缓存一下dummy，避免每次都反射创建

  static {
    initFunctions();
  }

  public static void addFunction(Class type) {

    try {
      Constructor constructor = type.getConstructor(String.class, String.class);
      AbstractScalarFunction sample = (AbstractScalarFunction) constructor.newInstance(null, null);
      for (FunctionSignature signature : sample.getFunctionSignature()) {
        Constructor oldConstructor = javaFunctionCaches.put(signature, constructor);
        if (oldConstructor != null) {
          logger.warn(" dup function :" + signature.getName() + ", old class : " + oldConstructor.getClass()
              .getName());
        }
      }
    } catch (Exception e) {
      throw GeneralUtil.nestedException(e);
    }

  }

  public static Map<FunctionSignature, Constructor<?>> getFunctionCaches() {
    return javaFunctionCaches;
  }

  private static void initFunctions() {
    //do nothing
  }

  public static AbstractScalarFunction getUserDefinedJavaFunction(String functionName, List<DataType> operandTypes,
                                                                  DataType resultType) {
    String name = functionName;
    Constructor constructor = javaFunctionCaches.get(FunctionSignature.getFunctionSignature(null, name));

    if (constructor == null) return null;

    String inputClazz = computeJavaClassName(operandTypes.get(0));
    String resultClazz = computeJavaClassName(resultType);

    try {
      return (AbstractScalarFunction) constructor.newInstance(inputClazz, resultClazz);
    } catch (Exception e) {
      throw GeneralUtil.nestedException(e);
    }
  }

  public static String computeJavaClassName(DataType type) {

    if (type instanceof BooleanType) {
      return "boolean";
    }
    if (type instanceof LongType) {
      return "long";
    }
    if (type instanceof IntegerType) {
      return "int";
    }
    if (type instanceof ShortType) {
      return "short";
    }
    if (type instanceof ByteType) {
      return "byte";
    }
    if (type instanceof FloatType) {
      return "float";
    }
    if (type instanceof DoubleType) {
      return "double";
    }
    if (type instanceof VarcharType) {
      return "string";
    }
    if (type instanceof CharType) {
      return "char";
    }
    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Compute java class error");
  }

}
