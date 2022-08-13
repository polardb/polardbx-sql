package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.ClassFinder;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.ByteType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DoubleType;
import com.alibaba.polardbx.optimizer.core.datatype.FloatType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ShortType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.Dummy;
import com.google.common.collect.Lists;
import com.sun.jdi.CharType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
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
      if (constructor == null) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "need constructor in your code");
      }
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
    } else if (type instanceof LongType) {
      return "long";
    } else if (type instanceof IntegerType) {
      return "int";
    } else if (type instanceof ShortType) {
      return "short";
    } else if (type instanceof ByteType) {
      return "byte";
    } else if (type instanceof FloatType) {
      return "float";
    } else if (type instanceof DoubleType) {
      return "double";
    } else if (type instanceof VarcharType || type instanceof CharType) {
      return "string";
    }
    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Compute java class error");
  }

}
