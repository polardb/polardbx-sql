package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserDefinedJavaFunctionManager extends ExtraFunctionManager{
  private static final Logger logger = LoggerFactory.getLogger(UserDefinedJavaFunctionManager.class);
  public static Map<String, Constructor<?>> javaFunctionCaches = new HashMap<>();

  public static List<DataType> userInputTypes;
  public static DataType userResultType;

  static {
    initFunctions();
  }

  public static boolean containsFunction(String name) {
    return javaFunctionCaches.containsKey(name);
  }

  public static boolean removeFunction(String name) {
    javaFunctionCaches.remove(name);
    return true;
  }

  public static void addFunction(Class type, List<DataType> inputTypes, DataType resultType) {

    try {
      Constructor constructor = type.getConstructor(List.class, DataType.class);
      AbstractScalarFunction sample = (AbstractScalarFunction) constructor.newInstance(null, null);

      for (String functionName : sample.getFunctionNames()) {
        Constructor oldConstructor = javaFunctionCaches.put(functionName, constructor);
        if (oldConstructor != null) {
          logger.warn(" dup function :" + functionName + ", old class : " + oldConstructor.getClass()
              .getName());
        }
      }
      userInputTypes = inputTypes;
      userResultType = resultType;
    } catch (Exception e) {
      throw GeneralUtil.nestedException(e);
    }

  }

  public static Map<String, Constructor<?>> getJavaFunctionCaches() {
    return javaFunctionCaches;
  }

  private static void initFunctions() {
    //do nothing
  }

  public static AbstractScalarFunction getUserDefinedJavaFunction(String functionName, List<DataType> operandTypes,
                                                                  DataType resultType) {
    Constructor constructor = javaFunctionCaches.get(functionName);

    if (constructor == null) return null;

    try {
      UserDefinedJavaFunction sample = (UserDefinedJavaFunction) constructor.newInstance(operandTypes, resultType);

      if (userResultType == null || userInputTypes.isEmpty()) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Need input type and result type");
      }
      sample.setUserInputType(userInputTypes);
      sample.setUserResultType(userResultType);

      return (AbstractScalarFunction) sample;
    } catch (Exception e) {
      throw GeneralUtil.nestedException(e);
    }
  }
}
