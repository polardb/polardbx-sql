package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.ClassFinder;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.Dummy;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

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

  public static void addFunction(Class clazz) {

    try {
      Constructor constructor = clazz.getConstructor(List.class, DataType.class);
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
//    List<Class> classes = Lists.newArrayList();
//    // 查找默认build-in的函数
//
//    ClassFinder.ClassFilter filter = new ClassFinder.ClassFilter() {
//
//      @Override
//      public boolean filter(Class clazz) {
//        int mod = clazz.getModifiers();
//        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod)
//            && AbstractScalarFunction.class.isAssignableFrom(clazz);
//      }
//
//      @Override
//      public boolean preFilter(String classFulName) {
//        return StringUtils.contains(classFulName, "function");
//        // 包含function名字的类
//      }
//
//    };
//    classes.addAll(ClassFinder.findClassesInPackage("com.alibaba.polardbx", filter));
//    // 查找用户自定义的扩展函数
//    classes.addAll(ExtensionLoader.getAllExtendsionClass(AbstractScalarFunction.class));
//
//    for (Class clazz : classes) {
//      if (clazz == Dummy.class) {
//        continue;
//      }
//      addFunction(clazz);
//    }
  }
}
