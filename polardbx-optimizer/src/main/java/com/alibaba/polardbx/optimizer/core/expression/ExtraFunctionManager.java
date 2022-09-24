/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.expression;

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

/**
 * {@linkplain AbstractScalarFunction}加载器，以类名做为Function Name，<strong>注意：忽略了大小写</stong>
 * <p>
 * <pre>
 * Function加载：
 * 1. 自动扫描IExtraFunction对应Package目录下的所有Function实现
 * 2. 自动扫描Extension扩展方式下的自定义实现，比如在META-INF/tddl 或 META-INF/services 添加扩展配置文件
 * </pre>
 *
 * @author jianghang 2013-11-8 下午5:30:35
 * @since 5.0.0
 */
public class ExtraFunctionManager {

    private static final Logger logger = LoggerFactory.getLogger(ExtraFunctionManager.class);
    private static Map<FunctionSignature, Constructor<?>> functionCaches = new HashMap<>();
    // 缓存一下dummy，避免每次都反射创建

    static {
        initFunctions();
    }

    /**
     * 查找对应签名的函数类，忽略大小写, 使用签名定位
     */
    public static AbstractScalarFunction getExtraFunction(String functionName, List<DataType> operandTypes,
                                                          DataType resultType) {
        String name = functionName;
        String a = "";
        Constructor constructor = functionCaches.get(FunctionSignature.getFunctionSignature(null, name));

        if (constructor == null) {
            return new Dummy(functionName, operandTypes, resultType);
        }

        try {
            return (AbstractScalarFunction) constructor.newInstance(operandTypes, resultType);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static void addFunction(Class clazz) {

        try {
            Constructor constructor = clazz.getConstructor(List.class, DataType.class);
            AbstractScalarFunction sample = (AbstractScalarFunction) constructor.newInstance(null, null);
            for (FunctionSignature signature : sample.getFunctionSignature()) {
                Constructor oldConstructor = functionCaches.put(signature, constructor);
                if (oldConstructor != null) {
                    logger.warn(" dup function :" + signature.getName() + ", old class : " + oldConstructor.getClass()
                        .getName());
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

    }

    private static void initFunctions() {
        List<Class> classes = Lists.newArrayList();
        // 查找默认build-in的函数

        ClassFinder.ClassFilter filter = new ClassFinder.ClassFilter() {

            @Override
            public boolean filter(Class clazz) {
                int mod = clazz.getModifiers();
                return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod)
                    && AbstractScalarFunction.class.isAssignableFrom(clazz);
            }

            @Override
            public boolean preFilter(String classFulName) {
                return StringUtils.contains(classFulName, "function");
                // 包含function名字的类
            }

        };
        classes.addAll(ClassFinder.findClassesInPackage("com.alibaba.polardbx", filter));
        // 查找用户自定义的扩展函数
        classes.addAll(ExtensionLoader.getAllExtendsionClass(AbstractScalarFunction.class));

        for (Class clazz : classes) {
            if (clazz == Dummy.class) {
                continue;
            }
            addFunction(clazz);
        }
    }
}
