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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.ClassFinder;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionConstructor;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignature;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.google.common.collect.Lists;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Search all of the code generated vectorized expression implementation.
 * And then, load class and make class constructor of them.
 */
public class VectorizedExpressionRegistry {
    private static final Map<ExpressionSignature, ExpressionConstructor<?>> EXPRESSIONS = new HashMap<>(1024);

    static {
        try {
            initExpressions();
        } catch (Exception e) {
            GeneralUtil.nestedException("Failed to initialize vectorized expression registry: ", e);
        }
    }

    private static void initExpressions() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException {
        List<Class> classes = Lists.newArrayList();

        ClassFinder.ClassFilter filter = new ClassFinder.ClassFilter() {
            @Override
            public boolean filter(Class klass) {
                int mod = klass.getModifiers();
                return !Modifier.isAbstract(mod)
                    && !Modifier.isInterface(mod)
                    && VectorizedExpression.class.isAssignableFrom(klass)
                    && klass.getAnnotation(ExpressionSignatures.class) != null;
            }

            @Override
            public boolean preFilter(String classFulName) {
                return classFulName.endsWith("VectorizedExpression");
            }
        };

        classes.addAll(ClassFinder.findClassesInPackage("com.alibaba.polardbx", filter));

        for (@SuppressWarnings("unchecked") Class<? extends VectorizedExpression> klass : classes) {
            for (ExpressionSignature expressionSignature : ExpressionSignature.from(klass)) {
                if (EXPRESSIONS.containsKey(expressionSignature)) {
                    Class<?> existingClass = EXPRESSIONS.get(expressionSignature).getDeclaringClass();
                    throw new IllegalStateException(
                        "Expression signature " + expressionSignature + " has been defined by class: " + existingClass
                            + " , but redefined in class: " + klass);
                }

                EXPRESSIONS.put(expressionSignature, ExpressionConstructor.of(klass));
            }
        }
    }

    public static Optional<ExpressionConstructor<?>> builderConstructorOf(ExpressionSignature signature) {
        return Optional.ofNullable(EXPRESSIONS.get(signature));
    }
}
