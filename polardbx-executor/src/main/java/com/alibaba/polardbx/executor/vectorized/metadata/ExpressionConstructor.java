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

package com.alibaba.polardbx.executor.vectorized.metadata;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.lang.reflect.Constructor;

public class ExpressionConstructor<T extends VectorizedExpression> {
    private final Constructor<T> constructor;

    private ExpressionConstructor(Constructor<T> constructor) {
        this.constructor = constructor;
    }

    public T build(int outputIndex, VectorizedExpression[] args) {
        try {
            return constructor.newInstance(outputIndex, args);
        } catch (Exception e) {
            throw GeneralUtil.nestedException("Failed to create instance: ", e);
        }
    }

    public T build(DataType<?> dataType, int outputIndex, VectorizedExpression[] args) {
        try {
            if (constructor.getParameterCount() == 2) {
                return constructor.newInstance(outputIndex, args);
            }
            return constructor.newInstance(dataType, outputIndex, args);
        } catch (Exception e) {
            throw GeneralUtil.nestedException("Failed to create instance: ", e);
        }
    }

    public Class<T> getDeclaringClass() {
        return constructor.getDeclaringClass();
    }

    public static <T extends VectorizedExpression> ExpressionConstructor<T> of(Class<T> klass) {
        try {
            Class<?>[] parameterTypes = null;
            // prefer to 3 parameters constructor.
            for (Constructor<?> constructor : klass.getConstructors()) {
                if (constructor.getParameterCount() == 3) {
                    parameterTypes = new Class<?>[] {DataType.class, int.class, VectorizedExpression[].class};
                    break;
                }
            }
            if (parameterTypes == null) {
                parameterTypes = new Class<?>[] {int.class, VectorizedExpression[].class};
            }
            return new ExpressionConstructor<>(klass.getDeclaredConstructor(parameterTypes));
        } catch (NoSuchMethodException e) {
            throw GeneralUtil.nestedException("Failed to get builder constructor from class " + klass, e);
        }
    }
}