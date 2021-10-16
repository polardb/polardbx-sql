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

package com.alibaba.polardbx.executor.utils;

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.annotation.Parameter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class TestParameterUtil {
    public static List<Object[]> parameters(Class<?> klass) {
        return Arrays.stream(klass.getDeclaredMethods())
            .filter(TestParameterUtil::checkParameterMethod)
            .map(TestParameterUtil::invokeParameterMethod)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    public static boolean checkParameterMethod(Method method) {
        if (!method.isAnnotationPresent(Parameter.class)) {
            return false;
        }
        Preconditions.checkArgument(Modifier.isStatic(method.getModifiers()), "Parameter method %s must be static!",
            method.getName());
        Preconditions.checkArgument(Modifier.isPublic(method.getModifiers()), "Parameter method %s must be public!",
            method.getName());
        Preconditions
            .checkArgument(method.getParameterCount() == 0, "Parameter method %s should not require any argument!",
                method.getName());

        return true;
    }

    public static Collection<Object[]> invokeParameterMethod(Method method) {
        try {
            return (Collection<Object[]>) method.invoke(null);
        } catch (Exception e) {
            throw GeneralUtil
                .nestedException(String.format("Failed to generate parameter using method %s", method.getName()), e);
        }
    }
}
