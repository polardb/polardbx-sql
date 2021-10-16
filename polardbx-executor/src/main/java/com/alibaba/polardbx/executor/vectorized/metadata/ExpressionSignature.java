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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExpressionSignature {
    private final String name;
    private final ArgumentInfo[] args;
    private final ExpressionMode mode;

    public ExpressionSignature(String name,
                               ArgumentInfo[] args,
                               ExpressionMode mode) {
        this.name = name.toUpperCase();
        this.args = args;
        this.mode = mode;
    }

    public ExpressionSignature(String name,
                               ArgumentInfo[] args) {
        this(name, args, ExpressionMode.PROJECT);
    }

    public static Collection<ExpressionSignature> from(Class<?> klass)
        throws NoSuchFieldException, IllegalAccessException {
        Preconditions.checkNotNull(klass);
        ExpressionSignatures annotation = klass.getAnnotation(ExpressionSignatures.class);
        Preconditions.checkNotNull(annotation, "ExpressionSignatures annotation of class " + klass + " is null!");

        String[] names = annotation.names();
        String[] typeNames = annotation.argumentTypes();
        ArgumentKind[] argumentKinds = annotation.argumentKinds();
        ExpressionMode mode = annotation.mode();

        if (typeNames.length != argumentKinds.length) {
            throw new IllegalArgumentException(klass + " argument type and kind length doesn't match!");
        }

        ArgumentInfo[] args = new ArgumentInfo[typeNames.length];
        for (int i = 0; i < typeNames.length; i++) {
            DataType<?> type = (DataType<?>) DataTypes.class.getDeclaredField(typeNames[i] + "Type").get(null);
            args[i] = new ArgumentInfo(type, argumentKinds[i]);
        }

        return Arrays.stream(names)
            .map(name ->
                new ExpressionSignature(name.toUpperCase(), args, mode))
            .collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public ArgumentInfo[] getArgs() {
        return args;
    }

    public ExpressionMode getMode() {
        return mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExpressionSignature that = (ExpressionSignature) o;
        return getName().equals(that.getName()) &&
            Arrays.equals(getArgs(), that.getArgs()) &&
            mode == that.mode;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getName());
        result = 31 * result + Arrays.hashCode(getArgs());
        result = 31 * result + Objects.hash(mode);
        return result;
    }

    @Override
    public String toString() {
        return "ExpressionSignature{" +
            "name='" + name + '\'' +
            ", args=" + Arrays.toString(args) +
            ", mode=" + mode +
            '}';
    }
}