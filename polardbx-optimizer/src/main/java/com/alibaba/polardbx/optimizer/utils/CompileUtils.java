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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import org.eclipse.jdt.internal.compiler.DefaultErrorHandlingPolicies;
import org.eclipse.jdt.internal.compiler.IErrorHandlingPolicy;
import org.eclipse.jdt.internal.compiler.IProblemFactory;
import org.eclipse.jdt.internal.compiler.env.ICompilationUnit;
import org.eclipse.jdt.internal.compiler.impl.CompilerOptions;
import org.eclipse.jdt.internal.compiler.problem.DefaultProblemFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompileUtils {
    public static final String PACKAGE_NAME = "com.alibaba.polardbx.optimizer.core.function.calc.scalar";

    public static final String JAVA_UDF_PATH =
        "com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction";

    public static final List<String> allowedPackage = Arrays.asList(
        // basic
        "java/lang/Object.class",
        "java/lang/String.class",
        "java/lang/StringBuffer.class",
        "java/lang/StringBuilder.class",
        "java/lang/CharSequence.class",
        "java/lang/Integer.class",
        "java/lang/Long.class",
        "java/lang/Short.class",
        "java/lang/Byte.class",
        "java/lang/Boolean.class",
        "java/lang/Number.class",
        "java/lang/Math.class",
        "java/sql/Blob.class",
        "java/sql/Time.class",
        "java/sql/Timestamp.class",
        "java/sql/Date.class",
        "java/nio/Buffer.class",
        "java/nio/ByteBuffer.class",
        "java/nio/ByteOrder.class",
        "java/math/",
        "java/text/",
        "java/time/",
        "java/util/",

        // scalar function
        "com/alibaba/polardbx/optimizer/core/function/calc/",
        "com/alibaba/polardbx/optimizer/core/expression/IExtraFunction.class",
        "com/alibaba/polardbx/optimizer/core/expression/IFunction.class",
        "com/alibaba/polardbx/optimizer/core/expression/IFunction$FunctionType.class",
        "com/alibaba/polardbx/optimizer/core/expression/bean/CoronaFunctionSignature.class",
        "com/alibaba/polardbx/optimizer/core/datatype/DataType.class",
        "com/alibaba/polardbx/optimizer/context/ExecutionContext.class",
        "com/alibaba/polardbx/optimizer/config/table/Field.class",
        "com/alibaba/polardbx/common/charset/CollationName.class",

        // annotation
        "java/lang/annotation/",

        "java/lang/invoke/MethodHandles.class",
        "java/lang/invoke/MethodHandles$Lookup.class",
        "java/lang/invoke/LambdaMetafactory.class",

        // exception
        "java/lang/NullPointerException.class",
        "java/lang/IndexOutOfBoundsException.class",
        "java/lang/IllegalArgumentException.class",
        "java/lang/ArithmeticException.class",
        "java/lang/InterruptedException.class",
        "java/lang/ClassNotFoundException.class",
        "java/lang/RuntimeException.class",
        "java/lang/Exception.class",
        "java/lang/Throwable.class",
        "java/io/Serializable.class",
        "java/io/IOException.class",
        "java/lang/ArrayIndexOutOfBoundsException.class",
        "java/lang/ClassCastException.class",
        "java/lang/IllegalStateException.class",
        "java/lang/UnsupportedOperationException.class",
        "java/lang/NumberFormatException.class",
        "java/lang/NegativeArraySizeException.class",
        "java/lang/StringIndexOutOfBoundsException.class",
        "java/lang/CloneNotSupportedException.class",
        "java/lang/AssertionError.class",
        "java/lang/Error.class",

        // basic interface
        "java/lang/AutoCloseable.class",
        "java/lang/Override.class",
        "java/lang/Readable.class",
        "java/lang/Runnable.class",
        "java/lang/FunctionalInterface.class",
        "java/lang/Cloneable.class",
        "java/lang/Comparable.class",

        "java/io/ObjectOutputStream.class",
        "java/io/ObjectInputStream.class"
    );

    public static void checkInvalidJavaCode(String userJavaCode, String className) {
        JavaFunctionManager.UdfCompilationUnit compilationUnit =
            new JavaFunctionManager.UdfCompilationUnit(fullJavaCode(userJavaCode), CompileUtils.PACKAGE_NAME,
                className);
        IErrorHandlingPolicy errorHandlingPolicy = DefaultErrorHandlingPolicies.proceedWithAllProblems();

        Map<String, String> settings = new HashMap<>();
        settings.put(CompilerOptions.OPTION_Source,
            CompilerOptions.VERSION_1_8);
        settings.put(CompilerOptions.OPTION_TargetPlatform,
            CompilerOptions.VERSION_1_8);

        CompilerOptions compilerOptions = new CompilerOptions(settings);

        IProblemFactory problemFactory = new DefaultProblemFactory();

        org.eclipse.jdt.internal.compiler.Compiler compiler =
            new org.eclipse.jdt.internal.compiler.Compiler(compilationUnit,
                errorHandlingPolicy,
                compilerOptions,
                compilationUnit,
                problemFactory);
        compiler.compile(new ICompilationUnit[] {compilationUnit});

        if (compilationUnit.getProblemList().size() != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                compilationUnit.getProblemList().get(0).getMessage());
        }
    }

    public static String fullJavaCode(String userJavaCode) {
        StringBuilder sb = new StringBuilder();
        sb.append("package ").append(CompileUtils.PACKAGE_NAME).append(";\n");
        sb.append("import ").append(CompileUtils.JAVA_UDF_PATH).append(";\n");
        sb.append(userJavaCode);
        return sb.toString();
    }
}

