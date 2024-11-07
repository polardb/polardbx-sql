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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionAccessor;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionMetaRecord;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.utils.CompileUtils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.TypeKnownScalarFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AssignableOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.codehaus.commons.compiler.ICompiler;
import org.codehaus.commons.compiler.ICompiler;
import org.codehaus.commons.compiler.util.ResourceFinderClassLoader;
import org.codehaus.commons.compiler.util.resource.MapResourceCreator;
import org.codehaus.commons.compiler.util.resource.MapResourceFinder;
import org.codehaus.commons.compiler.util.resource.Resource;
import org.codehaus.commons.compiler.util.resource.StringResource;
import org.codehaus.janino.CompilerFactory;
import org.eclipse.jdt.core.compiler.IProblem;
import org.eclipse.jdt.internal.compiler.CompilationResult;
import org.eclipse.jdt.internal.compiler.ICompilerRequestor;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFileReader;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFormatException;
import org.eclipse.jdt.internal.compiler.env.ICompilationUnit;
import org.eclipse.jdt.internal.compiler.env.INameEnvironment;
import org.eclipse.jdt.internal.compiler.env.NameEnvironmentAnswer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JavaFunctionManager {

    static class ConstructItem {
        public Constructor constructor;
        public List<DataType> inputTypes;
        public DataType returnType;
        public String funcName;

        ConstructItem(Constructor constructor, List<DataType> inputTypes, DataType returnType, String funcName) {
            this.constructor = constructor;
            this.inputTypes = inputTypes;
            this.returnType = returnType;
            this.funcName = funcName;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(JavaFunctionManager.class);

    private static JavaFunctionManager INSTANCE = new JavaFunctionManager();

    public static JavaFunctionManager getInstance() {
        return INSTANCE;
    }

    private Map<String, Boolean> functionAndState = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private Map<String, UserDefinedJavaFunction> functionCaches = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private Map<String, ConstructItem> constructorCaches = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    static final ClassLoader udfClassLoader = new UDFClassLoader();

    private JavaFunctionManager() {
        initFunctionMeta(false);
    }

    public synchronized void initFunctionMeta(boolean isReload) {
        try {
            List<JavaFunctionMetaRecord> records = getAllFunctionMetas();
            if (records.size() == 0) {
                return;
            }

            for (JavaFunctionMetaRecord record : records) {
                if (containsFunction(record.funcName)) {
                    logger.error("found duplicate java function, wired, function name is " + record.funcName);
                    continue;
                }
                try {
                    if (isReload) {
                        dropFunction(record.funcName);
                    }
                    registerFunction(record);
                } catch (Exception ex) {
                    logger.error(String.format("register java function failed, function name is %s, exception is %s",
                        record.funcName, ex.getMessage()));
                }
            }

        } catch (Throwable e) {
            logger.error("init java function manager failed, caused by " + e.getMessage());
        }
    }

    public synchronized void registerFunction(JavaFunctionMetaRecord record) {
        try {
            functionAndState.put(record.funcName, record.noState);
            RelDataType returnType = stringToRelDataType(record.returnType).get(0);
            List<RelDataType> inputTypes = stringToRelDataType(record.inputTypes);
            List<String> inputNames = IntStream.range(1, inputTypes.size() + 1).boxed().map(t -> "arg" + t).collect(
                Collectors.toList());
            Function function = new TypeKnownScalarFunction(returnType, inputTypes, inputNames);
            final SqlFunction javaFunction =
                new SqlUserDefinedFunction(new SqlIdentifier(record.funcName, SqlParserPos.ZERO),
                    ReturnTypes.explicit(returnType), InferTypes.explicit(inputTypes),
                    new AssignableOperandTypeChecker(inputTypes, inputNames), inputTypes, function, false);

            synchronized (TddlOperatorTable.instance()) {
                Multimap<ReflectiveSqlOperatorTable.Key, SqlOperator> operators =
                    HashMultimap.create(TddlOperatorTable.instance().getOperators());
                operators.put(new ReflectiveSqlOperatorTable.Key(record.funcName, javaFunction.getSyntax()),
                    javaFunction);
                TddlOperatorTable.instance().setOperators(operators);
            }
            SqlStdOperatorTable.instance().enableTypeCoercion(javaFunction);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "register java udf failed, caused by " + e.getMessage());
        }
    }

    public synchronized IScalarFunction getJavaFunction(String funcName) {
        if (!functionAndState.containsKey(funcName)) {
            return null;
        }
        Boolean noState = functionAndState.get(funcName);

        if (noState) {
            return getNoStateFunction(funcName);
        } else {
            return getNormalFunction(funcName);
        }
    }

    public synchronized UserDefinedJavaFunction getNoStateFunction(String funcName) {
        UserDefinedJavaFunction function = functionCaches.get(funcName);

        if (function != null) {
            return function;
        }

        try {
            compileJavaFunction(funcName, true);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        return functionCaches.get(funcName);
    }

    public synchronized UserDefinedJavaFunction getNormalFunction(String funcName) {
        ConstructItem constructItem = constructorCaches.get(funcName);

        if (constructItem == null) {
            try {
                compileJavaFunction(funcName, false);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        try {
            return newFunctionFromConstructor(constructorCaches.get(funcName));
        } catch (Exception ex) {
            logger.error("compile java function from constructor failed, function name is " + funcName);
            throw GeneralUtil.nestedException(ex);
        }
    }

    private synchronized UserDefinedJavaFunction newFunctionFromConstructor(ConstructItem item)
        throws InvocationTargetException, InstantiationException, IllegalAccessException {
        if (item == null) {
            return null;
        }
        UserDefinedJavaFunction function =
            (UserDefinedJavaFunction) item.constructor.newInstance();
        function.setInputTypes(item.inputTypes);
        function.setReturnType(item.returnType);
        function.setFuncName(item.funcName);
        function.setNoState(false);
        return function;
    }

    public synchronized void dropFunction(String funcName) {
        Boolean noState = functionAndState.remove(funcName);
        if (noState != null) {
            if (noState.equals(Boolean.TRUE)) {
                functionCaches.remove(funcName);
            } else if (noState.equals(Boolean.FALSE)) {
                constructorCaches.remove(funcName);
            }
        }
        synchronized (TddlOperatorTable.instance()) {
            Multimap<ReflectiveSqlOperatorTable.Key, SqlOperator> operators =
                HashMultimap.create(TddlOperatorTable.instance().getOperators());
            operators.removeAll(new ReflectiveSqlOperatorTable.Key(funcName.toLowerCase(), SqlSyntax.FUNCTION));
            TddlOperatorTable.instance().setOperators(operators);
        }
        // disable type coercion
        SqlStdOperatorTable.instance().disableTypeCoercion(funcName, SqlSyntax.FUNCTION);
    }

    public synchronized void reload() {
        functionAndState.clear();
        functionCaches.clear();
        constructorCaches.clear();
        initFunctionMeta(true);
    }

    public synchronized void compileJavaFunction(String funcName, boolean noState) {
        JavaFunctionRecord record = getFunctionRecord(funcName);
        if (record == null) {
            logger.warn("get empty java function definition from meta db, function name is " + funcName);
            return;
        }
        compileJavaFunction(funcName, noState, record);
    }

    private static class UDFClassLoader extends ClassLoader {
        static final ClassLoader outerClassLoader = UDFClassLoader.class.getClassLoader();

        private UDFClassLoader() {
            super(outerClassLoader);
        }

        @Override
        public URL getResource(String name) {
            if (!isValidResource(name)) {
                return null;
            }
            return outerClassLoader.getResource(name);
        }

        @Override
        public URL findResource(String name) {
            return getResource(name);
        }

        protected Class<?> findClass(String name) throws ClassNotFoundException {
            if (!isValidResource(name.replace('.', '/') + ".class")) {
                throw new ClassNotFoundException(name);
            }
            return outerClassLoader.loadClass(name);
        }

        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (!isValidResource(name.replace('.', '/') + ".class")) {
                throw new ClassNotFoundException(name);
            }
            return outerClassLoader.loadClass(name);
        }
    }

    private static boolean isValidResource(String resource) {
        if (resource.startsWith("/")) {
            resource = resource.substring(1);
        }
        final String name = resource;
        if (CompileUtils.allowedPackage.stream().anyMatch(name::startsWith)) {
            return true;
        }
        return false;
    }

    public static final class UdfCompilationUnit implements ICompilationUnit, ICompilerRequestor, INameEnvironment {
        List<IProblem> problemList = new ArrayList<>();
        private final String className;

        private final String packageName;
        private final char[] sourceCode;

        public UdfCompilationUnit(String sourceCode, String packageName, String className) {
            this.className = className;
            this.packageName = packageName;
            this.sourceCode = sourceCode.toCharArray();
        }

        @Override
        public char[] getFileName() {
            return className.toCharArray();
        }

        @Override
        public char[] getContents() {
            return sourceCode;
        }

        @Override
        public char[] getMainTypeName() {
            return className.toCharArray();
        }

        @Override
        public char[][] getPackageName() {
            List<char[]> result =
                Arrays.stream(org.apache.commons.lang3.StringUtils.split(packageName, ".")).map(String::toCharArray)
                    .collect(Collectors.toList());
            return result.stream().toArray(char[][]::new);
        }

        @Override
        public boolean ignoreOptionalProblems() {
            return false;
        }

        @Override
        public void cleanup() {
        }

        @Override
        public void acceptResult(CompilationResult result) {
            if (result.hasErrors()) {
                Collections.addAll(problemList, result.getProblems());
            }
        }

        @Override
        public NameEnvironmentAnswer findType(char[][] compoundTypeName) {
            List<String> elements = Arrays.stream(compoundTypeName).map(String::new).collect(Collectors.toList());
            return findType(String.join(".", elements));
        }

        @Override
        public NameEnvironmentAnswer findType(char[] typeName, char[][] packageName) {
            if (packageName == null) {
                return typeName == null ? null : findType(new String(typeName));
            }
            List<String> elements = Arrays.stream(packageName).map(String::new).collect(Collectors.toList());
            elements.add(new String(typeName));
            return findType(String.join(".", elements));
        }

        @SuppressWarnings("resource")
        private NameEnvironmentAnswer findType(String className) {
            if (className.equals(this.className)) {
                return new NameEnvironmentAnswer(this, null);
            }

            String resourceName = className.replace('.', '/') + ".class";

            if (CompileUtils.allowedPackage.stream().noneMatch(resourceName::startsWith)) {
                return null;
            }

            try (InputStream is = JavaFunctionManager.udfClassLoader.getResourceAsStream(resourceName)) {
                if (is != null) {
                    byte[] classBytes = ByteStreams.toByteArray(is);
                    char[] fileName = className.toCharArray();
                    ClassFileReader classFileReader = new ClassFileReader(classBytes, fileName, true);
                    return new NameEnvironmentAnswer(classFileReader, null);
                }
            } catch (IOException | ClassFormatException exc) {
                throw new RuntimeException(exc);
            }
            return null;
        }

        private boolean isPackage(String result) {
            if (result.equals(this.className)) {
                return false;
            }
            String resourceName = result.replace('.', '/') + ".class";
            try (InputStream is = udfClassLoader.getResourceAsStream(resourceName)) {
                return is == null;
            } catch (IOException e) {
                return false;
            }
        }

        @Override
        public boolean isPackage(char[][] parentPackageName, char[] packageName) {
            if (Character.isUpperCase(packageName[0])) {
                return false;
            }
            StringBuilder result = new StringBuilder();
            int i = 0;
            if (parentPackageName != null) {
                for (; i < parentPackageName.length; i++) {
                    if (i > 0) {
                        result.append('.');
                    }
                    result.append(parentPackageName[i]);
                }
            }

            if (i > 0) {
                result.append('.');
            }
            result.append(packageName);

            boolean isPackage = isPackage(result.toString());
            return isPackage;
        }

        public List<IProblem> getProblemList() {
            return problemList;
        }
    }

    public synchronized void compileJavaFunction(String funcName, boolean noState, JavaFunctionRecord record) {
        try {
            DataType returnType = stringToDataType(record.returnType).get(0);
            List<DataType> inputTypes = stringToDataType(record.inputTypes);
            ResourceFinderClassLoader classLoader =
                compileAndLoadClass(CompileUtils.fullJavaCode(record.code), record.className);
            Class functionClass =
                classLoader.loadClass(String.format("%s.%s", CompileUtils.PACKAGE_NAME, record.className));
            Constructor constructor = functionClass.getConstructor();
            if (noState) {
                UserDefinedJavaFunction function =
                    (UserDefinedJavaFunction) constructor.newInstance();
                function.setInputTypes(inputTypes);
                function.setReturnType(returnType);
                function.setFuncName(funcName);
                function.setNoState(true);
                functionCaches.put(funcName, function);
            } else {
                constructorCaches.put(funcName, new ConstructItem(constructor, inputTypes, returnType, funcName));
            }
        } catch (Exception ex) {
            logger.error("compile java function failed, function name is " + funcName);
            throw GeneralUtil.nestedException(ex);
        }
    }

    private JavaFunctionRecord getFunctionRecord(String funcName) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            JavaFunctionAccessor functionAccessor = new JavaFunctionAccessor();
            functionAccessor.setConnection(connection);
            List<JavaFunctionRecord> records = functionAccessor.queryFunctionByName(funcName);
            return records.size() == 0 ? null : records.get(0);
        } catch (Exception ex) {
            logger.error("get java function definition from meta db failed, function name is " + funcName);
            throw GeneralUtil.nestedException(ex);
        }
    }

    private List<JavaFunctionMetaRecord> getAllFunctionMetas() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            JavaFunctionAccessor functionAccessor = new JavaFunctionAccessor();
            functionAccessor.setConnection(connection);
            return functionAccessor.queryAllFunctionMetas();
        } catch (Exception ex) {
            logger.error("get all java function meta from meta db failed");
            throw GeneralUtil.nestedException(ex);
        }
    }

    public synchronized ResourceFinderClassLoader compileAndLoadClass(String code, String className) {
        CompilerFactory compilerFactory = new CompilerFactory();
        ICompiler compiler = compilerFactory.newCompiler();
        Map<String, byte[]> classes = new HashMap<>();
        compiler.setClassFileCreator(new MapResourceCreator(classes));

        try {
            compiler.compile(new Resource[] {
                new StringResource(
                    String.format("%s/%s.java", CompileUtils.PACKAGE_NAME, className),
                    code
                )
            });
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e.toString());
        }
        return new ResourceFinderClassLoader(
            new MapResourceFinder(classes),    // resourceFinder
            ClassLoader.getSystemClassLoader() // parent
        );
    }

    private List<RelDataType> stringToRelDataType(String str) {
        try {
            if (StringUtils.isEmpty(str)) {
                return new ArrayList<>();
            }
            return FastsqlUtils.parseDataType(str).stream()
                .map(type -> DataTypeUtil.createBasicSqlType(TddlRelDataTypeSystemImpl.getInstance(), type)).collect(
                    Collectors.toList());
        } catch (Exception ex) {
            logger.error("parse datatype list failed, str is " + str);
            throw ex;
        }
    }

    private List<DataType> stringToDataType(String str) {
        try {
            return stringToRelDataType(str).stream()
                .map(type -> type.isStruct() ? null : DataTypeUtil.calciteToDrdsType(type)).collect(
                    Collectors.toList());
        } catch (Exception ex) {
            logger.error("parse datatype list failed, str is " + str);
            throw ex;
        }
    }

    public synchronized boolean containsFunction(String name) {
        return functionAndState.containsKey(name);
    }

    public synchronized long getFuncNum() {
        return functionAndState.size();
    }
}
