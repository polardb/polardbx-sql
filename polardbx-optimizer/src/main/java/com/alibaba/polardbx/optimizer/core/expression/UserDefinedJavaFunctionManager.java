package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.table.UserDefinedJavaFunctionAccessor;
import com.alibaba.polardbx.gms.metadb.table.UserDefinedJavaFunctionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.codehaus.commons.compiler.util.ResourceFinderClassLoader;
import org.codehaus.commons.compiler.util.resource.MapResourceCreator;
import org.codehaus.commons.compiler.util.resource.MapResourceFinder;
import org.codehaus.commons.compiler.util.resource.Resource;
import org.codehaus.commons.compiler.util.resource.StringResource;
import org.codehaus.commons.compiler.ICompiler;
import org.codehaus.janino.CompilerFactory;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UserDefinedJavaFunctionManager {

    private static final Logger logger = LoggerFactory.getLogger(UserDefinedJavaFunctionManager.class);
    public static TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    public static Map<String, Constructor<?>> javaFunctionCaches = new HashMap<>();

    public static Map<String, List<DataType>> userInputTypesByFuncName = new HashMap<>();
    public static Map<String, DataType> userResultTypeByFuncName = new HashMap<>();

    public static boolean containsFunction(String name) {
        return javaFunctionCaches.containsKey(name);
    }

    public static void removeFunctionFromCache(String name) {
        javaFunctionCaches.remove(name);
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
                userInputTypesByFuncName.put(functionName, inputTypes);
                userResultTypeByFuncName.put(functionName, resultType);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static void dropFunction(String funcNameUpper) {
        UserDefinedJavaFunctionManager.removeFunctionFromCache(funcNameUpper);
        ReflectiveSqlOperatorTable.remove(funcNameUpper);
        System.gc();

        Connection connection = MetaDbUtil.getConnection();
        UserDefinedJavaFunctionAccessor.deleteFunctionByName(funcNameUpper.toLowerCase(), connection);
    }

    public static void addFunctionFromMeta(UserDefinedJavaFunctionRecord record) {
        String funcName = record.funcName;
        String className = record.className;
        String code = record.code;

        ClassLoader cl = compileAndLoadClass(code, className);

        //compute type
        List<DataType> inputDataTypes = new ArrayList<>();
        for (String type : record.inputTypes.split(",")) {
            inputDataTypes.add(computeDataType(type));
        }
        DataType resultDataType = computeDataType(record.resultType);

        //add function
        try {
            addFunction(
                cl.loadClass(String.format("com.alibaba.polardbx.optimizer.core.function.calc.scalar.%s", className)),
                inputDataTypes, resultDataType);
            final SqlFunction UserDefinedJavaFunction = new SqlFunction(
                record.funcName.toUpperCase(),
                SqlKind.OTHER_FUNCTION,
                computeReturnType(record.resultType),
                InferTypes.FIRST_KNOWN,
                OperandTypes.ONE_OR_MORE,
                SqlFunctionCategory.SYSTEM
            );
            RexUtils.addUnpushableFunction(UserDefinedJavaFunction);
            ReflectiveSqlOperatorTable.register(UserDefinedJavaFunction);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Initialize function error");
        }
    }

    public static void initFunctions() {
        Connection connection = MetaDbUtil.getConnection();
        List<UserDefinedJavaFunctionRecord> records = UserDefinedJavaFunctionAccessor.queryAllFunctions(connection);

        if (records.size() == 0) {
            return;
        }

        records.stream()
            .filter(record -> !containsFunction(record.funcName))
            .forEach(UserDefinedJavaFunctionManager::addFunctionFromMeta);
    }

    public static AbstractScalarFunction getUserDefinedJavaFunction(String functionName, List<DataType> operandTypes,
                                                                    DataType resultType) {
        Constructor constructor = javaFunctionCaches.get(functionName);

        if (constructor == null) {
            return null;
        }

        try {
            UserDefinedJavaFunction sample =
                (UserDefinedJavaFunction) constructor.newInstance(operandTypes, resultType);

            List<DataType> userInputTypes = userInputTypesByFuncName.get(functionName);
            DataType userResultType = userResultTypeByFuncName.get(functionName);
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

    public static ClassLoader compileAndLoadClass(String code, String className) {
        final String packageName = "com.alibaba.polardbx.optimizer.core.function.calc.scalar";

        CompilerFactory compilerFactory = new CompilerFactory();
        ICompiler compiler = compilerFactory.newCompiler();
        Map<String, byte[]> classes = new HashMap<>();
        compiler.setClassFileCreator(new MapResourceCreator(classes));

        try {
            compiler.compile(new Resource[] {
                new StringResource(
                    String.format("%s/%s.java", packageName, className),
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

    public static DataType computeDataType(String type) {
        SqlTypeName name = SqlTypeName.get(type.toUpperCase());
        if (name == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Unsupport type " + type);
        }
        return DataTypeUtil.calciteToDrdsType(factory.createSqlType(name));
    }

    public static SqlReturnTypeInference computeReturnType(String returnType) {
        SqlTypeName name = SqlTypeName.get(returnType.toUpperCase());
        if (name == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Unsupport return type " + returnType);
        }
        return ReturnTypes.explicit(name);
    }

}
