package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.UserDefinedJavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJavaFunction;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateJavaFunction;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalCreateJavaFunctionHandler extends HandlerCommon {

  public static TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

  public LogicalCreateJavaFunctionHandler(IRepository repo) {
    super(repo);
  }

  @Override
  public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
    final String PACKAGE_NAME = "com.alibaba.polardbx.optimizer.core.function.calc.scalar";

    final LogicalCreateJavaFunction logicalCreateJavaFunction = (LogicalCreateJavaFunction) logicalPlan;
    final SqlCreateJavaFunction sqlCreateJavaFunction = (SqlCreateJavaFunction) logicalCreateJavaFunction.getNativeSqlNode();
    final String funcName = sqlCreateJavaFunction.getFuncName().toString();
    final List<String> inputTypes = sqlCreateJavaFunction.getInputTypes();
    final String returnType = sqlCreateJavaFunction.getReturnType();
    final String userImportString = sqlCreateJavaFunction.getImportString() == null ? "" : sqlCreateJavaFunction.getImportString();
    final String userJavaCode = sqlCreateJavaFunction.getJavaCode();

    if (funcName.equals("") ||
        inputTypes.isEmpty() ||
        returnType.equals("") ||
        userJavaCode.equals("")) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Create java_function syntax error");
    }

    if(ExtraFunctionManager.getFunctionCaches()
        .containsKey(
            FunctionSignature.
            getFunctionSignature(null, funcName))) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format("Function %s already exists", funcName));
    }

    String className = funcName.substring(0, 1).toUpperCase() + funcName.substring(1).toLowerCase();
    String CODE = String.format(
            "package %s;\n" +
            "import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;\n" +
            "import com.alibaba.polardbx.optimizer.core.datatype.DataType;\n" +
            "import java.util.List;\n"+
            "%s\n"+
            "public class %s extends UserDefinedJavaFunction {\n" +
            "        public %s(List<DataType> operandTypes, DataType resultType) {\n" +
            "        super(operandTypes, resultType);\n" +
            "    }\n" +
            "@Override\n" +
            "public String[] getFunctionNames() {\n" +
            "    return new String[] {\"%s\"};\n" +
            "}\n" +
            "@Override\n" +
            "%s" +
            "}",
        PACKAGE_NAME, userImportString, className, className, funcName.toUpperCase(), userJavaCode);

    CompilerFactory compilerFactory = new CompilerFactory();
    ICompiler compiler = compilerFactory.newCompiler();
    Map<String, byte[]> classes = new HashMap<>();
    compiler.setClassFileCreator(new MapResourceCreator(classes));

    try {
      compiler.compile(new Resource[] {
          new StringResource(
              String.format("%s/%s.java", PACKAGE_NAME, className),
              CODE
          )
      });
    } catch (Exception e) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e.toString());
    }
    ClassLoader cl = new ResourceFinderClassLoader(
        new MapResourceFinder(classes),    // resourceFinder
        ClassLoader.getSystemClassLoader() // parent
    );

    List<DataType> inputDataTypes = new ArrayList<>(inputTypes.size());
    for (String inputType : inputTypes) {
      inputDataTypes.add(computeDataType(inputType));
    }
    DataType resultDataType = computeDataType(returnType);

    try{
      UserDefinedJavaFunctionManager.addFunction(cl.loadClass(String.format("%s.%s", PACKAGE_NAME, className)), inputDataTypes, resultDataType);
    } catch (Exception e) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Add function error");
    }

    //new SqlUserDefinedFunction
    final SqlFunction UserDefinedJavaFunction= new SqlFunction(
        funcName.toUpperCase(),
        SqlKind.OTHER_FUNCTION,
        //需要使用其他计算方法
        computeReturnType(returnType),
        InferTypes.FIRST_KNOWN,
        OperandTypes.ONE_OR_MORE,
        SqlFunctionCategory.SYSTEM
    );
    ReflectiveSqlOperatorTable.register(UserDefinedJavaFunction);
    RexUtils.addUnpushableFunction(UserDefinedJavaFunction);
    return new AffectRowCursor(0);
  }

  private DataType computeDataType(String type) {
    SqlTypeName name = SqlTypeName.get(type.toUpperCase());
    if (name == null) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Unsupport type "+type);
    }
    return DataTypeUtil.calciteToDrdsType(factory.createSqlType(name));
  }



  private SqlReturnTypeInference computeReturnType(String returnType) {
    SqlTypeName name = SqlTypeName.get(returnType.toUpperCase());
    if (name == null) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Unsupport return type " + returnType);
    }
    return ReturnTypes.explicit(name);
  }

  private boolean isNumericType(String type) {
    return type.equals("BIGINT") || type.equals("INTEGER")
        || type.equals("DOUBLE") || type.equals("FLOAT");
  }
}
