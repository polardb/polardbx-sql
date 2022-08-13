package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.UserDefinedJavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJavaFunction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateJavaFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.codehaus.commons.compiler.util.ResourceFinderClassLoader;
import org.codehaus.commons.compiler.util.resource.MapResourceCreator;
import org.codehaus.commons.compiler.util.resource.MapResourceFinder;
import org.codehaus.commons.compiler.util.resource.Resource;
import org.codehaus.commons.compiler.util.resource.StringResource;
import org.codehaus.commons.compiler.ICompiler;
import org.codehaus.janino.CompilerFactory;

import java.util.HashMap;
import java.util.Map;

public class LogicalCreateJavaFunctionHandler extends HandlerCommon {

  public LogicalCreateJavaFunctionHandler(IRepository repo) {
    super(repo);
  }

  @Override
  public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
    final LogicalCreateJavaFunction logicalCreateJavaFunction = (LogicalCreateJavaFunction) logicalPlan;
    final SqlCreateJavaFunction sqlCreateJavaFunction = (SqlCreateJavaFunction) logicalCreateJavaFunction.getNativeSqlNode();
    final String funcName = sqlCreateJavaFunction.getFuncName().toString();
    final String inputType = sqlCreateJavaFunction.getInputType();
    final String returnType = sqlCreateJavaFunction.getReturnType();
    final String packageName = sqlCreateJavaFunction.getPackageName();
    final String javaCode = sqlCreateJavaFunction.getJavaCode();

    if (funcName.equals("") ||
        inputType.equals("") ||
        returnType.equals("") ||
        packageName.equals("") ||
        javaCode.equals("")) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Create java_function syntax error");
    }

    if(ExtraFunctionManager.getFunctionCaches()
        .containsKey(
            FunctionSignature.
            getFunctionSignature(null, funcName))) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format("Function %s already exists", funcName));
    }

    //load javacode
    CompilerFactory compilerFactory = new CompilerFactory();
    ICompiler compiler = compilerFactory.newCompiler();
    Map<String, byte[]> classes = new HashMap<String, byte[]>();
    compiler.setClassFileCreator(new MapResourceCreator(classes));

    try {
      compiler.compile(new Resource[] {
          new StringResource(
              String.format("%s/%s.java", packageName, funcName),
              javaCode
          )
      });
    } catch (Exception e) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Compile code error");
    }
    ClassLoader cl = new ResourceFinderClassLoader(
        new MapResourceFinder(classes),    // resourceFinder
        ClassLoader.getSystemClassLoader() // parent
    );

    try{
      UserDefinedJavaFunctionManager.addFunction(cl.loadClass(String.format("%s.%s", packageName, funcName)));
    } catch (Exception e) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Add function error");
    }

    ReflectiveSqlOperatorTable.register(
        new SqlFunction(
            funcName.toUpperCase(),
            SqlKind.OTHER_FUNCTION,
            //待判断
            ReturnTypes.BIGINT,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY,
            //待判断
            SqlFunctionCategory.NUMERIC
        )
    );
    return new AffectRowCursor(new int[] {1});
  }
}
