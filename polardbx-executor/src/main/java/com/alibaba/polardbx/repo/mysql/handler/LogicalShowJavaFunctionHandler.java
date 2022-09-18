package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.UserDefinedJavaFunctionManager;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public class LogicalShowJavaFunctionHandler extends HandlerCommon {

    public LogicalShowJavaFunctionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("java_function");
        result.addColumn("FunctionName", DataTypes.StringType);
        List<String> functions = UserDefinedJavaFunctionManager.getFunctionList();

        functions.stream().forEach(funcName -> {
            result.addRow(new Object[] {funcName});
        });
        return result;
    }
}
