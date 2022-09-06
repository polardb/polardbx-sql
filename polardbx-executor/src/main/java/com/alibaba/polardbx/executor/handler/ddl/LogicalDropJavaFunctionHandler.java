package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.DropJavaFunctionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.UserDefinedJavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropJavaFunction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropJavaFunction;

public class LogicalDropJavaFunctionHandler extends HandlerCommon {
    public LogicalDropJavaFunctionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDropJavaFunction logicalDropJavaFunction = (LogicalDropJavaFunction) logicalPlan;
        final SqlDropJavaFunction sqlDropJavaFunction =
            (SqlDropJavaFunction) logicalDropJavaFunction.getNativeSqlNode();
        final String funcNameUpper = sqlDropJavaFunction.getFuncName().toString().toUpperCase();
        final boolean ifExist = sqlDropJavaFunction.isIfExists();

        if (funcNameUpper.equals("")) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Drop java_function syntax error");
        }

        if (!UserDefinedJavaFunctionManager.containsFunction(funcNameUpper)) {
            if (ifExist) {
                return new AffectRowCursor(0);
            }
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Java function %s not found", funcNameUpper));
        }

        UserDefinedJavaFunctionManager.dropFunction(funcNameUpper);

        SyncManagerHelper.sync(new DropJavaFunctionSyncAction(funcNameUpper));

        return new AffectRowCursor(0);
    }
}
