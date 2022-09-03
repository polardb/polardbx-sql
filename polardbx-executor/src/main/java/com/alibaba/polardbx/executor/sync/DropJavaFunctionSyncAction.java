package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.core.expression.UserDefinedJavaFunctionManager;

public class DropJavaFunctionSyncAction implements ISyncAction{
    private String funcName;

    public String getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }

    public DropJavaFunctionSyncAction() {

    }

    public DropJavaFunctionSyncAction(String funcName) {
        this.funcName = funcName;
    }

    @Override
    public ResultCursor sync() {
        UserDefinedJavaFunctionManager.dropFunction(funcName);
        return null;
    }
}
