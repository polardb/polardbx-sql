package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.metadb.table.UserDefinedJavaFunctionAccessor;
import com.alibaba.polardbx.gms.metadb.table.UserDefinedJavaFunctionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.expression.UserDefinedJavaFunctionManager;

import java.sql.Connection;

public class CreateJavaFunctionSyncAction implements ISyncAction {
    private String funcName;

    public String getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }

    public CreateJavaFunctionSyncAction() {

    }

    public CreateJavaFunctionSyncAction(String funcName) {
        this.funcName = funcName;
    }

    @Override
    public ResultCursor sync() {
        Connection conn = MetaDbUtil.getConnection();
        UserDefinedJavaFunctionRecord record = UserDefinedJavaFunctionAccessor.queryFunctionByName(funcName, conn).get(0);
        UserDefinedJavaFunctionManager.addFunctionFromMeta(record);
        return null;
    }
}
