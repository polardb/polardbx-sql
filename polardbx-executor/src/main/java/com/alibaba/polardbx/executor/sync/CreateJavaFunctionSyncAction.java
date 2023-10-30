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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionAccessor;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;

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
        try (Connection conn = MetaDbUtil.getConnection()) {
            JavaFunctionAccessor functionAccessor = new JavaFunctionAccessor();
            functionAccessor.setConnection(conn);
            JavaFunctionRecord record = functionAccessor.queryFunctionByName(funcName).get(0);
            JavaFunctionManager.getInstance().registerFunction(record);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "create java function sync failed, caused by : " + e.getMessage());
        }
        return null;
    }
}
