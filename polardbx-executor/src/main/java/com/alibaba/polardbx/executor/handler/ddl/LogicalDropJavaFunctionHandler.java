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

        if (!UserDefinedJavaFunctionManager.containsFunction(funcNameUpper) && !ifExist) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Java function %s not found", funcNameUpper));
        }

        UserDefinedJavaFunctionManager.dropFunction(funcNameUpper);

        SyncManagerHelper.sync(new DropJavaFunctionSyncAction(funcNameUpper));

        return new AffectRowCursor(0);
    }
}
