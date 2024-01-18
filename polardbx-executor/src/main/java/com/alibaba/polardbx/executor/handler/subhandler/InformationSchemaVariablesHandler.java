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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.view.InformationSchemaGlobalVariables;
import com.alibaba.polardbx.optimizer.view.InformationSchemaSessionVariables;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * handle global and session variables
 *
 * @author shengyu
 */
public class InformationSchemaVariablesHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaVariablesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    private static final Class fetchTimerTaskInfoSyncActionClass;

    static {
        // Since executor-module can not 'see' transaction-module,
        // we have to use this way to get transaction information.
        try {
            fetchTimerTaskInfoSyncActionClass =
                Class.forName("com.alibaba.polardbx.transaction.sync.FetchTimerTaskInfoSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaGlobalVariables
            || virtualView instanceof InformationSchemaSessionVariables;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        String sql;
        final boolean isGlobal = virtualView instanceof InformationSchemaGlobalVariables;
        if (isGlobal) {
            sql = "show global variables";
        } else {
            sql = "show session variables";
        }

        ExecutionContext newExecutionContext = executionContext.copy();
        newExecutionContext.newStatement();
        ExecutionPlan executionPlan = Planner.getInstance().plan(sql, newExecutionContext);
        Cursor resultCursor = ExecutorHelper.execute(executionPlan.getPlan(), newExecutionContext);
        boolean showAllParams = executionContext.getParamManager().getBoolean(
            ConnectionParams.SHOW_ALL_PARAMS);
        if (showAllParams && resultCursor instanceof ArrayResultCursor) {
            // Add timer task parameters into result.
            // 1. Get all task variables from leader.
            final ISyncAction fetchTimerTaskInfoSyncAction;
            try {
                fetchTimerTaskInfoSyncAction =
                    (ISyncAction) fetchTimerTaskInfoSyncActionClass.getConstructor(String.class)
                        .newInstance(executionContext.getSchemaName());
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
            }
            final List<List<Map<String, Object>>> allTaskValues = SyncManagerHelper.sync(fetchTimerTaskInfoSyncAction);

            if (allTaskValues == null) {
                return resultCursor;
            }

            // 2. Add them into result.
            for (List<Map<String, Object>> maps : allTaskValues) {
                if (maps == null) {
                    continue;
                }

                for (Map<String, Object> allValues : maps) {
                    if (allValues == null) {
                        continue;
                    }
                    ((ArrayResultCursor) resultCursor).addRow(
                        new Object[] {allValues.get("VARIABLE_NAME").toString(), allValues.get("VALUE").toString()});
                }
            }
        }

        return resultCursor;
    }
}
