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

import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.view.InformationSchemaGlobalVariables;
import com.alibaba.polardbx.optimizer.view.InformationSchemaSessionVariables;
import com.alibaba.polardbx.optimizer.view.VirtualView;

/**
 * handle global and session variables
 *
 * @author shengyu
 */
public class InformationSchemaVariablesHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaVariablesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaGlobalVariables
            || virtualView instanceof InformationSchemaSessionVariables;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        String sql;
        if (virtualView instanceof InformationSchemaGlobalVariables) {
            sql = "show global variables";
        } else {
            sql = "show session variables";
        }

        ExecutionContext newExecutionContext = executionContext.copy();
        newExecutionContext.setTestMode(false);
        ExecutionPlan executionPlan = Planner.getInstance().plan(sql, newExecutionContext);
        return ExecutorHelper.execute(executionPlan.getPlan(), newExecutionContext);
    }
}
