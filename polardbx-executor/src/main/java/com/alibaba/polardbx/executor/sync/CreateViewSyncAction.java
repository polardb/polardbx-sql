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

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

/**
 * @author dylan
 */
public class CreateViewSyncAction implements ISyncAction {

    private String viewName;
    private String schemaName;

    public CreateViewSyncAction() {

    }

    public CreateViewSyncAction(String schemaName, String viewName) {
        this.schemaName = schemaName;
        this.viewName = viewName;
    }

    @Override
    public ResultCursor sync() {
        if (viewName != null) {
            OptimizerContext.getContext(schemaName).getViewManager().invalidate(viewName);
            PlanManager.getInstance().invalidateTable(schemaName, viewName);
        }

        return null;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String tableName) {
        this.viewName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}