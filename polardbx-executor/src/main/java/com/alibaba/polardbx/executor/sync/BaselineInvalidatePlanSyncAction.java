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
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

import static com.alibaba.polardbx.gms.module.LogPattern.LOAD_DATA;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;

/**
 * invalidate baseline by schema and table
 */
public class BaselineInvalidatePlanSyncAction implements ISyncAction {

    private String schema;
    private String table;
    private boolean isForce;

    public BaselineInvalidatePlanSyncAction(String schema, String table, boolean isForce) {
        this.schema = schema;
        this.table = table;
        this.isForce = isForce;
    }

    @Override
    public ResultCursor sync() {
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.SPM,
                PROCESS_START,
                new String[] {"BaselineInvalidatePlanSyncAction", schema + "," + table + "," + isForce},
                LogLevel.NORMAL);
        PlanManager.getInstance().invalidateTable(schema, table, isForce);
        return null;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isIsForce() {
        return isForce;
    }

    public void setIsForce(boolean force) {
        isForce = force;
    }
}