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

import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;

import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;

public class UpdateStatisticSyncAction implements ISyncAction {

    private String schemaName = null;

    private String logicalTableName;

    private String jsonString;

    public UpdateStatisticSyncAction() {
    }

    public UpdateStatisticSyncAction(String schemaName, String logicalTableName, StatisticManager.CacheLine cacheLine) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        if (cacheLine != null) {
            this.jsonString = StatisticManager.CacheLine.serializeToJson(cacheLine);
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public void setLogicalTableName(String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }

    public String getJsonString() {
        return jsonString;
    }

    public void setJsonString(String jsonString) {
        this.jsonString = jsonString;
    }

    @Override
    public ResultCursor sync() {
        if (jsonString != null && getJsonString() != null) {
            StatisticManager.getInstance()
                .setCacheLine(schemaName, logicalTableName,
                    StatisticManager.CacheLine.deserializeFromJson(getJsonString()));
        }

        // refresh plancache
        PlanCache.getInstance().invalidate(logicalTableName);
        StatisticManager.getInstance().reloadNDVbyTableName(schemaName, logicalTableName);
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTIC,
                PROCESS_END,
                new String[] {
                    "sync statistic info " + schemaName + ", " + logicalTableName,
                    "succ"
                },
                LogLevel.NORMAL);
        return null;
    }
}
