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
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;

public class UpdateStatisticSyncAction implements ISyncAction {

    private String schemaName = null;

    private String logicalTableName;

    private String jsonString;

    private transient StatisticManager.CacheLine cacheLine;

    public UpdateStatisticSyncAction() {
    }

    public UpdateStatisticSyncAction(String schemaName, String logicalTableName, StatisticManager.CacheLine cacheLine) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.cacheLine = cacheLine;
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
        if (cacheLine == null) {
            return null;
        }
        if (jsonString == null) {
            jsonString = StatisticManager.CacheLine.serializeToJson(cacheLine);
        }
        return jsonString;
    }

    public void setJsonString(String jsonString) {
        this.jsonString = jsonString;
    }

    @Override
    public ResultCursor sync() {
        StatisticManager statisticManager = OptimizerContext.getContext(schemaName).getStatisticManager();
        if (cacheLine != null) {
            statisticManager.setCacheLine(logicalTableName, cacheLine);
        } else if (jsonString != null && getJsonString() != null) {
            statisticManager
                .setCacheLine(logicalTableName, StatisticManager.CacheLine.deserializeFromJson(getJsonString()));
        }

        // refresh plancache
        OptimizerContext.getContext(schemaName).getPlanManager().getPlanCache().invalidate(logicalTableName);
        statisticManager.reloadNDVbyTableName(logicalTableName);
        StatisticUtils.logInfo(schemaName, " sync statistic info succ:" + schemaName + ", " + logicalTableName);
        return null;
    }
}
