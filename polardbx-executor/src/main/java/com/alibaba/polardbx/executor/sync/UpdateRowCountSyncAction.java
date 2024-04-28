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
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;

import java.util.HashMap;
import java.util.Map;

public class UpdateRowCountSyncAction implements ISyncAction {

    private String schemaName = null;

    private Map<String, Long> rowCountMap;

    public UpdateRowCountSyncAction() {
    }

    public UpdateRowCountSyncAction(String schemaName, Map<String, Long> rowCountMap) {
        this.schemaName = schemaName;
        this.rowCountMap = rowCountMap;
    }

    public UpdateRowCountSyncAction(String schemaName, String logicalTable, Long rowCount) {
        this.schemaName = schemaName;
        this.rowCountMap = new HashMap<>();
        this.rowCountMap.put(logicalTable, rowCount);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Map<String, Long> getRowCountMap() {
        return rowCountMap;
    }

    public void setRowCountMap(Map<String, Long> rowCountMap) {
        this.rowCountMap = rowCountMap;
    }

    @Override
    public ResultCursor sync() {
        for (String logicalTableName : rowCountMap.keySet()) {
            StatisticManager.getInstance().setRowCount(schemaName, logicalTableName, rowCountMap.get(logicalTableName));
        }

        return null;
    }
}

