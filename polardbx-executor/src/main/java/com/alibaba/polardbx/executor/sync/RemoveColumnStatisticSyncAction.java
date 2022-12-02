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
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticService;

import java.util.List;

public class RemoveColumnStatisticSyncAction implements ISyncAction {

    private String schemaName = null;

    private String logicalTableName;

    private List<String> columnNameList;

    public RemoveColumnStatisticSyncAction() {
    }

    public RemoveColumnStatisticSyncAction(String schemaName, String logicalTableName, List<String> columnNameList) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.columnNameList = columnNameList;
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

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    @Override
    public ResultCursor sync() {
        if (logicalTableName != null && columnNameList != null) {
            StatisticManager.getInstance().removeLogicalColumnList(schemaName, logicalTableName, columnNameList);
        }
        return null;
    }
}
