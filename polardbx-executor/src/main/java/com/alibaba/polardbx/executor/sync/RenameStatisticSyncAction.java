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

public class RenameStatisticSyncAction implements ISyncAction {

    private String schemaName = null;

    private String oldlogicalTableName;

    private String newlogicalTableName;

    public RenameStatisticSyncAction() {
    }

    public RenameStatisticSyncAction(String schemaName, String oldlogicalTableName, String newlogicalTableName) {
        this.schemaName = schemaName;
        this.oldlogicalTableName = oldlogicalTableName;
        this.newlogicalTableName = newlogicalTableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getOldlogicalTableName() {
        return oldlogicalTableName;
    }

    public void setOldlogicalTableName(String oldlogicalTableName) {
        this.oldlogicalTableName = oldlogicalTableName;
    }

    public String getNewlogicalTableName() {
        return newlogicalTableName;
    }

    public void setNewlogicalTableName(String newlogicalTableName) {
        this.newlogicalTableName = newlogicalTableName;
    }

    @Override
    public ResultCursor sync() {
        if (oldlogicalTableName != null && newlogicalTableName != null) {
            StatisticManager.getInstance().renameTable(schemaName, oldlogicalTableName, newlogicalTableName);
        }
        return null;
    }
}
