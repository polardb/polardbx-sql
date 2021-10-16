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
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * For PolarDB-X only.
 */
public class TablesMetaChangePreemptiveSyncAction implements ISyncAction {
    private String schemaName;
    private List<String> logicalTables;
    private Long initWait;
    private Long interval;
    private TimeUnit timeUnit;

    public TablesMetaChangePreemptiveSyncAction() {

    }

    public TablesMetaChangePreemptiveSyncAction(String schemaName, List<String> logicalTables, Long initWait,
                                                Long interval, TimeUnit timeUnit) {
        this.schemaName = schemaName;
        this.logicalTables = logicalTables;
        this.initWait = initWait;
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    @Override
    public ResultCursor sync() {
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        for (String tableName : logicalTables) {
            ((GmsTableMetaManager) oldSchemaManager).tonewversion(tableName, true, initWait, interval, timeUnit);
        }
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<String> getLogicalTables() {
        return logicalTables;
    }

    public void setLogicalTables(List<String> logicalTables) {
        this.logicalTables = logicalTables;
    }

    public Long getInitWait() {
        return initWait;
    }

    public void setInitWait(Long initWait) {
        this.initWait = initWait;
    }

    public Long getInterval() {
        return interval;
    }

    public void setInterval(Long interval) {
        this.interval = interval;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }
}
