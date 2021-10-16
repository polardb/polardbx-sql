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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

import java.util.concurrent.TimeUnit;

/**
 * For PolarDB-X only.
 */
public class TableMetaChangePreemptiveSyncAction implements ISyncAction {
    protected final static Logger logger = LoggerFactory.getLogger(TableMetaChangePreemptiveSyncAction.class);

    private String schemaName;
    private String primaryTableName;
    private Long initWait;
    private Long interval;
    private TimeUnit timeUnit;

    public TableMetaChangePreemptiveSyncAction() {

    }

    public TableMetaChangePreemptiveSyncAction(String schemaName, String tableName, Long initWait, Long interval,
                                               TimeUnit timeUnit) {
        this.schemaName = schemaName;
        this.primaryTableName = tableName;
        this.initWait = initWait;
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    @Override
    public ResultCursor sync() {
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        ((GmsTableMetaManager) oldSchemaManager).tonewversion(primaryTableName, true, initWait, interval, timeUnit);
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    public static Logger getLogger() {
        return logger;
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
