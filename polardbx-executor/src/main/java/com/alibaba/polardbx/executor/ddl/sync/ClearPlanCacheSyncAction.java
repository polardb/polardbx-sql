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

package com.alibaba.polardbx.executor.ddl.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import org.apache.commons.lang.StringUtils;

public class ClearPlanCacheSyncAction implements ISyncAction {

    private String schemaName;
    private String tableName;

    public ClearPlanCacheSyncAction() {
    }

    public ClearPlanCacheSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    public ClearPlanCacheSyncAction(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public ResultCursor sync() {
        if (StringUtils.isNotEmpty(tableName)) {
            // sync with table name meaning seq handler had been fixed, clean baseline+plancache
            PlanCache.getInstance().invalidateByTable(schemaName, tableName);
        } else {
            // sync without table name meaning seq handler hadn't been fixed, only clean plancache by schema
            PlanCache.getInstance().invalidateBySchema(schemaName);
        }
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}

