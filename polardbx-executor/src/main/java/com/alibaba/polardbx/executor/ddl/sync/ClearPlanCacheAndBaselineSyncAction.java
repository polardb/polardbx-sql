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
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import org.apache.commons.lang.StringUtils;

public class ClearPlanCacheAndBaselineSyncAction implements ISyncAction {

    private String schema;
    private String table;

    public ClearPlanCacheAndBaselineSyncAction() {
    }

    public ClearPlanCacheAndBaselineSyncAction(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override
    public ResultCursor sync() {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(table)) {
            return null;
        }
        PlanManager.getInstance().invalidateTable(schema, table);
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
}
