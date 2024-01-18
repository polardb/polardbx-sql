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
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class DeleteBaselineSyncAction implements ISyncAction {

    private String schemaName;

    private final String parameterSql;

    private final Integer planInfoId;

    public DeleteBaselineSyncAction(String schemaName, String parameterSql) {
        this.schemaName = schemaName;
        this.parameterSql = parameterSql;
        this.planInfoId = null;
    }

    public DeleteBaselineSyncAction(String schemaName, String parameterSql, int planInfoId) {
        this.schemaName = schemaName;
        this.parameterSql = parameterSql;
        this.planInfoId = planInfoId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public ResultCursor sync() {
        if (planInfoId != null) {
            PlanManager.getInstance().deleteBaseline(schemaName, parameterSql, planInfoId);
        } else {
            PlanManager.getInstance().deleteBaseline(schemaName, parameterSql);
        }
        return null;
    }

    public String getParameterSql() {
        return parameterSql;
    }

    public Integer getPlanInfoId() {
        return planInfoId;
    }
}

