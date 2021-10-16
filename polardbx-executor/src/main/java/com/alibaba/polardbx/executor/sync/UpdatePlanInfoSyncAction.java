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
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class UpdatePlanInfoSyncAction implements ISyncAction {

    private String schemaName = null;

    private String jsonString;

    private String parameterSql;

    private int originPlanId;

    private transient PlanInfo planInfo;

    public UpdatePlanInfoSyncAction() {
    }

    public UpdatePlanInfoSyncAction(String schemaName, int originPlanId, PlanInfo planInfo, String parameterSql) {
        this.schemaName = schemaName;
        this.planInfo = planInfo;
        this.parameterSql = parameterSql;
        this.originPlanId = originPlanId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getJsonString() {
        if (jsonString == null) {
            jsonString = PlanInfo.serializeToJson(planInfo);
        }
        return jsonString;
    }

    public void setJsonString(String jsonString) {
        this.jsonString = jsonString;
    }

    public String getParameterSql() {
        return parameterSql;
    }

    public void setParameterSql(String parameterSql) {
        this.parameterSql = parameterSql;
    }

    public int getPlanOriginId() {
        return originPlanId;
    }

    public void setPlanOriginId(int originPlanId) {
        this.originPlanId = originPlanId;
    }

    @Override
    public ResultCursor sync() {
        PlanManager planManager = OptimizerContext.getContext(schemaName).getPlanManager();
        planManager
            .updatePlanInfo(planInfo != null ? planInfo : PlanInfo.deserializeFromJson(getJsonString()), parameterSql,
                originPlanId);
        return null;
    }
}

