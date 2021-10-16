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
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class DeleteBaselineSyncAction implements ISyncAction {

    private String schemaName = null;

    private String parameterSql;

    private int baselineInfoId;

    private Integer planInfoId;

    public DeleteBaselineSyncAction() {
    }

    public DeleteBaselineSyncAction(String schemaName, int baselineInfoId, String parameterSql) {
        this.schemaName = schemaName;
        this.baselineInfoId = baselineInfoId;
        this.parameterSql = parameterSql;
        this.planInfoId = null;
    }

    public DeleteBaselineSyncAction(String schemaName, int baselineInfoId, String parameterSql, int planInfoId) {
        this.schemaName = schemaName;
        this.baselineInfoId = baselineInfoId;
        this.parameterSql = parameterSql;
        this.planInfoId = planInfoId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public int getBaselineInfoId() {
        return baselineInfoId;
    }

    public void setBaselineInfoId(int baselineInfoId) {
        this.baselineInfoId = baselineInfoId;
    }

    public String getParameterSql() {
        return parameterSql;
    }

    public void setParameterSql(String parameterSql) {
        this.parameterSql = parameterSql;
    }

    public Integer getPlanInfoId() {
        return planInfoId;
    }

    public void setPlanInfoId(Integer planInfoId) {
        this.planInfoId = planInfoId;
    }

    @Override
    public ResultCursor sync() {
        PlanManager planManager = OptimizerContext.getContext(schemaName).getPlanManager();
        if (planInfoId != null) {
            planManager.deleteBaseline(baselineInfoId, parameterSql, planInfoId);
        } else {
            planManager.deleteBaseline(baselineInfoId, parameterSql);
        }
        return null;
    }
}

