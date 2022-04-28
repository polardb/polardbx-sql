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

public class BaselineValidateSyncAction implements ISyncAction {

    private String schemaName = null;

    private Integer baselineId = null;

    public BaselineValidateSyncAction() {
    }

    public BaselineValidateSyncAction(String schemaName, Integer baselineId) {
        this.schemaName = schemaName;
        this.baselineId = baselineId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Integer getBaselineId() {
        return baselineId;
    }

    public void setBaselineId(Integer baselineId) {
        this.baselineId = baselineId;
    }

    @Override
    public ResultCursor sync() {
        PlanManager planManager = OptimizerContext.getContext(schemaName).getPlanManager();
        if (baselineId != null) {
            planManager.forceValidate(baselineId);
        } else {
            planManager.forceValidateAll();
        }
        return null;
    }
}



