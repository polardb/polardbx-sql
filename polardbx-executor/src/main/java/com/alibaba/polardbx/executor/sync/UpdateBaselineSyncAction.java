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
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class UpdateBaselineSyncAction implements ISyncAction {

    private String schemaName = null;

    private String jsonString;

    private transient BaselineInfo baselineInfo;

    public UpdateBaselineSyncAction() {
    }

    public UpdateBaselineSyncAction(String schemaName, BaselineInfo baselineInfo) {
        this.schemaName = schemaName;
        this.baselineInfo = baselineInfo;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getJsonString() {
        if (jsonString == null) {
            jsonString = BaselineInfo.serializeBaseInfoToJson(baselineInfo);
        }
        return jsonString;
    }

    public void setJsonString(String jsonString) {
        this.jsonString = jsonString;
    }

    @Override
    public ResultCursor sync() {
        PlanManager planManager = OptimizerContext.getContext(schemaName).getPlanManager();
        planManager
            .updateBaseline(baselineInfo != null ? baselineInfo : BaselineInfo.deserializeFromJson(getJsonString()));
        return null;
    }
}

