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
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESSING;
import static com.alibaba.polardbx.gms.module.Module.SPM;

public class DeleteBaselineSyncAction implements ISyncAction {

    private String schemaName;

    private String parameterSql;

    private Integer planInfoId;

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
        if (planInfoId != null && planInfoId != 0) {
            ModuleLogInfo.getInstance()
                .logRecord(SPM, PROCESSING, new String[] {"delete plan " + planInfoId, parameterSql}, NORMAL);
            PlanManager.getInstance().deleteBaseline(schemaName, parameterSql, planInfoId);
        } else {
            ModuleLogInfo.getInstance()
                .logRecord(SPM, PROCESSING, new String[] {"delete baseline ", parameterSql}, NORMAL);
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

    public void setParameterSql(String parameterSql) {
        this.parameterSql = parameterSql;
    }

    public void setPlanInfoId(Integer planInfoId) {
        this.planInfoId = planInfoId;
    }
}

