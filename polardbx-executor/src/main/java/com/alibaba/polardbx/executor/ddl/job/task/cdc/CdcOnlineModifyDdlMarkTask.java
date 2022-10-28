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

package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;

import static com.alibaba.polardbx.common.cdc.ICdcManager.REFRESH_CREATE_SQL_4_PHY_TABLE;

/**
 * Created by ziyang.lb
 **/
@TaskName(name = "CdcOnlineModifyDdlMarkTask")
@Getter
@Setter
public class CdcOnlineModifyDdlMarkTask extends BaseDdlTask {
    private final PhysicalPlanData physicalPlanData;

    @JSONCreator
    public CdcOnlineModifyDdlMarkTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName);
        this.physicalPlanData = physicalPlanData;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        if (physicalPlanData.getKind() == SqlKind.ALTER_TABLE) {
            mark4AlterTable(executionContext);
        } else {
            throw new RuntimeException("not supported sql kind : " + physicalPlanData.getKind());
        }
    }

    private void mark4AlterTable(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        executionContext.getExtraCmds().put(REFRESH_CREATE_SQL_4_PHY_TABLE, "true");
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Public,
                executionContext.getExtraCmds());
    }
}
