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
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;

import static com.alibaba.polardbx.common.cdc.ICdcManager.REFRESH_CREATE_SQL_4_PHY_TABLE;
import static com.alibaba.polardbx.common.cdc.ICdcManager.USE_OMC;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * Created by ziyang.lb
 **/
@TaskName(name = "CdcAlterTableColumnDdlMarkTask")
@Getter
@Setter
public class CdcAlterTableColumnDdlMarkTask extends BaseDdlTask {
    private final PhysicalPlanData physicalPlanData;
    private final boolean useOMC;
    private final Long versionId;

    @JSONCreator
    public CdcAlterTableColumnDdlMarkTask(String schemaName, PhysicalPlanData physicalPlanData, boolean useOMC,
                                          long versionId) {
        super(schemaName);
        this.physicalPlanData = physicalPlanData;
        this.useOMC = useOMC;
        this.versionId = versionId;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (CBOUtil.isGsi(schemaName, physicalPlanData.getLogicalTableName())) {
            return;
        }
        prepareExtraCmdsKey(executionContext);
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
        executionContext.getExtraCmds().put(USE_OMC, useOMC);
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void prepareExtraCmdsKey(ExecutionContext executionContext) {
        if (CdcMarkUtil.isVersionIdInitialized(versionId)) {
            CdcMarkUtil.useDdlVersionId(executionContext, versionId);
        }
    }

    private String getDdlStmt(ExecutionContext executionContext) {
        return getDdlStmt(executionContext.getDdlContext().getDdlStmt());
    }

    private String getDdlStmt(String ddl) {
        if (CdcMarkUtil.isVersionIdInitialized(versionId)) {
            return CdcMarkUtil.buildVersionIdHint(versionId) + ddl;
        }
        return ddl;
    }
}
