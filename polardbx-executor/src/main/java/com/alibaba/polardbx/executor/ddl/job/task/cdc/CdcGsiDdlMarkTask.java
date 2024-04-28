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
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * Created by chengjin.lyf
 **/
@TaskName(name = "CdcGsiDdlMarkTask")
@Getter
@Setter
public class CdcGsiDdlMarkTask extends BaseDdlTask {

    private final PhysicalPlanData physicalPlanData;
    private String originalDdl;
    private String primaryTable;

    @JSONCreator
    public CdcGsiDdlMarkTask(String schemaName, PhysicalPlanData physicalPlanData, String primaryTable,
                             String originalDdl) {
        super(schemaName);
        this.physicalPlanData = physicalPlanData;
        this.primaryTable = primaryTable;
        this.originalDdl = originalDdl;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        injectGSI(executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        CdcMarkUtil.useOriginalDDL(executionContext);
        DdlType ddlType;
        SqlKind sqlKind;
        if (physicalPlanData.getKind() == SqlKind.CREATE_TABLE) {
            ddlType = DdlType.CREATE_GLOBAL_INDEX;
            sqlKind = SqlKind.CREATE_INDEX;
        } else if (physicalPlanData.getKind() == SqlKind.DROP_TABLE) {
            ddlType = DdlType.DROP_GLOBAL_INDEX;
            sqlKind = SqlKind.DROP_INDEX;
        } else if (physicalPlanData.getKind() == SqlKind.RENAME_TABLE) {
            ddlType = DdlType.RENAME_GLOBAL_INDEX;
            sqlKind = SqlKind.ALTER_RENAME_INDEX;
        } else {
            throw new RuntimeException("not supported sql kind : " + physicalPlanData.getKind());
        }

        String originalDdl = this.originalDdl != null ? this.originalDdl : executionContext.getOriginSql();
        if (StringUtils.isNotBlank(executionContext.getDdlContext().getCdcRewriteDdlStmt())) {
            originalDdl = executionContext.getDdlContext().getCdcRewriteDdlStmt();
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, primaryTable, sqlKind.name(),
                originalDdl, ddlType, executionContext.getDdlContext().getJobId(),
                getTaskId(),
                CdcDdlMarkVisibility.Public, buildExtendParameter(executionContext));
    }

    private void injectGSI(ExecutionContext executionContext) {
        executionContext.getExtraCmds().put(ICdcManager.CDC_IS_GSI, true);
        executionContext.getExtraCmds().put(ICdcManager.CDC_GSI_PRIMARY_TABLE, primaryTable);
    }

}
