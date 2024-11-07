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
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;

import static com.alibaba.polardbx.common.cdc.ICdcManager.FOREIGN_KEYS_DDL;
import static com.alibaba.polardbx.common.cdc.ICdcManager.REFRESH_CREATE_SQL_4_PHY_TABLE;
import static com.alibaba.polardbx.common.cdc.ICdcManager.USE_ORIGINAL_DDL;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

@TaskName(name = "CdcAlterTableRewrittenDdlMarkTask")
@Getter
@Setter
public class CdcAlterTableRewrittenDdlMarkTask extends BaseCdcTask {

    private PhysicalPlanData physicalPlanData;
    private String logicalSql;
    private boolean foreignKeys;

    @JSONCreator
    public CdcAlterTableRewrittenDdlMarkTask(String schemaName, PhysicalPlanData physicalPlanData, String logicalSql,
                                             Boolean foreignKeys) {
        super(schemaName);
        this.physicalPlanData = physicalPlanData;
        this.logicalSql = logicalSql;
        this.foreignKeys = foreignKeys != null && foreignKeys;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (CBOUtil.isGsi(schemaName, physicalPlanData.getLogicalTableName())) {
            return;
        }
        updateSupportedCommands(true, false, metaDbConnection);

        DdlContext ddlContext = executionContext.getDdlContext();
        AlterTablePreparedData alterTablePreparedData = physicalPlanData.getAlterTablePreparedData();

        // 加减列操作，可能会导致逻辑表结构和物理表结构不一致，重新对
        if (alterTablePreparedData != null) {
            boolean isAddColumns =
                alterTablePreparedData.getAddedColumns() != null && !alterTablePreparedData.getAddedColumns().isEmpty();
            boolean isDropColumns =
                alterTablePreparedData.getDroppedColumns() != null && !alterTablePreparedData.getDroppedColumns()
                    .isEmpty();
            if (isAddColumns || isDropColumns) {
                executionContext.getExtraCmds().put(REFRESH_CREATE_SQL_4_PHY_TABLE, "true");
                if (CollectionUtils.isNotEmpty(alterTablePreparedData.getAddedIndexes())) {
                    executionContext.getExtraCmds().put(USE_ORIGINAL_DDL, "true");
                }
            }
        }
        if (foreignKeys) {
            executionContext.getExtraCmds().put(FOREIGN_KEYS_DDL, "true");
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                logicalSql, DdlType.ALTER_TABLE, ddlContext.getJobId(), getTaskId(), CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
    }

}
