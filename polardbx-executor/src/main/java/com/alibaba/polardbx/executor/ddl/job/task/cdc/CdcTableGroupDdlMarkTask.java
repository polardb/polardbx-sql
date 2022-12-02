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
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * created by ziyang.lb
 **/
@TaskName(name = "CdcTableGroupDdlMarkTask")
@Getter
@Setter
@Slf4j
public class CdcTableGroupDdlMarkTask extends BaseDdlTask {

    private String logicalTableName;
    private SqlKind sqlKind;
    private Map<String, Set<String>> targetTableTopology;
    private String ddlStmt;

    @JSONCreator
    public CdcTableGroupDdlMarkTask(String schemaName, String logicalTableName, SqlKind sqlKind,
                                    Map<String, Set<String>> targetTableTopology, String ddlStmt) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.sqlKind = sqlKind;
        this.targetTableTopology = targetTableTopology;
        this.ddlStmt = ddlStmt;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        mark4TableGroupChange(executionContext);
    }

    private void mark4TableGroupChange(ExecutionContext executionContext) {
        log.info("new topology for table {} is {}", logicalTableName, targetTableTopology);
        DdlContext ddlContext = executionContext.getDdlContext();

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, sqlKind.name(), ddlStmt, DdlType.ALTER_TABLEGROUP,
                ddlContext.getJobId(), getTaskId(), DdlVisibility.Private,
                buildExtendParameter(executionContext), true, targetTableTopology);
    }
}
