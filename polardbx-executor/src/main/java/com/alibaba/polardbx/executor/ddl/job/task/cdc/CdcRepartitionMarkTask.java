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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * created by ziyang.lb
 **/
@TaskName(name = "CdcRepartitionMarkTask")
@Getter
@Setter
public class CdcRepartitionMarkTask extends BaseCdcTask {
    private String logicalTableName;
    private SqlKind sqlKind;
    private CdcDdlMarkVisibility CdcDdlMarkVisibility;

    @JSONCreator
    public CdcRepartitionMarkTask(String schemaName, String logicalTableName, SqlKind sqlKind,
                                  CdcDdlMarkVisibility CdcDdlMarkVisibility) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.sqlKind = sqlKind;
        this.CdcDdlMarkVisibility = CdcDdlMarkVisibility;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        mark4RepartitionTable(executionContext);
    }

    private void mark4RepartitionTable(ExecutionContext executionContext) {
        if (CBOUtil.isGsi(schemaName, logicalTableName)) {
            return;
        }

        final String skipCutover =
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER);
        if (StringUtils.equalsIgnoreCase(skipCutover, Boolean.TRUE.toString())) {
            return;
        }

        // 主表和目标表之间已经完成了物理表的Switch操作，目标表以GSI的形式存在，依靠分布式事务，双边数据是强一致的
        // 需要在job结束前和Gsi被clean前，进行打标
        DdlContext ddlContext = executionContext.getDdlContext();
        Map<String, Object> param = buildExtendParameter(executionContext);
        param.put(ICdcManager.ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG, "");
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, sqlKind.name(), ddlContext.getDdlStmt(),
                ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(), CdcDdlMarkVisibility, param);
    }
}
