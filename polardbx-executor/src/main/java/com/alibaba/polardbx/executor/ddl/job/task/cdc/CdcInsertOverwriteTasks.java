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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * created by ziyang.lb
 **/
public class CdcInsertOverwriteTasks {

    @TaskName(name = "DropOriginTableMarkTask")
    @Getter
    @Setter
    public static class DropOriginTableMarkTask extends BaseDdlTask {
        private final String logicTableName;

        @JSONCreator
        public DropOriginTableMarkTask(String schemaName, String logicTableName) {
            super(schemaName);
            this.logicTableName = logicTableName;
        }

        @Override
        protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
            updateSupportedCommands(true, false, metaDbConnection);
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);

            String sql = "drop table if exists " + surroundWithBacktick(logicTableName);
            CdcManagerHelper.getInstance().notifyDdlNew(schemaName, logicTableName, SqlKind.DROP_TABLE.name(),
                sql, DdlType.DROP_TABLE, getJobId(), getTaskId(), CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
        }
    }

    @TaskName(name = "RenameTempTableMarkTask")
    @Getter
    @Setter
    public static class RenameTempTableMarkTask extends BaseDdlTask {
        private final String renameFrom;
        private final String renameTo;

        @JSONCreator
        public RenameTempTableMarkTask(String schemaName, String renameFrom, String renameTo) {
            super(schemaName);
            this.renameFrom = renameFrom;
            this.renameTo = renameTo;
        }

        @Override
        protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
            updateSupportedCommands(true, false, metaDbConnection);
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);

            String sql = String.format("rename table %s to %s",
                surroundWithBacktick(renameFrom), surroundWithBacktick(renameTo));

            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
                String tbNamePattern = tddlRuleManager.getTableRule(renameTo).getTbNamePattern();

                Map<String, Object> params = buildExtendParameter(executionContext);
                params.put(ICdcManager.TABLE_NEW_NAME, renameTo);
                params.put(ICdcManager.TABLE_NEW_PATTERN, tbNamePattern);

                CdcManagerHelper.getInstance()
                    .notifyDdlNew(schemaName, renameFrom, SqlKind.RENAME_TABLE.name(), sql, DdlType.RENAME_TABLE,
                        getJobId(), getTaskId(), CdcDdlMarkVisibility.Public, params);
            } else {
                Map<String, Object> params = buildExtendParameter(executionContext);
                params.put(ICdcManager.TABLE_NEW_NAME, renameTo);

                PartitionInfo partitionInfo =
                    OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(renameTo);
                Map<String, Set<String>> topology = partitionInfo.getTopology();

                CdcManagerHelper.getInstance()
                    .notifyDdlNew(schemaName, renameFrom, SqlKind.RENAME_TABLE.name(), sql, DdlType.RENAME_TABLE,
                        getJobId(), getTaskId(), CdcDdlMarkVisibility.Public, params, true, topology);
            }
        }
    }
}
