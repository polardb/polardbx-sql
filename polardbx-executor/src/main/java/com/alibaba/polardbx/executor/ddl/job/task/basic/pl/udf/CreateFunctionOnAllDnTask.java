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

package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Set;

@Getter
@TaskName(name = "CreateFunctionOnAllDnTask")
public class CreateFunctionOnAllDnTask extends BaseDdlTask {
    private static String DROP_FUNCTION = "drop function if exists %s";

    private final String functionName;
    private final String createContent;

    @JSONCreator
    public CreateFunctionOnAllDnTask(String schemaName, String functionName, String createContent) {
        super(schemaName);
        this.functionName = functionName;
        this.createContent = createContent;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);

        Set<String> allDnId = ExecUtils.getAllDnStorageId();
        for (String dnId : allDnId) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(DROP_FUNCTION, functionName));
                stmt.execute(createContent);
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to create function on " + dnId, e);
            }
        }
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        Set<String> allDnId = ExecUtils.getAllDnStorageId();
        for (String dnId : allDnId) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(DROP_FUNCTION, functionName));
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to rollback create function on " + dnId, e);
            }
        }
    }
}
