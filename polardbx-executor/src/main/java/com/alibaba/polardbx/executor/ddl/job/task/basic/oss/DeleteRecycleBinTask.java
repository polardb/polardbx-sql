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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.sql.Statement;

@Getter
@TaskName(name = "DeleteRecycleBinTask")
public class DeleteRecycleBinTask extends BaseDdlTask {

    private String binName;

    @JSONCreator
    public DeleteRecycleBinTask(String schemaName, String binName) {
        super(schemaName);
        this.binName = binName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        try (Statement statement = metaDbConnection.createStatement()) {
            statement.executeUpdate(String.format("delete from %s where `schema_name` = '%s' and `name` = '%s'", GmsSystemTables.RECYCLE_BIN, schemaName, binName));
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "delete from recycle bin error", t);
        }
    }

    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {

    }
}
