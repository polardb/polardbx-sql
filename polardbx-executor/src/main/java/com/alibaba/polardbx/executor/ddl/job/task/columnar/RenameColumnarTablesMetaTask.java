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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "RenameColumnarTablesMetaTask")
public class RenameColumnarTablesMetaTask extends BaseGmsTask {
    private final List<String> oldTableNames;
    private final List<String> newTableNames;
    private final List<Long> versionIds;

    @JSONCreator
    public RenameColumnarTablesMetaTask(String schemaName, List<String> oldTableNames, List<String> newTableNames,
                                        List<Long> versionIds) {
        super(schemaName, null);
        this.oldTableNames = oldTableNames;
        this.newTableNames = newTableNames;
        this.versionIds = versionIds;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        TableMetaChanger.renameColumnarTablesMeta(metaDbConnection, schemaName, oldTableNames, newTableNames,
            versionIds, jobId);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.renameColumnarTablesMeta(metaDbConnection, schemaName, newTableNames, oldTableNames,
            versionIds, jobId);
    }

}
