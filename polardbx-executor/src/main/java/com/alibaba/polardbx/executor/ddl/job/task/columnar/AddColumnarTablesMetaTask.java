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
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AddColumnarTablesMetaTask")
public class AddColumnarTablesMetaTask extends BaseGmsTask {

    private final String columnarTableName;
    private final Engine engine;
    private final Long versionId;

    @JSONCreator
    public AddColumnarTablesMetaTask(String schemaName, String logicalTableName, String columnarTableName,
                                     Long versionId, Engine engine) {
        super(schemaName, logicalTableName);
        this.columnarTableName = columnarTableName;
        this.versionId = versionId;
        this.engine = engine;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.addColumnarTableMeta(
            metaDbConnection,
            schemaName,
            logicalTableName,
            columnarTableName,
            engine);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.removeColumnarTableMeta(metaDbConnection,
            schemaName,
            columnarTableName);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        TableMetaChanger.afterRemovingTableMeta(schemaName, logicalTableName);
    }
}
