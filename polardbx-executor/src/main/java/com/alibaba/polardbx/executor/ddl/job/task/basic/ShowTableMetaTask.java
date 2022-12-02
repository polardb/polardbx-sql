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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import lombok.Getter;

import java.sql.Connection;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

@Getter
@TaskName(name = "ShowTableMetaTask")
public class ShowTableMetaTask extends BaseGmsTask {

    @JSONCreator
    public ShowTableMetaTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.showTableMeta(metaDbConnection, schemaName, logicalTableName);
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        SequenceBaseRecord sequenceRecord =
            tableInfoManager.fetchSequence(schemaName, AUTO_SEQ_PREFIX + logicalTableName);

        TableMetaChanger.triggerSchemaChange(metaDbConnection, schemaName, logicalTableName, sequenceRecord,
            tableInfoManager);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        PlanManager.getInstance().invalidateCacheBySchema(schemaName);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        TableMetaChanger.afterNewTableMeta(schemaName, logicalTableName);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.hideTableMeta(metaDbConnection, schemaName, logicalTableName);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName));
    }
}

