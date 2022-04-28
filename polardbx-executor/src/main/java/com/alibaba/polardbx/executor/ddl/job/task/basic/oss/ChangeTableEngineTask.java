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
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "ChangeTableEngineTask")
public class ChangeTableEngineTask extends BaseGmsTask {
    private final Engine sourceEngine;
    private final Engine targetEngine;

    @JSONCreator
    public ChangeTableEngineTask(String schemaName, String logicalTableName,
                                 Engine sourceEngine, Engine targetEngine) {
        super(schemaName, logicalTableName);
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.changeTableEngine(metaDbConnection, schemaName, logicalTableName, targetEngine);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.changeTableEngine(metaDbConnection, schemaName, logicalTableName, sourceEngine);
    }
}
