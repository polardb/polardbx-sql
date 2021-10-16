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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Deprecated
@TaskName(name = "PrimaryTableAndGsiSyncTask")
@Getter
public class PrimaryTableAndGsiSyncTask extends BaseSyncTask {

    final String primaryTableName;
    final String indexName;

    @JSONCreator
    public PrimaryTableAndGsiSyncTask(String schemaName,
                                      String primaryTableName,
                                      String indexName) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.indexName = indexName;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        new TableSyncTask(schemaName, indexName).executeImpl(executionContext);
        new TableSyncTask(schemaName, primaryTableName).executeImpl(executionContext);
    }

    @Override
    public String remark() {
        return String.format("|table=%s,index=%s", primaryTableName, indexName);
    }

}
