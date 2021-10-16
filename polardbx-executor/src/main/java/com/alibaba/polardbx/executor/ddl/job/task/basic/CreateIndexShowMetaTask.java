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
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateIndexShowMetaTask")
public class CreateIndexShowMetaTask extends BaseGmsTask {

    private String indexName;
    private String dbIndex;
    private String phyTableName;

    @JSONCreator
    public CreateIndexShowMetaTask(String schemaName, String logicalTableName, String indexName, String dbIndex,
                                   String phyTableName) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.addIndexMeta(metaDbConnection, schemaName, logicalTableName, indexName, dbIndex, phyTableName);
    }

}
