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
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * for Clustered Global index drop column
 * clean up gsi column metadata
 * <p>
 * will delete from [indexes]
 * will delete from [tables_ext]
 */
@TaskName(name = "GsiDropColumnCleanUpTask")
@Getter
public class GsiDropColumnCleanUpTask extends BaseGmsTask {

    final private String indexName;
    final private List<String> columns;

    @JSONCreator
    public GsiDropColumnCleanUpTask(String schemaName,
                                    String logicalTableName,
                                    String indexName,
                                    List<String> columns) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.columns = columns;
    }

    /**
     * see: GsiManager#alterGsi# alterTable.dropColumn()
     */
    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // TODO(moyi) make it transactional
        for (String column : this.getColumns()) {
            ExecutorContext
                .getContext(executionContext.getSchemaName())
                .getGsiManager()
                .getGsiMetaManager()
                .removeColumnMeta(metaDbConnection, schemaName, logicalTableName, indexName, column);
            LOGGER.info(String.format("Drop GSI column cleanup task. schema:%s, table:%s, index:%s, column:%s",
                schemaName,
                logicalTableName,
                indexName,
                column));
        }
    }
}
