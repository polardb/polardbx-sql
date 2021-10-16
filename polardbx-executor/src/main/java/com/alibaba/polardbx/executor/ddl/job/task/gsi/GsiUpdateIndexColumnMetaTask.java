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
import com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * for Clustered Global Index alter column
 * clean up gsi column metadata
 * <p>
 * will delete from [indexes]
 * will delete from [tables_ext]
 */
@TaskName(name = "GsiUpdateIndexColumnMetaTask")
@Getter
public class GsiUpdateIndexColumnMetaTask extends BaseGmsTask {

    final private String indexName;
    final private String oldColumnName;
    final private String newColumnName;
    final private String nullable;

    @JSONCreator
    public GsiUpdateIndexColumnMetaTask(String schemaName,
                                        String logicalTableName,
                                        String indexName,
                                        String oldColumnName,
                                        String newColumnName,
                                        boolean nullable) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.oldColumnName = oldColumnName;
        this.newColumnName = newColumnName;
        this.nullable = GsiUtils.toNullableString(nullable);
    }

    /**
     * see: GsiManager#alterGsi
     */
    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        GsiMetaChanger.updateIndexColumnMeta(
            metaDbConnection,
            schemaName,
            logicalTableName,
            indexName,
            oldColumnName,
            newColumnName,
            nullable
        );
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(String
            .format("Change GSI column task. schema:%s, table:%s, index:%s, oldColumn:%s, newColumn:%s, nullable:%s",
                schemaName,
                logicalTableName,
                indexName,
                oldColumnName,
                newColumnName,
                nullable));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        //todo implement me
    }
}