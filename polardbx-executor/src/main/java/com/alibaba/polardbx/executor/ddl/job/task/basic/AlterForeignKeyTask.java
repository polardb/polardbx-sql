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
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wenki
 */
@Getter
@TaskName(name = "AlterForeignKeyTask")
public class AlterForeignKeyTask extends BaseGmsTask {
    private String dbIndex;
    private String phyTableName;
    private List<ForeignKeyData> addedForeignKeys;
    private List<String> droppedForeignKeys;
    private boolean withoutIndex;

    @JSONCreator
    public AlterForeignKeyTask(String schemaName,
                               String logicalTableName,
                               String dbIndex,
                               String phyTableName,
                               List<ForeignKeyData> addedForeignKeys,
                               List<String> droppedForeignKeys,
                               boolean withoutIndex) {
        super(schemaName, logicalTableName);
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
        this.addedForeignKeys = addedForeignKeys;
        this.droppedForeignKeys = droppedForeignKeys;
        this.withoutIndex = withoutIndex;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.addForeignKeyMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName,
            addedForeignKeys, withoutIndex);
        TableMetaChanger.dropForeignKeyMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName,
            droppedForeignKeys);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (GeneralUtil.isNotEmpty(addedForeignKeys)) {
            List<String> rollbackDroppedFks =
                addedForeignKeys.stream().map(fk -> fk.indexName).collect(Collectors.toList());
            TableMetaChanger.dropForeignKeyMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName,
                rollbackDroppedFks);
        }
        if (GeneralUtil.isNotEmpty(droppedForeignKeys)) {
            List<ForeignKeyData> rollbackAddedFks = new ArrayList<>();
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
            for (String indexName : droppedForeignKeys) {
                rollbackAddedFks.add(
                    tableMeta.getForeignKeys().get(schemaName + "/" + logicalTableName + "/" + indexName));
            }
            TableMetaChanger.addForeignKeyMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName,
                rollbackAddedFks, withoutIndex);
        }
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isNotEmpty(this.addedForeignKeys)) {
            sb.append("add foreign keys ").append(this.addedForeignKeys);
        }
        if (CollectionUtils.isNotEmpty(this.droppedForeignKeys)) {
            sb.append("drop foreign keys ").append(this.droppedForeignKeys);
        }
        sb.append(" on table ").append(this.getLogicalTableName());
        return "|" + sb;
    }
}
