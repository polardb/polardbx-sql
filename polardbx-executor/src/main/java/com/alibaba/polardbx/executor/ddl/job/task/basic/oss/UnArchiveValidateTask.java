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
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import lombok.Getter;

import java.util.List;

/**
 * @author Shi Yuxuan
 */
@Getter
@TaskName(name = "UnArchiveValidateTask")
public class UnArchiveValidateTask  extends BaseValidateTask {
    private List<String> tables;

    @JSONCreator
    public UnArchiveValidateTask(String schemaName, List<String> tables) {
        super(schemaName);
        this.tables = tables;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        // make sure all tables are valid local partition table
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        for (String table : tables) {
            LocalPartitionDefinitionInfo localPartitionDefinitionInfo = sm.getTable(table).getLocalPartitionDefinitionInfo();
            if (localPartitionDefinitionInfo == null) {
                throw new TddlNestableRuntimeException(String.format(
                    "table %s.%s is not a local partition table", schemaName, table));
            }
            if (StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableSchema()) ||
            StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableName())) {
                throw new TddlNestableRuntimeException(String.format(
                    "table %s.%s has no archive table", schemaName, table));
            }
        }
    }
}
