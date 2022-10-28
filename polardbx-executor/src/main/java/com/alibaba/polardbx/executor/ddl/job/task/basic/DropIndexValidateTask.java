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
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "DropIndexValidateTask")
public class DropIndexValidateTask extends BaseValidateTask {

    private String logicalTableName;
    private String indexName;
    private TableGroupConfig tableGroupConfig;

    @JSONCreator
    public DropIndexValidateTask(String schemaName, String logicalTableName, String indexName,
                                 TableGroupConfig tableGroupConfig) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.tableGroupConfig = TableGroupConfig.copyWithoutTables(tableGroupConfig);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);
        if (tableGroupConfig != null) {
            TableValidator.validateTableGroupChange(schemaName, tableGroupConfig);
        }
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + logicalTableName;
    }
}
