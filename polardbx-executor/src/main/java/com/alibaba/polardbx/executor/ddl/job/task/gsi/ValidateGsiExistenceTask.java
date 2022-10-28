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
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;

/**
 * Validate whether a GSI exists
 *
 * @since 2021/07
 */
@TaskName(name = "ValidateGsiExistenceTask")
@Getter
public class ValidateGsiExistenceTask extends BaseValidateTask {

    final private String primaryTableName;
    final private String indexName;
    private List<Long> tableGroupIds;
    private TableGroupConfig tableGroupConfig;

    @JSONCreator
    public ValidateGsiExistenceTask(String schemaName, String primaryTableName, String indexName,
                                    List<Long> tableGroupIds, TableGroupConfig tableGroupConfig) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.indexName = indexName;
        this.tableGroupIds = tableGroupIds;
        this.tableGroupConfig = TableGroupConfig.copyWithoutTables(tableGroupConfig);
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        doValidate(schemaName, primaryTableName, indexName, executionContext);
    }

    public void doValidate(String schemaName,
                           String primaryTableName,
                           String indexName,
                           ExecutionContext executionContext) {
        GsiValidator.validateGsiExistence(schemaName, primaryTableName, indexName, executionContext);
        TableValidator.validateTableInTableGroup(schemaName, indexName, tableGroupIds, executionContext);
        TableValidator.validateTableGroupChange(schemaName, tableGroupConfig);
    }

    @Override
    protected String remark() {
        return "|indexName: " + indexName;
    }
}
