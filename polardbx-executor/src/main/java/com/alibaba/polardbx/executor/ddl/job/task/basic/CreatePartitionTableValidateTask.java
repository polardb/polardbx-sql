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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "CreatePartitionTableValidateTask")
public class CreatePartitionTableValidateTask extends BaseValidateTask {

    private String logicalTableName;
    private boolean ifNotExists;

    @JSONCreator
    public CreatePartitionTableValidateTask(String schemaName, String logicalTableName, boolean ifNotExists) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableNonExistence(schemaName, logicalTableName, executionContext);
        //todo for partition table, maybe we need the corresponding physical table name checker
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
        // Remove the job and cache since we don't really do anything yet.
        // todo reload/remove partitionInfo
        // TableValidator.reloadTableRule(schemaName, logicalTableName);
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + logicalTableName;
    }
}
