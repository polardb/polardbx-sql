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
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "DropPartitionTableValidateTask")
public class DropPartitionTableValidateTask extends BaseValidateTask {

    private String logicalTableName;
    private boolean ifExists;

    @JSONCreator
    public DropPartitionTableValidateTask(String schemaName, String logicalTableName, boolean ifExists) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.ifExists = ifExists;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, logicalTableName, executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + logicalTableName;
    }

}
