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

package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "AlterGsiVisibilityValidateTask")
public class AlterGsiVisibilityValidateTask extends BaseValidateTask {
    private final String primaryTableName;
    private final String indexTableName;
    protected final String visibility;

    public AlterGsiVisibilityValidateTask(String schemaName, String primaryTableName, String indexTableName,
                                          String visibility) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.indexTableName = indexTableName;
        this.visibility = visibility;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        GsiValidator.validateGsiOrCci(schemaName, indexTableName);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
    }
}
